/*
 * Copyright 2025 Security Union LLC
 *
 * Licensed under either of
 *
 * * Apache License, Version 2.0
 *   (http://www.apache.org/licenses/LICENSE-2.0)
 * * MIT license
 *   (http://opensource.org/licenses/MIT)
 *
 * at your option.
 */

//! Debounced visibility/pin state tracker that emits coalesced
//! `SubscriptionUpdate` packets.
//!
//! The UI calls `set_visible(...)` and `set_pinned(...)` on every viewport
//! intersection change or pin toggle. Naively forwarding each call to the SFU
//! would generate one `SubscriptionUpdate` per change — bursty scrolls or pin
//! storms can dwarf the available signaling bandwidth. The coalescer batches
//! state changes inside a 100ms window and emits a single `SubscriptionUpdate`
//! at the end of that window carrying the *current* (declarative) state.
//!
//! ## Design
//!
//! - State (`visible`, `pinned` sets) lives in an inner `RefCell` shared via
//!   `Rc`, so multiple call sites (visibility callbacks, pin handlers, the
//!   timer flush, tests) mutate the same authoritative copy.
//! - On a state change we schedule a flush exactly once. If another change
//!   arrives while the flush is pending we do **not** schedule a second one —
//!   the pending flush will read whatever state is current at fire time.
//! - The emit is dependency-injected: production wires it to
//!   `SfuClient::emit_subscription_update`; tests inject a `Vec` capture so
//!   coalescing behaviour is verified without spinning up a transport.
//! - The flush trigger itself is overridable. In production
//!   `gloo::timers::callback::Timeout::new(100, …)` runs the closure 100ms
//!   later on the browser event loop. In tests we replace the trigger with a
//!   no-op that records the request — the test then drives `flush_now()`
//!   manually, side-stepping real time.
//!
//! ## Sequence counter
//!
//! Each emit increments a monotonic `seq` counter held inside the inner
//! state. The proto wire format (`SubscriptionUpdate`) does *not* currently
//! carry this value — it's purely client-local book-keeping for future
//! client-side dedup or out-of-order detection. Carrying it in code now
//! (rather than as a `// TODO` comment) makes the wave-3 contract explicit
//! and gives tests a stable counter to assert against.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use videocall_types::protos::subscription_packet::{SubscriptionUpdate, VisibilitySlot};

/// Default debounce window. 100ms matches the bead spec and is long enough to
/// collapse a typical scroll burst while short enough that pin clicks still
/// feel responsive.
pub(crate) const COALESCE_WINDOW_MS: u32 = 100;

/// Default `max_video_kbps`. Wave-3 stub value — replace once bandwidth
/// estimation is plumbed through.
pub(crate) const DEFAULT_MAX_VIDEO_KBPS: u32 = 2000;

/// Boxed callback that ships an emit out to the world. Production wires this
/// to `SfuClient::emit_subscription_update` via `spawn_local`; tests inject a
/// capture.
pub(crate) type EmitFn = Box<dyn Fn(SubscriptionUpdate)>;

/// Boxed callback that schedules `flush_now` to run after the debounce
/// window. Production uses `gloo::timers::callback::Timeout`; tests replace
/// it with a no-op so they can drive `flush_now()` deterministically.
///
/// The argument is a closure that performs the flush — the trigger
/// implementation is responsible for arranging for it to be invoked after
/// `COALESCE_WINDOW_MS`.
pub(crate) type FlushTrigger = Box<dyn Fn(Box<dyn FnOnce()>)>;

struct Inner {
    visible: HashSet<u64>,
    pinned: HashSet<u64>,
    /// True if a flush is already scheduled and hasn't fired yet. Guards
    /// against piling up multiple timers during a burst.
    flush_pending: bool,
    /// Monotonically incremented on every emit. Wire-format does not carry
    /// this yet (see module doc); kept client-local.
    seq: u64,
    max_video_kbps: u32,
    receive_all_audio: bool,
    /// vc-3s8: when true, the SFU fans out video to every current and future
    /// room member (minus self), capped at the server's MAX_VISIBLE_VIDEO.
    /// Mirrors `receive_all_audio` for video so a webinar listener that
    /// hasn't yet flipped any peer to visible still receives video from
    /// senders that join AFTER the coalescer's initial flush.
    receive_all_video: bool,
    emit: EmitFn,
}

impl Inner {
    /// Build the proto payload from current state and hand it to the
    /// emit sink. Also clears the pending flag and bumps the sequence
    /// counter.
    fn flush(&mut self) {
        self.flush_pending = false;
        // Monotonic — never wraps in practice (would require >18 quintillion
        // emits in a single session). Plain `+=` makes the intent clearer
        // than `wrapping_add`.
        self.seq += 1;

        // Slots are sorted so emits are deterministic — useful for tests and
        // for any future server-side diffing.
        let mut visible_sorted: Vec<u64> = self.visible.iter().copied().collect();
        visible_sorted.sort_unstable();
        let slots: Vec<VisibilitySlot> = visible_sorted
            .into_iter()
            .map(|session_id| VisibilitySlot {
                session_id,
                ..Default::default()
            })
            .collect();

        let mut pinned_sorted: Vec<u64> = self.pinned.iter().copied().collect();
        pinned_sorted.sort_unstable();

        let update = SubscriptionUpdate {
            pinned_sessions: pinned_sorted,
            slots,
            max_video_kbps: self.max_video_kbps,
            receive_all_audio: self.receive_all_audio,
            receive_all_video: self.receive_all_video,
            ..Default::default()
        };

        (self.emit)(update);
    }
}

/// Coalesces visibility/pin mutations and emits a single
/// `SubscriptionUpdate` at most every `COALESCE_WINDOW_MS`.
#[derive(Clone)]
pub(crate) struct SubscriptionCoalescer {
    inner: Rc<RefCell<Inner>>,
    /// Wrapped in `Rc` because the trigger closure (which captures a clone of
    /// the coalescer) may need to outlive any single mutation call.
    trigger: Rc<FlushTrigger>,
}

impl std::fmt::Debug for SubscriptionCoalescer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Closures inside `Inner` aren't `Debug`; show the observable state
        // instead. Use `try_borrow` so logging during a mutation can't
        // panic — fall back to a placeholder if the cell is already
        // borrowed mutably (e.g. by an in-flight `set_visible`).
        match self.inner.try_borrow() {
            Ok(inner) => f
                .debug_struct("SubscriptionCoalescer")
                .field("visible", &inner.visible)
                .field("pinned", &inner.pinned)
                .field("flush_pending", &inner.flush_pending)
                .field("seq", &inner.seq)
                .field("max_video_kbps", &inner.max_video_kbps)
                .field("receive_all_audio", &inner.receive_all_audio)
                .field("receive_all_video", &inner.receive_all_video)
                .finish(),
            Err(_) => f
                .debug_struct("SubscriptionCoalescer")
                .field("state", &"<borrowed>")
                .finish(),
        }
    }
}

impl SubscriptionCoalescer {
    /// Construct with an emit sink and a flush trigger.
    pub(crate) fn new(emit: EmitFn, trigger: FlushTrigger) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                visible: HashSet::new(),
                pinned: HashSet::new(),
                flush_pending: false,
                seq: 0,
                max_video_kbps: DEFAULT_MAX_VIDEO_KBPS,
                receive_all_audio: true,
                // vc-3s8: default to "see everyone" for video too. The SFU
                // applies MAX_VISIBLE_VIDEO so this can't blow up fan-out,
                // and matching the audio default closes the webinar
                // first-joiner gap: a listener that hasn't yet flipped any
                // peer to visible still gets video from late-joining
                // publishers. Once the UI starts emitting set_peer_visibility
                // for the visible tiles, the more restrictive slots tier
                // takes precedence (cap drains pins/slots first, fan-out
                // fills any leftover capacity).
                receive_all_video: true,
                emit,
            })),
            trigger: Rc::new(trigger),
        }
    }

    /// Update visibility for `session_id`. Schedules a coalesced flush if
    /// state actually changed.
    pub(crate) fn set_visible(&self, session_id: u64, visible: bool) {
        let changed = {
            let mut inner = self.inner.borrow_mut();
            if visible {
                inner.visible.insert(session_id)
            } else {
                inner.visible.remove(&session_id)
            }
        };
        if changed {
            self.schedule_flush();
        }
    }

    /// Update pin state for `session_id`. Schedules a coalesced flush if
    /// state actually changed.
    pub(crate) fn set_pinned(&self, session_id: u64, pinned: bool) {
        let changed = {
            let mut inner = self.inner.borrow_mut();
            if pinned {
                inner.pinned.insert(session_id)
            } else {
                inner.pinned.remove(&session_id)
            }
        };
        if changed {
            self.schedule_flush();
        }
    }

    /// Drop `session_id` from both the visible and pinned sets. Called when
    /// a peer leaves the meeting so we don't keep reporting them to the SFU
    /// — otherwise a pinned-then-departed participant would keep bandwidth
    /// reserved for them indefinitely. Schedules a coalesced flush only if
    /// either set actually contained the id.
    pub(crate) fn forget_peer(&self, session_id: u64) {
        let changed = {
            let mut inner = self.inner.borrow_mut();
            // Remove from both sets unconditionally (not short-circuited).
            let visible_changed = inner.visible.remove(&session_id);
            let pinned_changed = inner.pinned.remove(&session_id);
            visible_changed || pinned_changed
        };
        if changed {
            self.schedule_flush();
        }
    }

    /// Drop every tracked peer at once. Used on connection failure when we
    /// tear down all decoder state. Emitting a final empty update lets the
    /// SFU release any reserved bandwidth even if reconnect spins up a new
    /// session immediately after. No-op (no flush scheduled) when both
    /// sets are already empty.
    pub(crate) fn forget_all_peers(&self) {
        let changed = {
            let mut inner = self.inner.borrow_mut();
            let had_any = !inner.visible.is_empty() || !inner.pinned.is_empty();
            inner.visible.clear();
            inner.pinned.clear();
            had_any
        };
        if changed {
            self.schedule_flush();
        }
    }

    /// Schedule a flush via the injected trigger, but only if no flush is
    /// already pending. The flush closure captures a clone of the coalescer
    /// so it can read the *current* state at fire time, not the state at
    /// schedule time (declarative coalescing).
    fn schedule_flush(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.flush_pending {
                return;
            }
            inner.flush_pending = true;
        }
        let me = self.clone();
        let closure: Box<dyn FnOnce()> = Box::new(move || {
            me.flush_now();
        });
        (self.trigger)(closure);
    }

    /// Force an immediate flush. Used by the timer callback (production) and
    /// by tests to drive deterministic emits without waiting on wall-clock
    /// time.
    pub(crate) fn flush_now(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.flush();
    }

    /// Client-local sequence counter. Exposed for tests and future
    /// instrumentation. Not on the wire.
    #[cfg(test)]
    pub(crate) fn seq(&self) -> u64 {
        self.inner.borrow().seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a coalescer whose flush trigger is a no-op (records nothing,
    /// schedules nothing). Tests drive `flush_now()` manually to simulate
    /// the 100ms timer firing.
    fn coalescer_with_capture() -> (SubscriptionCoalescer, Rc<RefCell<Vec<SubscriptionUpdate>>>) {
        let emitted: Rc<RefCell<Vec<SubscriptionUpdate>>> = Rc::new(RefCell::new(Vec::new()));
        let emitted_clone = emitted.clone();
        let emit: EmitFn = Box::new(move |u| {
            emitted_clone.borrow_mut().push(u);
        });
        // No-op trigger: drops the flush closure on the floor. Tests call
        // `flush_now()` manually when they want the flush to fire.
        let trigger: FlushTrigger = Box::new(|_closure| {});
        (SubscriptionCoalescer::new(emit, trigger), emitted)
    }

    #[test]
    fn coalesces_rapid_visibility_changes_into_single_emit() {
        let (c, emitted) = coalescer_with_capture();

        // 5 rapid changes within the 100ms window. The first one schedules
        // a flush; subsequent ones are folded into the same pending flush.
        c.set_visible(1, true);
        c.set_visible(2, true);
        c.set_visible(3, true);
        c.set_visible(2, false); // toggle off
        c.set_visible(4, true);

        // Nothing emitted yet — trigger is a no-op so the flush hasn't fired.
        assert_eq!(emitted.borrow().len(), 0);

        // Simulate the 100ms timer firing.
        c.flush_now();

        let snapshot = emitted.borrow();
        assert_eq!(snapshot.len(), 1, "exactly one coalesced emit");
        let update = &snapshot[0];
        // Final state: sessions 1, 3, 4 visible (2 was toggled off).
        let slot_ids: Vec<u64> = update.slots.iter().map(|s| s.session_id).collect();
        assert_eq!(slot_ids, vec![1, 3, 4]);
        assert!(update.pinned_sessions.is_empty());
        assert_eq!(update.max_video_kbps, DEFAULT_MAX_VIDEO_KBPS);
        assert!(update.receive_all_audio);
        // vc-3s8: coalescer defaults to receive_all_video=true so a webinar
        // listener that flushes an opening update before the first sender
        // joins still gets video from late-joining publishers.
        assert!(
            update.receive_all_video,
            "vc-3s8: receive_all_video must default to true to match \
             receive_all_audio semantics"
        );
        assert_eq!(c.seq(), 1, "sequence counter advanced exactly once");
    }

    #[test]
    fn pinning_a_peer_includes_session_in_pinned_sessions() {
        let (c, emitted) = coalescer_with_capture();

        c.set_visible(42, true);
        c.set_pinned(42, true);
        c.flush_now();

        let snapshot = emitted.borrow();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].pinned_sessions, vec![42]);
        let slot_ids: Vec<u64> = snapshot[0].slots.iter().map(|s| s.session_id).collect();
        assert_eq!(slot_ids, vec![42]);
    }

    #[test]
    fn no_op_changes_do_not_schedule_flush() {
        // Track whether the trigger fired. If a no-op `set_visible` were to
        // schedule a flush we'd see the trigger called.
        let trigger_calls = Rc::new(RefCell::new(0u32));
        let trigger_calls_clone = trigger_calls.clone();
        let emit: EmitFn = Box::new(|_| {});
        let trigger: FlushTrigger = Box::new(move |_closure| {
            *trigger_calls_clone.borrow_mut() += 1;
        });
        let c = SubscriptionCoalescer::new(emit, trigger);

        // First call changes state → schedules.
        c.set_visible(1, true);
        assert_eq!(*trigger_calls.borrow(), 1);

        // Redundant call (already visible) → no schedule.
        c.set_visible(1, true);
        assert_eq!(*trigger_calls.borrow(), 1);

        // Redundant unpin (never pinned) → no schedule.
        c.set_pinned(1, false);
        assert_eq!(*trigger_calls.borrow(), 1);
    }

    #[test]
    fn second_change_during_pending_flush_does_not_re_schedule() {
        // The pending flag should suppress a second trigger call while a
        // flush is in flight. After the flush fires the flag resets and the
        // next change schedules again.
        let trigger_calls = Rc::new(RefCell::new(0u32));
        let trigger_calls_clone = trigger_calls.clone();
        let emit: EmitFn = Box::new(|_| {});
        let trigger: FlushTrigger = Box::new(move |_closure| {
            *trigger_calls_clone.borrow_mut() += 1;
        });
        let c = SubscriptionCoalescer::new(emit, trigger);

        c.set_visible(1, true); // schedules (1)
        c.set_visible(2, true); // pending → no new schedule
        c.set_visible(3, true); // pending → no new schedule
        assert_eq!(*trigger_calls.borrow(), 1);

        c.flush_now(); // clears pending

        c.set_visible(4, true); // schedules again (2)
        assert_eq!(*trigger_calls.borrow(), 2);
    }

    #[test]
    fn forget_peer_clears_pinned_and_visible_state() {
        // Critical lifecycle bug: a pinned peer who disconnects must not
        // stay in `pinned_sessions` (the SFU would reserve bandwidth for
        // them forever). `forget_peer` should scrub both sets and emit a
        // fresh update.
        let (c, emitted) = coalescer_with_capture();

        c.set_visible(7, true);
        c.set_pinned(7, true);
        c.flush_now();
        assert_eq!(emitted.borrow().len(), 1);
        assert_eq!(emitted.borrow()[0].pinned_sessions, vec![7]);

        c.forget_peer(7);
        c.flush_now();
        let snapshot = emitted.borrow();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot[1].pinned_sessions.is_empty());
        assert!(snapshot[1].slots.is_empty());
    }

    #[test]
    fn forget_peer_for_untracked_id_is_noop() {
        // Forgetting a peer we never tracked shouldn't fire the trigger —
        // otherwise unrelated PARTICIPANT_LEFT broadcasts (peers we never
        // had visible) would generate phantom emits.
        let trigger_calls = Rc::new(RefCell::new(0u32));
        let trigger_calls_clone = trigger_calls.clone();
        let emit: EmitFn = Box::new(|_| {});
        let trigger: FlushTrigger = Box::new(move |_closure| {
            *trigger_calls_clone.borrow_mut() += 1;
        });
        let c = SubscriptionCoalescer::new(emit, trigger);

        c.forget_peer(99);
        assert_eq!(*trigger_calls.borrow(), 0);

        // Sanity: a real removal still schedules.
        c.set_visible(1, true);
        assert_eq!(*trigger_calls.borrow(), 1);
        c.flush_now();
        c.forget_peer(1);
        assert_eq!(*trigger_calls.borrow(), 2);
    }

    #[test]
    fn forget_all_peers_clears_both_sets_and_emits_empty() {
        let (c, emitted) = coalescer_with_capture();

        c.set_visible(1, true);
        c.set_visible(2, true);
        c.set_pinned(2, true);
        c.flush_now();
        assert_eq!(emitted.borrow()[0].slots.len(), 2);

        c.forget_all_peers();
        c.flush_now();
        let snapshot = emitted.borrow();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot[1].slots.is_empty());
        assert!(snapshot[1].pinned_sessions.is_empty());
    }

    #[test]
    fn forget_all_peers_when_empty_is_noop() {
        let trigger_calls = Rc::new(RefCell::new(0u32));
        let trigger_calls_clone = trigger_calls.clone();
        let emit: EmitFn = Box::new(|_| {});
        let trigger: FlushTrigger = Box::new(move |_closure| {
            *trigger_calls_clone.borrow_mut() += 1;
        });
        let c = SubscriptionCoalescer::new(emit, trigger);

        c.forget_all_peers();
        assert_eq!(*trigger_calls.borrow(), 0);
    }

    #[test]
    fn flush_reads_state_at_fire_time_not_schedule_time() {
        // The pending flush picks up the *current* state, not a snapshot
        // taken when the flush was scheduled. This is the core of
        // "declarative coalescing".
        let (c, emitted) = coalescer_with_capture();

        c.set_visible(1, true); // schedules
        c.set_visible(2, true); // folds in
        c.set_visible(1, false); // folds in (and net-toggles 1 off)
        c.flush_now();

        let snapshot = emitted.borrow();
        let slot_ids: Vec<u64> = snapshot[0].slots.iter().map(|s| s.session_id).collect();
        assert_eq!(slot_ids, vec![2], "only the final state is emitted");
    }
}
