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
 *
 * Unless you explicitly state otherwise, any contribution intentionally
 * submitted for inclusion in the work by you, as defined in the Apache-2.0
 * license, shall be dual licensed as above, without any additional terms or
 * conditions.
 */

//! P5 wave-5 (p5-8) standalone unit tests for the outbound priority queue.
//!
//! Complements the inline tests inside `sfu::priority_queue` (p5-1 / p5-2)
//! by locking down the contract on the four behaviors most likely to regress
//! under future drop-policy or scheduler tweaks:
//!
//! 1. Strict priority ordering — given equal backlogs in P0/P1/P4 (each below
//!    the fairness quantum), the consumer drains P0 in its entirety before
//!    P1, then P1 before P4.
//! 2. Fairness quantum bounds starvation — a P0 packet injected after a
//!    pre-existing P4 backlog must be served within one fairness window
//!    (≤ `FAIRNESS_QUANTUM` further P4 drains), never after all 100.
//! 3. `TailDropOldest` on P3VideoBase evicts the OLDEST queued entry and
//!    admits the new packet at the tail; the head shifts.
//! 4. `HeadDropOldest` on P4Enhancement keeps the in-flight backlog and
//!    rejects the new entry — the queue still drains the original 256
//!    packets, in order, with the overflow attempt absent.
//! 5. `NeverDrop` on P0Control refuses sends past capacity with
//!    `SendOutcome::Refused(SendError)`.

use bytes::Bytes;

use crate::sfu::priority_queue::{
    Class, DropPolicy, PriorityReceiver, PrioritySender, SendError, SendOutcome, FAIRNESS_QUANTUM,
};

fn payload(tag: &str, n: usize) -> Bytes {
    Bytes::from(format!("{tag}#{n}").into_bytes())
}

#[tokio::test]
async fn strict_priority_order() {
    // Push 3 packets each to P0Control, P1Audio, and P4Enhancement.
    // recv() must return all 3 P0 packets first, then all 3 P1, then all 3 P4.
    // Per-class count of 3 keeps us well below FAIRNESS_QUANTUM (8) so the
    // quantum never kicks in — this isolates strict priority.
    let (sender, channels) = PrioritySender::new();
    let per_class = 3;
    let classes = [Class::P0Control, Class::P1Audio, Class::P4Enhancement];

    for class in classes {
        for i in 0..per_class {
            let outcome = sender.send(class, payload(&format!("{class:?}"), i));
            assert_eq!(outcome, SendOutcome::Sent, "fill {class:?}#{i}");
        }
    }

    let mut rx = PriorityReceiver::new(channels);
    for class in classes {
        for i in 0..per_class {
            let got = rx.recv().await.expect("packet must be available");
            assert_eq!(
                got,
                payload(&format!("{class:?}"), i),
                "expected {class:?}#{i} next under strict priority"
            );
        }
    }

    drop(sender);
    assert!(rx.recv().await.is_none(), "recv must terminate after drain");
}

#[tokio::test]
async fn fairness_quantum_prevents_starvation() {
    // Bead spec: push 100 packets to P4Enhancement; after 4 P4 packets have
    // been drained, push 1 P0 control. The P0 packet MUST be returned within
    // the next FAIRNESS_QUANTUM (8) recv calls — never after all 100 P4.
    //
    // This is the inverse of the wave-2 inline test
    // `priority_receiver_continuous_p0_yields_to_p4_every_quantum`, which
    // exercises P0 starving P4. Here we ensure a low-priority backlog cannot
    // monopolize the receiver against a single control packet — exactly the
    // anti-starvation invariant that makes the fairness quantum worth its
    // small inversion cost.
    let (sender, channels) = PrioritySender::new();
    let total_p4 = 100;
    for i in 0..total_p4 {
        assert_eq!(
            sender.send(Class::P4Enhancement, payload("p4", i)),
            SendOutcome::Sent,
            "fill p4#{i}"
        );
    }
    let mut rx = PriorityReceiver::new(channels);

    // Drain the first 4 P4 packets — this advances the P4 drain counter to 4,
    // well below the 8-quantum boundary, so strict priority still holds.
    for i in 0..4 {
        let got = rx.recv().await.expect("p4 packet must be available");
        assert_eq!(got, payload("p4", i), "first-4 P4 drain prefix");
    }

    // Inject a P0 control packet mid-burst.
    assert_eq!(
        sender.send(Class::P0Control, payload("p0", 0)),
        SendOutcome::Sent,
        "p0 injection should succeed"
    );

    // Bead acceptance: "within 8 calls". Implementation guarantees the
    // stronger property — strict priority resumes immediately because the
    // P4 drain counter (4) is below FAIRNESS_QUANTUM (8), so the very next
    // recv MUST return the P0 packet. We assert the stronger property since
    // weakening to a bounded loop would mask a real preemption regression.
    let next = rx.recv().await.expect("p0 packet must arrive");
    assert_eq!(
        next,
        payload("p0", 0),
        "P0 must preempt a lower-priority P4 backlog — got {next:?} instead"
    );

    // Sanity: the remaining 96 P4 packets drain in FIFO order after the P0
    // preempt. If a quantum reset bug ever loses the in-flight P4 cursor,
    // this would surface as an out-of-order or short drain.
    for i in 4..total_p4 {
        let got = rx.recv().await.expect("remaining p4 packet must drain");
        assert_eq!(got, payload("p4", i), "P4 backlog resumes after P0 preempt");
    }

    drop(sender);
    assert!(rx.recv().await.is_none(), "recv must terminate after drain");
}

#[tokio::test]
async fn drop_policy_tail_drop_oldest() {
    // P3VideoBase: capacity=256, policy=TailDropOldest.
    // Push 256 distinct packets — all admitted. Push a 257th — outcome is
    // SendOutcome::Dropped(P3VideoBase, "tail_drop_oldest"), the head ("0")
    // is evicted, and the new "overflow" entry sits at the tail.
    assert_eq!(Class::P3VideoBase.capacity(), 256);
    assert_eq!(Class::P3VideoBase.drop_policy(), DropPolicy::TailDropOldest);

    let (sender, mut channels) = PrioritySender::new();
    let cap = Class::P3VideoBase.capacity();

    for i in 0..cap {
        assert_eq!(
            sender.send(Class::P3VideoBase, payload("v", i)),
            SendOutcome::Sent,
            "fill v#{i} should succeed"
        );
    }

    let outcome = sender.send(Class::P3VideoBase, Bytes::from_static(b"overflow"));
    match outcome {
        SendOutcome::Dropped(Class::P3VideoBase, reason) => {
            assert_eq!(reason, "tail_drop_oldest");
        }
        other => panic!("expected Dropped(P3VideoBase, tail_drop_oldest), got {other:?}"),
    }

    // Drain and verify: queue still holds `cap` entries, original head ("v#0")
    // is gone, the new head is "v#1", and the new tail is "overflow".
    let mut drained = Vec::with_capacity(cap);
    while let Some(b) = channels.p3_video_base.try_recv() {
        drained.push(b);
    }
    assert_eq!(drained.len(), cap, "queue should still hold {cap} items");
    assert_eq!(
        &drained[0][..],
        payload("v", 1).as_ref(),
        "head shifted to v#1"
    );
    assert_eq!(
        &drained[drained.len() - 1][..],
        b"overflow",
        "new entry sits at the tail"
    );
    assert!(
        drained.iter().all(|b| &b[..] != payload("v", 0).as_ref()),
        "v#0 (original head) must have been evicted"
    );
}

#[tokio::test]
async fn drop_policy_head_drop_oldest() {
    // P4Enhancement: capacity=256, policy=HeadDropOldest.
    // Per the doc comment on DropPolicy::HeadDropOldest, "head drop" keeps
    // the in-flight head and drops the NEW entry. After filling capacity and
    // attempting one more send, the queue still holds the original 256 in
    // order, "overflow" is not present, and the outcome is
    // SendOutcome::Dropped(P4Enhancement, "head_drop_new").
    assert_eq!(Class::P4Enhancement.capacity(), 256);
    assert_eq!(
        Class::P4Enhancement.drop_policy(),
        DropPolicy::HeadDropOldest
    );

    let (sender, mut channels) = PrioritySender::new();
    let cap = Class::P4Enhancement.capacity();

    for i in 0..cap {
        assert_eq!(
            sender.send(Class::P4Enhancement, payload("e", i)),
            SendOutcome::Sent,
            "fill e#{i} should succeed"
        );
    }

    let outcome = sender.send(Class::P4Enhancement, Bytes::from_static(b"overflow"));
    match outcome {
        SendOutcome::Dropped(Class::P4Enhancement, reason) => {
            assert_eq!(
                reason, "head_drop_new",
                "HeadDropOldest drops the new entry"
            );
        }
        other => panic!("expected Dropped(P4Enhancement, head_drop_new), got {other:?}"),
    }

    let mut drained = Vec::with_capacity(cap);
    while let Some(b) = channels.p4_enhancement.try_recv() {
        drained.push(b);
    }
    assert_eq!(drained.len(), cap, "in-flight backlog preserved intact");
    for (i, b) in drained.iter().enumerate() {
        assert_eq!(
            &b[..],
            payload("e", i).as_ref(),
            "FIFO order preserved at slot {i}"
        );
    }
    assert!(
        drained.iter().all(|b| &b[..] != b"overflow"),
        "head_drop policy must reject the new entry"
    );
}

#[tokio::test]
async fn never_drop_class_refuses_when_full() {
    // P0Control: capacity=32, policy=NeverDrop.
    // Fill all 32 slots successfully, then attempt a 33rd send — the outcome
    // must be SendOutcome::Refused(SendError), and the queue is unchanged
    // (still holds the original 32 in order).
    assert_eq!(Class::P0Control.capacity(), 32);
    assert_eq!(Class::P0Control.drop_policy(), DropPolicy::NeverDrop);

    let (sender, mut channels) = PrioritySender::new();
    let cap = Class::P0Control.capacity();

    for i in 0..cap {
        assert_eq!(
            sender.send(Class::P0Control, payload("c", i)),
            SendOutcome::Sent,
            "fill c#{i} should succeed"
        );
    }

    let outcome = sender.send(Class::P0Control, Bytes::from_static(b"overflow"));
    assert_eq!(
        outcome,
        SendOutcome::Refused(SendError),
        "NeverDrop must Refuse, not Drop"
    );

    // Drain and verify the refused entry never landed in the queue.
    let mut drained = Vec::with_capacity(cap);
    while let Some(b) = channels.p0_control.try_recv() {
        drained.push(b);
    }
    assert_eq!(drained.len(), cap, "refusal must not alter queue contents");
    for (i, b) in drained.iter().enumerate() {
        assert_eq!(
            &b[..],
            payload("c", i).as_ref(),
            "FIFO order intact at slot {i}"
        );
    }
    assert!(
        drained.iter().all(|b| &b[..] != b"overflow"),
        "refused entry must not be in queue"
    );
}

// Sanity check that FAIRNESS_QUANTUM remains the value the bead spec assumes
// (8). If this constant ever changes, this file's acceptance reasoning needs
// to be revisited.
#[test]
fn fairness_quantum_constant_matches_bead_spec() {
    assert_eq!(FAIRNESS_QUANTUM, 8);
}
