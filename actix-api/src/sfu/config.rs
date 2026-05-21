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

use std::fmt;
use std::sync::Arc;

use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SfuMode {
    Legacy,
    Sfu,
}

impl fmt::Display for SfuMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SfuMode::Legacy => f.write_str("legacy"),
            SfuMode::Sfu => f.write_str("sfu"),
        }
    }
}

/// Default join-milestone "interesting clips" used when `SFU_JOIN_MILESTONES`
/// is unset. Crossing each of these participant counts emits exactly one
/// `sfu_join_milestone` tracing event per room (see the `JoinRoom` handler in
/// `actors::chat_server`). Kept sorted + deduped to match the parser's
/// post-processing contract.
const DEFAULT_JOIN_MILESTONES: &[u64] = &[10, 50, 100, 250, 500, 1000, 2000, 4000, 8000];

/// SFU runtime configuration, snapshotted from process env once at startup.
///
/// Note: this struct intentionally does NOT derive `Copy` — `milestones` is an
/// `Arc<[u64]>` (cheap to clone, shareable). It is read-mostly and cloned at
/// most a couple of times per process, so the `Arc` overhead is irrelevant.
#[derive(Debug, Clone)]
pub struct SfuConfig {
    pub mode: SfuMode,
    /// Sorted, deduped list of participant-count milestones. Crossing any of
    /// these values from below logs one `sfu_join_milestone` event per room.
    ///
    /// Semantics of `SFU_JOIN_MILESTONES`:
    ///   - **unset** → the sane default list ([`DEFAULT_JOIN_MILESTONES`]) is
    ///     ON. This is the documented "default a sane list" behaviour.
    ///   - **explicit empty string** (`SFU_JOIN_MILESTONES=`) → OFF: an empty
    ///     slice, so the crossing check short-circuits with zero overhead.
    ///   - **explicit list** → parsed (comma-separated u64), trimmed, invalid
    ///     tokens warned-and-skipped, then sorted + deduped.
    ///
    /// Wrapped in `Arc<[u64]>` so the `ChatServer` actor can hold a cheap
    /// shared handle without copying the list on every access.
    pub milestones: Arc<[u64]>,
}

impl SfuConfig {
    pub fn from_env() -> Self {
        let mode = match std::env::var("SFU_MODE") {
            Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "" | "legacy" => SfuMode::Legacy,
                "sfu" => SfuMode::Sfu,
                other => {
                    warn!("unknown SFU_MODE value {:?}, falling back to legacy", other);
                    SfuMode::Legacy
                }
            },
            Err(_) => SfuMode::Legacy,
        };
        let milestones = Self::parse_milestones(std::env::var("SFU_JOIN_MILESTONES").ok());
        Self { mode, milestones }
    }

    /// Parse the `SFU_JOIN_MILESTONES` env value into a sorted, deduped list.
    ///
    /// See [`SfuConfig::milestones`] for the unset/empty/list semantics. Mirrors
    /// the warn-don't-panic philosophy used for `SFU_MODE`: a bad token logs a
    /// warning and is skipped rather than taking the server down.
    fn parse_milestones(raw: Option<String>) -> Arc<[u64]> {
        match raw {
            // Unset → default list ON.
            None => Arc::from(DEFAULT_JOIN_MILESTONES),
            Some(s) => {
                let trimmed = s.trim();
                // Explicit empty string → OFF (no overhead at the crossing site).
                if trimmed.is_empty() {
                    return Arc::from(&[][..]);
                }
                let mut values: Vec<u64> = Vec::new();
                for token in trimmed.split(',') {
                    let token = token.trim();
                    if token.is_empty() {
                        // Tolerate stray/trailing commas (e.g. "10,,50,").
                        continue;
                    }
                    match token.parse::<u64>() {
                        Ok(v) => values.push(v),
                        Err(_) => warn!(
                            "ignoring invalid SFU_JOIN_MILESTONES token {:?} \
                             (expected a non-negative integer)",
                            token
                        ),
                    }
                }
                values.sort_unstable();
                values.dedup();
                Arc::from(values)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // `SFU_MODE` is read from process-global env state. Cargo runs unit tests in
    // parallel by default, so we serialize the env-twiddling tests through a
    // single mutex to keep them from racing each other (and from racing any
    // other test in the binary that happens to read `SFU_MODE`).
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const ENV_KEY: &str = "SFU_MODE";

    /// RAII guard that snapshots `SFU_MODE` on construction and restores it on
    /// drop, so each test leaves the process env exactly as it found it.
    struct EnvGuard {
        prior: Option<String>,
    }

    impl EnvGuard {
        fn new() -> Self {
            Self {
                prior: std::env::var(ENV_KEY).ok(),
            }
        }

        fn set(&self, value: &str) {
            std::env::set_var(ENV_KEY, value);
        }

        fn unset(&self) {
            std::env::remove_var(ENV_KEY);
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prior {
                Some(v) => std::env::set_var(ENV_KEY, v),
                None => std::env::remove_var(ENV_KEY),
            }
        }
    }

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn unset_defaults_to_legacy() {
        let _guard = lock_env();
        let env = EnvGuard::new();
        env.unset();

        assert_eq!(SfuConfig::from_env().mode, SfuMode::Legacy);
    }

    #[test]
    fn legacy_value_parses_as_legacy() {
        let _guard = lock_env();
        let env = EnvGuard::new();
        env.set("legacy");

        assert_eq!(SfuConfig::from_env().mode, SfuMode::Legacy);
    }

    #[test]
    fn sfu_value_parses_as_sfu() {
        let _guard = lock_env();
        let env = EnvGuard::new();
        env.set("sfu");

        assert_eq!(SfuConfig::from_env().mode, SfuMode::Sfu);
    }

    // ===== SFU_JOIN_MILESTONES parsing tests =====
    //
    // `parse_milestones` takes the raw `Option<String>` directly, so these
    // tests do NOT touch process-global env state and need no EnvGuard. The
    // one test that goes through `from_env` (to confirm wiring) takes the env
    // lock and twiddles the milestones key via a dedicated guard.

    const MILESTONES_KEY: &str = "SFU_JOIN_MILESTONES";

    /// RAII guard for the `SFU_JOIN_MILESTONES` env var, mirroring `EnvGuard`.
    struct MilestonesEnvGuard {
        prior: Option<String>,
    }

    impl MilestonesEnvGuard {
        fn new() -> Self {
            Self {
                prior: std::env::var(MILESTONES_KEY).ok(),
            }
        }

        fn set(&self, value: &str) {
            std::env::set_var(MILESTONES_KEY, value);
        }

        fn unset(&self) {
            std::env::remove_var(MILESTONES_KEY);
        }
    }

    impl Drop for MilestonesEnvGuard {
        fn drop(&mut self) {
            match &self.prior {
                Some(v) => std::env::set_var(MILESTONES_KEY, v),
                None => std::env::remove_var(MILESTONES_KEY),
            }
        }
    }

    #[test]
    fn milestones_unset_defaults_to_sane_list() {
        let parsed = SfuConfig::parse_milestones(None);
        assert_eq!(&*parsed, DEFAULT_JOIN_MILESTONES);
    }

    #[test]
    fn milestones_explicit_empty_is_off() {
        // Explicit empty string disables the feature entirely.
        let parsed = SfuConfig::parse_milestones(Some(String::new()));
        assert!(parsed.is_empty());
        // Whitespace-only is treated the same as empty.
        let parsed_ws = SfuConfig::parse_milestones(Some("   ".to_string()));
        assert!(parsed_ws.is_empty());
    }

    #[test]
    fn milestones_explicit_list_parses_sorted_deduped() {
        // Out of order, with duplicates and surrounding whitespace.
        let parsed = SfuConfig::parse_milestones(Some(" 100, 10 ,50,10 ".to_string()));
        assert_eq!(&*parsed, &[10u64, 50, 100]);
    }

    #[test]
    fn milestones_invalid_tokens_warn_and_skip() {
        // "abc" and "-5" are not valid u64; stray commas are tolerated. The
        // valid tokens survive, sorted + deduped.
        let parsed = SfuConfig::parse_milestones(Some("10,abc,50,,-5,100,50".to_string()));
        assert_eq!(&*parsed, &[10u64, 50, 100]);
    }

    #[test]
    fn milestones_all_invalid_yields_empty() {
        let parsed = SfuConfig::parse_milestones(Some("abc,xyz".to_string()));
        assert!(parsed.is_empty());
    }

    #[test]
    fn from_env_threads_milestones_through() {
        let _guard = lock_env();
        let env = MilestonesEnvGuard::new();
        env.set("5,1,5,3");
        assert_eq!(&*SfuConfig::from_env().milestones, &[1u64, 3, 5]);

        // And unset → default list ON via the full from_env path.
        env.unset();
        assert_eq!(&*SfuConfig::from_env().milestones, DEFAULT_JOIN_MILESTONES);
    }

    #[test]
    fn invalid_value_falls_back_to_legacy() {
        // Documented choice: an unrecognized `SFU_MODE` logs a warning and
        // falls back to `Legacy` rather than panicking. The reasoning is that
        // a typo in a deployment env var should not take the server down --
        // operators see the warning, traffic keeps flowing on the safe
        // default, and they can correct the value without an outage.
        let _guard = lock_env();
        let env = EnvGuard::new();
        env.set("invalid");

        assert_eq!(SfuConfig::from_env().mode, SfuMode::Legacy);
    }
}
