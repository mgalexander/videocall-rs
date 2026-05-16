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

#[derive(Debug, Clone, Copy)]
pub struct SfuConfig {
    pub mode: SfuMode,
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
        Self { mode }
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
