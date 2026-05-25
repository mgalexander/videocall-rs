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

//! NATS connection helper for meeting-api.
//!
//! Mirrors the env-var contract established by
//! `actix-api/src/nats_connect.rs` so both services pick up the same auth+TLS
//! posture without duplicating wiring at the call sites.
//!
//! NOTE: This is intentionally a near-clone of the actix-api helper rather
//! than a shared crate. Extracting a `nats-connect` crate is tracked
//! separately; for now we keep the surface area small and per-service.
//!
//! ## Env vars (read on every call)
//!
//! - `NATS_USER` + `NATS_PASSWORD` — if **both** are set, NATS basic auth is
//!   used. If only one is set, that's a misconfiguration and we refuse to
//!   connect (fail-loud is better than fail-open).
//! - `NATS_TLS` — `1` / `true` / `yes` (case-insensitive) enables TLS on the
//!   client port. The CA cert is picked up from the system trust store by
//!   default; override with `NATS_TLS_CA` (path to a PEM file).
//!
//! Unset env vars → no auth, plaintext connection (back-compat with the
//! existing production deployments that have `auth.enabled: false`).
//!
//! ## Connect tunables
//!
//! - `ping_interval` is locked at 10 s (matches the prior hardcoded value at
//!   the call sites; not env-tunable on purpose).

use std::path::PathBuf;
use std::time::Duration;

use async_nats::Client;
use async_nats::ConnectOptions;

/// Possible failure modes from [`connect`].
#[derive(Debug)]
pub enum NatsConnectError {
    /// Connection-level failure from `async_nats::ConnectOptions::connect`.
    /// We capture the upstream error as a string so this variant stays stable
    /// across `async-nats` versions (its error type is a parameterised
    /// `async_nats::Error<ConnectErrorKind>` boxed dyn-Error).
    Transport(String),
    /// `NATS_USER` set without `NATS_PASSWORD` (or vice versa).
    PartialCredentials,
    /// `NATS_TLS_CA` was set but pointed at a file we couldn't read.
    CaReadError(std::io::Error, PathBuf),
}

impl std::fmt::Display for NatsConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "NATS connect failed: {e}"),
            Self::PartialCredentials => write!(
                f,
                "NATS credentials misconfigured: NATS_USER and NATS_PASSWORD \
                 must be set together (or both unset for no-auth back-compat)"
            ),
            Self::CaReadError(e, path) => {
                write!(f, "NATS_TLS_CA read failed at {}: {e}", path.display())
            }
        }
    }
}

impl std::error::Error for NatsConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CaReadError(e, _) => Some(e),
            Self::Transport(_) | Self::PartialCredentials => None,
        }
    }
}

/// Returns `true` when `NATS_USER` is set to a non-empty value.
///
/// The meeting-api binary uses this to decide whether a connect failure
/// should be fatal (auth explicitly requested) or merely logged (legacy
/// anonymous deployments).
pub fn auth_requested() -> bool {
    std::env::var("NATS_USER")
        .map(|s| !s.is_empty())
        .unwrap_or(false)
}

/// Connect to NATS at `url`, applying auth/TLS from the environment.
///
/// Logs the resulting posture at INFO (`auth=on/off`, `tls=on/off`) so
/// operators can confirm at a glance whether a pod is hardened. The URL
/// itself is logged as well; if your URL contains credentials, *don't* —
/// pass them via `NATS_USER`/`NATS_PASSWORD` instead.
pub async fn connect(url: &str) -> Result<Client, NatsConnectError> {
    let user = std::env::var("NATS_USER").ok().filter(|s| !s.is_empty());
    let password = std::env::var("NATS_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());

    if user.is_some() != password.is_some() {
        return Err(NatsConnectError::PartialCredentials);
    }

    let tls_enabled = std::env::var("NATS_TLS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    let ca_path = std::env::var("NATS_TLS_CA").ok().filter(|s| !s.is_empty());

    let mut opts = ConnectOptions::new()
        .require_tls(tls_enabled)
        .ping_interval(Duration::from_secs(10));

    if let (Some(u), Some(p)) = (user.as_ref(), password.as_ref()) {
        opts = opts.user_and_password(u.clone(), p.clone());
    }

    if tls_enabled {
        if let Some(path) = ca_path {
            let pathbuf = PathBuf::from(&path);
            // Validate the file is readable before handing it to async-nats,
            // so the error message says where the file was.
            if let Err(e) = std::fs::metadata(&pathbuf) {
                return Err(NatsConnectError::CaReadError(e, pathbuf));
            }
            opts = opts.add_root_certificates(pathbuf);
        }
        // Otherwise async-nats falls back to the system root store.
    }

    tracing::info!(
        target: "nats_connect",
        nats_url = %url,
        auth = if user.is_some() { "on" } else { "off" },
        tls = if tls_enabled { "on" } else { "off" },
        "connecting to NATS",
    );

    opts.connect(url)
        .await
        .map_err(|e| NatsConnectError::Transport(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex;

    // Env-var tests must serialise to avoid stepping on each other. We use
    // `tokio::sync::Mutex` because the guard is held across `.await` points
    // (the synchronous env-var reads inside `connect` must observe a stable
    // env, and the test holds the lock until the future resolves).
    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    fn clear_env() {
        for k in ["NATS_USER", "NATS_PASSWORD", "NATS_TLS", "NATS_TLS_CA"] {
            std::env::remove_var(k);
        }
    }

    #[tokio::test]
    async fn partial_credentials_is_an_error() {
        let _guard = ENV_LOCK.lock().await;
        clear_env();
        std::env::set_var("NATS_USER", "alice");
        // password unset on purpose
        let result = connect("nats://127.0.0.1:1").await;
        assert!(matches!(result, Err(NatsConnectError::PartialCredentials)));

        clear_env();
        std::env::set_var("NATS_PASSWORD", "secret");
        let result = connect("nats://127.0.0.1:1").await;
        assert!(matches!(result, Err(NatsConnectError::PartialCredentials)));

        clear_env();
    }

    #[tokio::test]
    async fn ca_path_missing_file_is_an_error() {
        let _guard = ENV_LOCK.lock().await;
        clear_env();
        std::env::set_var("NATS_TLS", "true");
        std::env::set_var("NATS_TLS_CA", "/nonexistent/ca.pem");
        let result = connect("nats://127.0.0.1:1").await;
        assert!(matches!(result, Err(NatsConnectError::CaReadError(..))));
        clear_env();
    }

    #[tokio::test]
    async fn auth_requested_reflects_nats_user() {
        let _guard = ENV_LOCK.lock().await;
        clear_env();
        assert!(!auth_requested());
        std::env::set_var("NATS_USER", "alice");
        assert!(auth_requested());
        std::env::set_var("NATS_USER", "");
        assert!(!auth_requested());
        clear_env();
    }
}
