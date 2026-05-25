// SPDX-License-Identifier: Apache-2.0 OR MIT
//! Integration matrix for `sec_api::nats_connect` against real NATS servers.
//!
//! Runs four cells of (client-creds, server-auth) and asserts the helper
//! behaves the way the Phase A-D rollout depends on:
//!
//!   | client                 | server         | expected                    |
//!   |------------------------|----------------|-----------------------------|
//!   | A. no creds            | auth-enabled   | error (Authorization)       |
//!   | B. creds set in env    | auth-enabled   | success                     |
//!   | C. creds set in env    | no-auth        | success (creds ignored)     |
//!   | D. no creds            | no-auth        | success (today's baseline)  |
//!
//! Cell C is the load-bearing one for zero-downtime rollout: it proves that
//! Phase B (deploy SFU pods with creds) is safe while Phase C (enable auth
//! on the server) has not yet run. See sfu-update/audits/nats-auth-rollout.md.
//!
//! The test reads the two server URLs from env so it can run against any
//! local sandbox. Default fits the docker setup in
//! `sfu-update/audits/nats-sandbox-up.sh`:
//!
//!   NATS_TEST_AUTH_URL=nats://127.0.0.1:24222  (basic auth enabled)
//!   NATS_TEST_NOAUTH_URL=nats://127.0.0.1:24223
//!   NATS_TEST_USER=sfu-cluster
//!   NATS_TEST_PASSWORD=testpass123
//!
//! Skipped silently if either URL is unset or unreachable; only fails when
//! the matrix is testable but a cell misbehaves.

use std::time::Duration;

use sec_api::nats_connect::{self, NatsConnectError};

/// Set the env vars `connect()` reads so each cell has a clean state.
/// Returns a guard that resets env on drop so cells don't pollute each other.
struct EnvGuard;
impl Drop for EnvGuard {
    fn drop(&mut self) {
        for k in ["NATS_USER", "NATS_PASSWORD", "NATS_TLS", "NATS_TLS_CA"] {
            std::env::remove_var(k);
        }
    }
}

fn with_clean_env() -> EnvGuard {
    for k in ["NATS_USER", "NATS_PASSWORD", "NATS_TLS", "NATS_TLS_CA"] {
        std::env::remove_var(k);
    }
    EnvGuard
}

/// Try a TCP probe before running the matrix; skip if servers aren't up.
async fn server_reachable(url: &str) -> bool {
    // Strip the nats:// prefix and parse host:port.
    let stripped = url.strip_prefix("nats://").unwrap_or(url);
    let host_port = stripped.rsplit_once('@').map(|x| x.1).unwrap_or(stripped);
    tokio::time::timeout(
        Duration::from_millis(500),
        tokio::net::TcpStream::connect(host_port),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
#[ignore = "needs local NATS sandboxes; run with `cargo test --test nats_auth_integration -- --ignored`"]
async fn auth_matrix_cell_a_nocreds_to_auth_server_refused() {
    let auth_url = env("NATS_TEST_AUTH_URL", "nats://127.0.0.1:24222");
    if !server_reachable(&auth_url).await {
        eprintln!("skipped: {} not reachable", auth_url);
        return;
    }

    let _g = with_clean_env();
    let result = nats_connect::connect(&auth_url).await;
    assert!(
        result.is_err(),
        "expected refusal when client has no creds and server has auth, got: {result:?}",
    );
    if let Err(NatsConnectError::Transport(msg)) = result {
        // The async-nats error message should mention authorization. This is
        // diagnostic, not strict, so don't fail the test if the wording shifts.
        eprintln!("cell A error message: {msg}");
    }
}

#[tokio::test]
#[ignore = "needs local NATS sandboxes; run with `cargo test --test nats_auth_integration -- --ignored`"]
async fn auth_matrix_cell_b_creds_to_auth_server_accepted() {
    let auth_url = env("NATS_TEST_AUTH_URL", "nats://127.0.0.1:24222");
    if !server_reachable(&auth_url).await {
        eprintln!("skipped: {} not reachable", auth_url);
        return;
    }

    let _g = with_clean_env();
    std::env::set_var("NATS_USER", env("NATS_TEST_USER", "sfu-cluster"));
    std::env::set_var("NATS_PASSWORD", env("NATS_TEST_PASSWORD", "testpass123"));
    let result = nats_connect::connect(&auth_url).await;
    assert!(
        result.is_ok(),
        "expected success when client and server both authenticate, got: {result:?}",
    );
}

#[tokio::test]
#[ignore = "needs local NATS sandboxes; run with `cargo test --test nats_auth_integration -- --ignored`"]
async fn auth_matrix_cell_c_creds_to_noauth_server_accepted() {
    // The critical "Phase B is safe" cell. If this fails, deploying SFU pods
    // with creds BEFORE flipping NATS auth on will break the cluster.
    let noauth_url = env("NATS_TEST_NOAUTH_URL", "nats://127.0.0.1:24223");
    if !server_reachable(&noauth_url).await {
        eprintln!("skipped: {} not reachable", noauth_url);
        return;
    }

    let _g = with_clean_env();
    std::env::set_var("NATS_USER", env("NATS_TEST_USER", "sfu-cluster"));
    std::env::set_var("NATS_PASSWORD", env("NATS_TEST_PASSWORD", "testpass123"));
    let result = nats_connect::connect(&noauth_url).await;
    assert!(
        result.is_ok(),
        "EXPECTED Phase B safety: client-with-creds must connect to a still-\
         permissive NATS server. If this fails the rollout is not zero-downtime. \
         got: {result:?}",
    );
}

#[tokio::test]
#[ignore = "needs local NATS sandboxes; run with `cargo test --test nats_auth_integration -- --ignored`"]
async fn auth_matrix_cell_d_nocreds_to_noauth_server_accepted() {
    let noauth_url = env("NATS_TEST_NOAUTH_URL", "nats://127.0.0.1:24223");
    if !server_reachable(&noauth_url).await {
        eprintln!("skipped: {} not reachable", noauth_url);
        return;
    }

    let _g = with_clean_env();
    let result = nats_connect::connect(&noauth_url).await;
    assert!(
        result.is_ok(),
        "today's baseline: no-creds to no-auth server should always work, got: {result:?}",
    );
}
