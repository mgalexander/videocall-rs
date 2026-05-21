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

use std::net::ToSocketAddrs;

use actix::Actor;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use tracing::{error, info};

use sec_api::actors::chat_server::ChatServer;
use sec_api::server_diagnostics::ServerDiagnostics;
use sec_api::session_manager::SessionManager;
use sec_api::sfu::{SfuConfig, SfuMode};
use sec_api::version;
use sec_api::webtransport::{self, Certs};

/// vc-zf8k (Bead B-b): forwarding-liveness-aware health endpoint.
///
/// Returns 200 when the SFU is forwarding in steady state OR when it is
/// idle/empty (nothing to forward). Returns 503 ONLY when forwarding SHOULD be
/// happening (a room with receivers + publishers was observed within the
/// threshold) but no forward has happened within the threshold — i.e. the
/// "forwarding-dead zombie" condition the DEFECT-JOINHANDLE-PANIC spec calls
/// out. The decision reuses the vc-9eh liveness semantics via
/// [`sec_api::sfu::forwarding_health::forwarding_health_decision`].
async fn health_responder() -> impl Responder {
    use sec_api::sfu::forwarding_health::{
        forwarding_health_decision, forwarding_silence_threshold, global, now_millis, HealthStatus,
    };
    let snap = global().snapshot();
    let status = forwarding_health_decision(
        now_millis(),
        snap.last_forward_ms,
        snap.last_should_forward_ms,
        forwarding_silence_threshold(),
    );
    match status {
        HealthStatus::Healthy => HttpResponse::Ok().body("Ok"),
        HealthStatus::ForwardingStalled => {
            error!(
                last_forward_ms = snap.last_forward_ms,
                last_should_forward_ms = snap.last_should_forward_ms,
                "/healthz: forwarding stalled while receivers+publishers present \
                 — reporting 503 so the liveness probe can restart the pod"
            );
            HttpResponse::ServiceUnavailable().body("forwarding stalled")
        }
    }
}

/// vc-zf8k (Bead B-a): install the fail-fast panic hook.
///
/// Installed FIRST in `main`, before any task is spawned, so a panic on ANY
/// thread/task (per-session bridge writer, per-room dispatcher, health server,
/// runtime driver) terminates the WHOLE process. tokio's default behavior only
/// unwinds the panicking task, which is exactly how the
/// DEFECT-JOINHANDLE-PANIC zombie arose: forwarding tasks died, but the
/// process and the health server survived with a static 200 `/healthz` and 0
/// k8s restarts. We chain to the existing default hook so the panic message,
/// location, and backtrace are still printed, THEN `std::process::abort()` to
/// crash non-zero so k8s restarts the pod and forwarding recovers.
///
/// We use an explicit hook rather than `panic = "abort"` in the Cargo profile
/// so unit/integration tests that intentionally panic-and-catch (e.g.
/// `#[should_panic]`, `catch_unwind`) are unaffected — the hook only runs in
/// this binary's process.
fn install_fail_fast_panic_hook() {
    let default_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // Print the standard panic message/location/backtrace first.
        default_panic_hook(panic_info);
        // Also surface via tracing for log aggregation, then crash the process.
        error!(
            "FATAL: panic in SFU process — aborting so k8s restarts the pod and \
             forwarding recovers: {}",
            panic_info
        );
        std::process::abort();
    }));
}

#[actix_rt::main]
async fn main() {
    install_fail_fast_panic_hook();

    // vc-zf8k (Bead B-a): the panic-hook self-test re-execs THIS binary with
    // `SFU_PANIC_HOOK_SELFTEST=1` set, expecting the hook to abort the process.
    // Trigger the panic AFTER the hook is installed and return; the integration
    // test asserts the abnormal (SIGABRT) exit. No-op in normal operation.
    if std::env::var_os("SFU_PANIC_HOOK_SELFTEST").is_some() {
        panic!("forwarding-task panic (panic-hook self-test)");
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_writer(std::io::stderr)
        .init();

    // vc-8wd: arm targeted SFU tracing from SFU_TRACE_ROOM/SESSION (read
    // once). No-op when unset, which is the default.
    sec_api::sfu::trace::init();

    info!("Starting WebTransport server with actor-based session handling");

    // SFU_TRANSPORT_KIND identifies this binary's transport family for the
    // wave-3 ADMISSION_DECISION{REDIRECT} DNS template (bead vc-8oa / p6-5).
    // Set unconditionally at startup so the JoinRoom handler doesn't have to
    // care which binary it's hosted in; the chart can override this for
    // non-standard deployments.
    if std::env::var_os("SFU_TRANSPORT_KIND").is_none() {
        std::env::set_var("SFU_TRANSPORT_KIND", "webtransport");
    }

    let pod_name = std::env::var("POD_NAME").ok();
    let self_ordinal = sec_api::sfu::affinity::self_ordinal_from_env();
    let replicas = sec_api::sfu::affinity::replicas_from_env();
    info!(
        pod_name = ?pod_name,
        replicas,
        self_ordinal = ?self_ordinal,
        "affinity init"
    );
    // p6-5 follow-up: surface a misconfigured POD_NAME at startup rather
    // than letting the JoinRoom handler silently skip redirects on every
    // join. `self_ordinal_from_env()` returns `None` ONLY when POD_NAME
    // is set but cannot be parsed as `<name>-<u32>`.
    if pod_name.is_some() && self_ordinal.is_none() {
        tracing::warn!(
            pod_name = ?pod_name,
            "POD_NAME is set but the trailing ordinal could not be parsed; \
             room→pod affinity redirects will be DISABLED for this pod. \
             Expected form: <statefulset>-<ordinal>, e.g. \
             rustlemania-webtransport-0"
        );
    }

    let sfu_config = SfuConfig::from_env();
    info!("sfu mode: {}", sfu_config.mode);
    if sfu_config.mode == SfuMode::Sfu {
        info!("sfu mode active (no-op shim)");
    }

    // Connect to NATS. Auth/TLS posture is driven by the NATS_USER /
    // NATS_PASSWORD / NATS_TLS / NATS_TLS_CA env vars; see
    // `nats_connect` module + sfu-update/audits/nats-acl-audit.md.
    let nats_url = std::env::var("NATS_URL").expect("NATS_URL env var must be defined");
    let nats_client = sec_api::nats_connect::connect(&nats_url)
        .await
        .expect("Failed to connect to NATS");
    info!("Connected to NATS at {}", nats_url);

    // Start ChatServer actor.
    // vc-ud6o E3: grab the shared connection-state handle BEFORE `.start()`
    // consumes the actor, then thread it through `webtransport::start` so each
    // `SessionLogic` can read the `Active` gate off-actor.
    let chat_server_actor = ChatServer::new(nats_client.clone()).await;
    let connection_states = chat_server_actor.connection_states_handle();
    let chat_server = chat_server_actor.start();
    info!("ChatServer actor started");

    // Create SessionManager
    let session_manager = SessionManager::new();

    // Create connection tracker with message channel
    let (connection_tracker, tracker_sender, tracker_receiver) =
        ServerDiagnostics::new_with_channel(nats_client.clone());

    // Start the connection tracker message processing task
    let connection_tracker = std::sync::Arc::new(connection_tracker);
    let tracker_task = connection_tracker.clone();
    tokio::spawn(async move {
        tracker_task.run_message_loop(tracker_receiver).await;
    });

    // Health server setup
    let health_listen = std::env::var("HEALTH_LISTEN_URL")
        .expect("expected HEALTH_LISTEN_URL to be set")
        .to_socket_addrs()
        .expect("expected HEALTH_LISTEN_URL to be a valid socket address")
        .next()
        .expect("expected HEALTH_LISTEN_URL to be a valid socket address");

    // WebTransport server options
    let opt = webtransport::WebTransportOpt {
        listen: std::env::var("LISTEN_URL")
            .expect("expected LISTEN_URL to be set")
            .to_socket_addrs()
            .expect("expected LISTEN_URL to be a valid socket address")
            .next()
            .expect("expected LISTEN_URL to be a valid socket address"),
        certs: Certs {
            key: std::env::var("KEY_PATH")
                .expect("expected KEY_PATH to be set")
                .into(),
            cert: std::env::var("CERT_PATH")
                .expect("expected CERT_PATH to be set")
                .into(),
        },
    };

    // Start health server
    actix_rt::spawn(async move {
        info!("Starting health/metrics HTTP server: {:?}", health_listen);
        let server = HttpServer::new(|| {
            App::new()
                .route("/healthz", web::get().to(health_responder))
                .route("/version", web::get().to(version::webtransport_version))
        });

        match server.bind(&health_listen) {
            Ok(server) => {
                info!("Health server successfully bound to: {:?}", health_listen);
                if let Err(e) = server.run().await {
                    error!("Health server runtime error: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to bind health server to {:?}: {}", health_listen, e);
            }
        }
    });

    // Start WebTransport server with ChatServer
    let _ = actix_rt::spawn(async move {
        if let Err(e) = webtransport::start(
            opt,
            chat_server,
            nats_client,
            tracker_sender,
            session_manager,
            connection_states,
        )
        .await
        {
            error!("WebTransport server error: {}", e);
        }
    })
    .await;
}
