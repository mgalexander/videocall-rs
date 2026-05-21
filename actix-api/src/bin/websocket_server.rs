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

use actix::Actor;
use actix_cors::Cors;
use actix_web::{
    cookie::{
        time::{Duration, OffsetDateTime},
        Cookie, SameSite,
    },
    error, get, web, App, Error, HttpRequest, HttpResponse, HttpServer,
};
use reqwest::header::LOCATION;
use sec_api::{
    actors::chat_server::ChatServer,
    auth::{
        fetch_oauth_request, generate_and_store_oauth_request, request_token, upsert_user,
        AuthRequest,
    },
    db::{get_pool, PostgresPool},
    lobby::{ws_connect, ws_connect_authenticated},
    models::{AppConfig, AppState},
    server_diagnostics::ServerDiagnostics,
    session_manager::SessionManager,
    sfu::{SfuConfig, SfuMode},
    version,
};
use tracing::{debug, error, info};
use videocall_types::truthy;

const SCOPE: &str = "email profile";

/**
 * Health check endpoint for k8s liveness/readiness probes.
 *
 * Returns HTTP 200 with a plain "Ok" body. Mirrors the webtransport
 * server's `health_responder`. Requires no auth so probes can reach it
 * regardless of OAuth/DB configuration.
 */
async fn health_responder() -> HttpResponse {
    HttpResponse::Ok().body("Ok")
}

/**
 * Query parameters for the login endpoint
 */
#[derive(Debug, serde::Deserialize)]
struct LoginQuery {
    #[serde(rename = "returnTo")]
    return_to: Option<String>,
}

/**
 * Function used by the Web Application to initiate OAuth.
 *
 * The server responds with the OAuth login URL.
 *
 * The server implements PKCE (Proof Key for Code Exchange) to protect itself and the users.
 */
#[get("/login")]
async fn login(
    pool: web::Data<PostgresPool>,
    cfg: web::Data<AppConfig>,
    query: web::Query<LoginQuery>,
) -> Result<HttpResponse, Error> {
    // TODO: verify if user exists in the db by looking at the session cookie, (if the client provides one.)
    info!("Login endpoint called with query: {:?}", query);
    let pool2 = pool.clone();
    let return_to = query.return_to.clone();
    info!("return_to value: {:?}", return_to);

    // 2. Generate and Store OAuth Request.
    let (csrf_token, pkce_challenge) = {
        let pool = pool2.clone();
        web::block(move || generate_and_store_oauth_request(pool, return_to)).await?
    }
    .map_err(|e| {
        error!("{:?}", e);
        error::ErrorInternalServerError(e)
    })?;

    // 3. Craft OAuth Login URL
    let oauth_login_url = format!("{oauth_url}?client_id={client_id}&redirect_uri={redirect_url}&response_type=code&scope={scope}&prompt=select_account&pkce_challenge={pkce_challenge}&state={state}&access_type=offline",
                                    oauth_url=cfg.oauth_auth_url,
                                    redirect_url=cfg.oauth_redirect_url,
                                    client_id=cfg.oauth_client_id,
                                    scope=SCOPE,
                                    pkce_challenge=pkce_challenge.as_str(),
                                    state=&csrf_token.secret()
    );

    // 4. Redirect the browser to the OAuth Login URL.
    let mut response = HttpResponse::Found();
    response.append_header((LOCATION, oauth_login_url));
    Ok(response.finish())
}

/**
 * Handle OAuth callback from Web App.
 *
 * This service is responsible for using the provided authentication code to fetch
 * the OAuth access_token and refresh token.
 *
 * It upserts the user using their email and stores the access_token & refresh_code.
 */
#[get("/login/callback")]
async fn handle_google_oauth_callback(
    pool: web::Data<PostgresPool>,
    info: web::Query<AuthRequest>,
    cfg: web::Data<AppConfig>,
) -> Result<HttpResponse, Error> {
    let state = info.state.clone();

    // 1. Fetch OAuth request, if this fails, probably a hacker is trying to p*wn us.
    let oauth_request = {
        let pool = pool.clone();
        web::block(move || fetch_oauth_request(pool, state)).await?
    }
    .map_err(|e| {
        error!("{:?}", e);
        error::ErrorBadRequest("couldn't find a request, are you a hacker?")
    })?;

    // 2. Request token from OAuth provider.
    let (oauth_response, claims) = request_token(
        &cfg.oauth_redirect_url,
        &cfg.oauth_client_id,
        &cfg.oauth_secret,
        &oauth_request.pkce_verifier,
        &cfg.oauth_token_url,
        &info.code,
    )
    .await
    .map_err(|err| {
        error!("{:?}", err);
        error::ErrorBadRequest("couldn't find a request, are you a hacker?")
    })?;

    // 3. Store tokens and create user.
    {
        let claims = claims.clone();
        web::block(move || upsert_user(pool, &claims, &oauth_response)).await?
    }
    .map_err(|err| {
        error!("{:?}", err);
        error::ErrorInternalServerError(err)
    })?;

    // 4. Create session cookie with email.
    let cookie_domain = std::env::var("COOKIE_DOMAIN").ok();

    let mut cookie_builder = Cookie::build("email", claims.email.clone())
        .path("/")
        .same_site(SameSite::Lax)
        .expires(OffsetDateTime::now_utc().checked_add(Duration::hours(87600)));

    if let Some(domain) = cookie_domain.as_ref() {
        cookie_builder = cookie_builder.domain(domain.clone());
    }
    let cookie = cookie_builder.finish();

    // Also store the name in a separate cookie
    let mut name_cookie_builder = Cookie::build("name", claims.name.clone())
        .path("/")
        .same_site(SameSite::Lax)
        .expires(OffsetDateTime::now_utc().checked_add(Duration::hours(87600)));

    if let Some(domain) = cookie_domain.as_ref() {
        name_cookie_builder = name_cookie_builder.domain(domain.clone());
    }
    let name_cookie = name_cookie_builder.finish();

    // 5. Send cookies and redirect browser to return_to URL or fallback to after_login_url
    let redirect_url = oauth_request
        .return_to
        .unwrap_or_else(|| cfg.after_login_url.clone());
    info!(
        "OAuth login successful for user: {} ({}), redirecting to: {}",
        claims.name, claims.email, redirect_url
    );
    let mut response = HttpResponse::Found();
    response.append_header((LOCATION, redirect_url));
    response.cookie(cookie);
    response.cookie(name_cookie);
    Ok(response.finish())
}

/**
 * Check if the user has an active session
 * Returns 200 if session is valid, 401 if not
 */
#[get("/session")]
async fn check_session(req: HttpRequest) -> Result<HttpResponse, Error> {
    debug!("Session check request from: {:?}", req.connection_info());
    debug!("All cookies: {:?}", req.cookies());

    if let Some(cookie) = req.cookie("email") {
        if !cookie.value().is_empty() {
            info!("Session valid for: {}", cookie.value());
            return Ok(HttpResponse::Ok().finish());
        }
    }
    info!("No valid session found, returning 401");
    Ok(HttpResponse::Unauthorized().finish())
}

/**
 * Get the current user's profile
 * Returns email and name from session cookies
 */
#[get("/profile")]
async fn get_profile(req: HttpRequest) -> Result<HttpResponse, Error> {
    let email = req
        .cookie("email")
        .map(|c| c.value().to_string())
        .ok_or_else(|| error::ErrorUnauthorized("No session"))?;

    let name = req
        .cookie("name")
        .map(|c| c.value().to_string())
        .unwrap_or_else(|| email.clone());

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "email": email,
        "name": name
    })))
}

/**
 * Logout endpoint - clears session cookies
 */
#[get("/logout")]
async fn logout() -> Result<HttpResponse, Error> {
    info!("User logging out");

    let cookie_domain = std::env::var("COOKIE_DOMAIN").ok();

    // Create expired cookies to clear them
    let mut email_cookie_builder = Cookie::build("email", "")
        .path("/")
        .expires(OffsetDateTime::now_utc());

    if let Some(domain) = cookie_domain.as_ref() {
        email_cookie_builder = email_cookie_builder.domain(domain.clone());
    }
    let email_cookie = email_cookie_builder.finish();

    let mut name_cookie_builder = Cookie::build("name", "")
        .path("/")
        .expires(OffsetDateTime::now_utc());

    if let Some(domain) = cookie_domain.as_ref() {
        name_cookie_builder = name_cookie_builder.domain(domain.clone());
    }
    let name_cookie = name_cookie_builder.finish();

    let mut response = HttpResponse::Ok();
    response.cookie(email_cookie);
    response.cookie(name_cookie);
    Ok(response.finish())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_writer(std::io::stderr)
        .init();
    info!("start");

    // vc-8wd: arm targeted SFU tracing from SFU_TRACE_ROOM/SESSION (read
    // once). No-op when unset, which is the default.
    sec_api::sfu::trace::init();

    // SFU_TRANSPORT_KIND identifies this binary's transport family for the
    // wave-3 ADMISSION_DECISION{REDIRECT} DNS template (bead vc-8oa / p6-5).
    // Set unconditionally at startup so the JoinRoom handler doesn't have to
    // care which binary it's hosted in; the chart can override this for
    // non-standard deployments.
    if std::env::var_os("SFU_TRANSPORT_KIND").is_none() {
        std::env::set_var("SFU_TRANSPORT_KIND", "websocket");
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
             rustlemania-websocket-0"
        );
    }

    let sfu_config = SfuConfig::from_env();
    info!("sfu mode: {}", sfu_config.mode);
    if sfu_config.mode == SfuMode::Sfu {
        info!("sfu mode active (no-op shim)");
    }

    let nats_url = std::env::var("NATS_URL").expect("NATS_URL env var must be defined");
    let nats_client = sec_api::nats_connect::connect(&nats_url)
        .await
        .expect("Failed to connect to NATS");

    // vc-ud6o E3: grab the shared connection-state handle BEFORE `.start()`
    // consumes the actor. It is cloned into every `SessionLogic` via AppState
    // so the off-actor media-publish path can read the `Active` gate lock-free.
    let chat_server = ChatServer::new(nats_client.clone()).await;
    let connection_states = chat_server.connection_states_handle();
    let chat = chat_server.start();

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
    let oauth_client_id: String =
        std::env::var("OAUTH_CLIENT_ID").unwrap_or_else(|_| String::from(""));
    let oauth_auth_url: String =
        std::env::var("OAUTH_AUTH_URL").unwrap_or_else(|_| String::from(""));
    let oauth_token_url: String =
        std::env::var("OAUTH_TOKEN_URL").unwrap_or_else(|_| String::from(""));
    let oauth_secret: String =
        std::env::var("OAUTH_CLIENT_SECRET").unwrap_or_else(|_| String::from(""));
    let oauth_redirect_url: String =
        std::env::var("OAUTH_REDIRECT_URL").unwrap_or_else(|_| String::from(""));
    let after_login_url: String =
        std::env::var("UI_ENDPOINT").unwrap_or_else(|_| String::from("/"));
    let db_enabled: bool = truthy(Some(
        &std::env::var("DATABASE_ENABLED").unwrap_or_else(|_| String::from("false")),
    ));

    HttpServer::new(move || {
        let cors = Cors::permissive();

        if oauth_client_id.is_empty() {
            App::new()
                .wrap(cors)
                .app_data(web::Data::new(AppState {
                    chat: chat.clone(),
                    nats_client: nats_client.clone(),
                    tracker_sender: tracker_sender.clone(),
                    session_manager: session_manager.clone(),
                    connection_states: connection_states.clone(),
                }))
                .service(check_session)
                .service(get_profile)
                .service(logout)
                .service(ws_connect_authenticated)
                .service(ws_connect)
                .route("/version", web::get().to(version::websocket_version))
                .route("/healthz", web::get().to(health_responder))
        } else if db_enabled {
            // OAuth requires database (r2d2 pool for legacy OAuth code)
            let pool = get_pool();
            App::new()
                .app_data(web::Data::new(pool))
                .app_data(web::Data::new(AppState {
                    chat: chat.clone(),
                    nats_client: nats_client.clone(),
                    tracker_sender: tracker_sender.clone(),
                    session_manager: session_manager.clone(),
                    connection_states: connection_states.clone(),
                }))
                .app_data(web::Data::new(AppConfig {
                    oauth_client_id: oauth_client_id.clone(),
                    oauth_auth_url: oauth_auth_url.clone(),
                    oauth_token_url: oauth_token_url.clone(),
                    oauth_secret: oauth_secret.clone(),
                    oauth_redirect_url: oauth_redirect_url.clone(),
                    after_login_url: after_login_url.clone(),
                }))
                .wrap(cors)
                .service(handle_google_oauth_callback)
                .service(login)
                .service(check_session)
                .service(get_profile)
                .service(logout)
                .service(ws_connect_authenticated)
                .service(ws_connect)
                .route("/version", web::get().to(version::websocket_version))
                .route("/healthz", web::get().to(health_responder))
        } else {
            // OAuth configured but database disabled - skip OAuth routes
            error!("OAuth is configured but DATABASE_ENABLED=false. OAuth requires database. Skipping OAuth routes.");
            App::new()
                .wrap(cors)
                .app_data(web::Data::new(AppState {
                    chat: chat.clone(),
                    nats_client: nats_client.clone(),
                    tracker_sender: tracker_sender.clone(),
                    session_manager: session_manager.clone(),
                    connection_states: connection_states.clone(),
                }))
                .service(check_session)
                .service(get_profile)
                .service(logout)
                .service(ws_connect_authenticated)
                .service(ws_connect)
                .route("/version", web::get().to(version::websocket_version))
                .route("/healthz", web::get().to(health_responder))
        }
    })
    .bind((
        "0.0.0.0",
        std::env::var("ACTIX_PORT")
            .unwrap_or_else(|_| String::from("8080"))
            .parse::<u16>()
            .unwrap(),
    ))?
    .run()
    .await
}
