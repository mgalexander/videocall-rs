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

mod audio_producer;
mod config;
mod orchestrate;
mod stats;
mod video_encoder; // VP9 encoder from videocall-cli
mod video_producer;
mod webtransport_client;

use std::time::Duration;

use audio_producer::AudioProducer;
use clap::Parser;
use config::{BotConfig, ClientConfig};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info, warn};
use video_producer::VideoProducer;
use webtransport_client::WebTransportClient;

use crate::orchestrate::OrchestrationConfig;

/// CLI for the videocall bot.
///
/// Two modes are supported:
///
/// 1. **Single-bot / config-file mode (default)**: when `--orchestrate` is
///    not passed, the bot loads its YAML config (via `BOT_CONFIG_PATH`) or
///    falls back to environment variables, then spawns one or more clients
///    that publish forever until ctrl-c.
///
/// 2. **Load-test orchestration mode**: when `--orchestrate` is passed,
///    spawns `--senders` publishing bots and `--listeners` subscribe-only
///    bots in `--room` against `--server-url` for `--duration` seconds,
///    then emits an aggregate JSON summary on stdout and exits.
#[derive(Parser, Debug)]
#[command(name = "bot", author, version, about = "Videocall load-test bot")]
struct Cli {
    /// Enable load-test orchestration mode. Requires `--room`, `--senders`,
    /// `--listeners`, `--duration`, and `--server-url`.
    #[arg(long)]
    orchestrate: bool,

    /// Room (meeting id) every spawned bot joins. Orchestration mode only.
    #[arg(long)]
    room: Option<String>,

    /// Number of publishing bots (video + audio). Orchestration mode only.
    #[arg(long)]
    senders: Option<usize>,

    /// Number of subscribe-only bots. Orchestration mode only.
    #[arg(long)]
    listeners: Option<usize>,

    /// Duration of the load test in seconds. Orchestration mode only.
    #[arg(long)]
    duration: Option<u64>,

    /// WebTransport server URL (e.g. `https://host:port`). Orchestration mode
    /// only; the lobby/room path is appended automatically.
    #[arg(long)]
    server_url: Option<String>,

    /// Skip TLS certificate verification. Orchestration mode only.
    #[arg(long, default_value_t = false)]
    insecure: bool,

    /// Path to the WAV file senders should publish.
    #[arg(long, default_value = "BundyBests2.wav")]
    audio_path: String,

    /// Directory containing the JPEG sequence senders should publish.
    #[arg(long, default_value = ".")]
    image_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging. The orchestration mode writes its JSON summary to
    // stdout, so logs are intentionally left on stderr (tracing-subscriber
    // default).
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    if cli.orchestrate {
        let cfg = build_orchestration_config(cli)?;
        return orchestrate::run(cfg).await;
    }

    info!("Starting videocall synthetic client bot (single-bot mode)");
    run_single_bot_mode().await
}

fn build_orchestration_config(cli: Cli) -> anyhow::Result<OrchestrationConfig> {
    let room = cli
        .room
        .ok_or_else(|| anyhow::anyhow!("--room is required in --orchestrate mode"))?;
    let senders = cli
        .senders
        .ok_or_else(|| anyhow::anyhow!("--senders is required in --orchestrate mode"))?;
    let listeners = cli
        .listeners
        .ok_or_else(|| anyhow::anyhow!("--listeners is required in --orchestrate mode"))?;
    let duration_s = cli
        .duration
        .ok_or_else(|| anyhow::anyhow!("--duration is required in --orchestrate mode"))?;
    let server_url = cli
        .server_url
        .ok_or_else(|| anyhow::anyhow!("--server-url is required in --orchestrate mode"))?;

    if senders == 0 && listeners == 0 {
        return Err(anyhow::anyhow!(
            "--senders and --listeners cannot both be zero"
        ));
    }

    Ok(OrchestrationConfig {
        room,
        senders,
        listeners,
        duration: Duration::from_secs(duration_s),
        server_url: url::Url::parse(&server_url)
            .map_err(|e| anyhow::anyhow!("Invalid --server-url: {e}"))?,
        insecure: cli.insecure,
        audio_path: cli.audio_path,
        image_dir: cli.image_dir,
    })
}

async fn run_single_bot_mode() -> anyhow::Result<()> {
    // Load configuration
    let config = BotConfig::from_env_or_default()?;
    info!("Loaded configuration for {} clients", config.clients.len());

    let server_url = config.server_url()?;
    let ramp_up_delay = Duration::from_millis(config.ramp_up_delay_ms.unwrap_or(1000));
    let insecure = config.insecure.unwrap_or(false);

    if insecure {
        warn!("WARNING: Certificate verification disabled - connection is insecure!");
    }

    // Start clients with linear ramp-up
    let mut client_handles = Vec::new();
    let total_clients = config.clients.len();

    for (index, client_config) in config.clients.into_iter().enumerate() {
        info!(
            "Starting client {} ({}) - audio: {}, video: {}",
            index, client_config.user_id, client_config.enable_audio, client_config.enable_video
        );

        let server_url_clone = server_url.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = run_client(client_config, server_url_clone, insecure).await {
                error!("Client failed: {}", e);
            }
        });

        client_handles.push(handle);

        // Linear ramp-up delay between client starts
        if index < total_clients - 1 {
            info!(
                "Waiting {}ms before starting next client",
                ramp_up_delay.as_millis()
            );
            time::sleep(ramp_up_delay).await;
        }
    }

    info!("All clients started, waiting for completion");

    // Wait for all clients to complete
    for handle in client_handles {
        let _ = handle.await;
    }

    info!("All clients finished");
    Ok(())
}

async fn run_client(
    config: ClientConfig,
    server_url: url::Url,
    insecure: bool,
) -> anyhow::Result<()> {
    info!("Initializing client: {}", config.user_id);

    // Create WebTransport client and connect
    let mut client = WebTransportClient::new(config.clone());
    client.connect(&server_url, insecure).await?;

    // Create packet channel for media producers
    let (packet_tx, packet_rx) = mpsc::channel::<Vec<u8>>(100);

    // Start packet sender task
    client.start_packet_sender(packet_rx).await;

    // Start media producers based on configuration
    let mut audio_producer: Option<AudioProducer> = None;
    let mut video_producer: Option<VideoProducer> = None;

    if config.enable_audio {
        info!("Starting audio producer for {}", config.user_id);
        match AudioProducer::from_wav_file(
            config.user_id.clone(),
            "BundyBests2.wav",
            packet_tx.clone(),
        ) {
            Ok(producer) => {
                audio_producer = Some(producer);
                info!("Audio producer started for {}", config.user_id);
            }
            Err(e) => {
                warn!(
                    "Failed to start audio producer for {}: {}",
                    config.user_id, e
                );
            }
        }
    }

    if config.enable_video {
        info!("Starting video producer for {}", config.user_id);
        // Use local image directory (images are in current directory)
        match VideoProducer::from_image_sequence(
            config.user_id.clone(),
            ".", // Images are in current directory (bot working dir)
            packet_tx.clone(),
        ) {
            Ok(producer) => {
                video_producer = Some(producer);
                info!("Video producer started for {}", config.user_id);
            }
            Err(e) => {
                warn!(
                    "Failed to start video producer for {}: {}",
                    config.user_id, e
                );
            }
        }
    }

    info!(
        "Client {} running with audio: {}, video: {}",
        config.user_id,
        audio_producer.is_some(),
        video_producer.is_some()
    );

    // Keep the client running
    // In a real scenario, you might want to run for a specific duration or until a signal
    tokio::signal::ctrl_c().await?;

    info!("Shutting down client: {}", config.user_id);

    // Clean shutdown
    client.stop();
    if let Some(mut audio) = audio_producer {
        audio.stop();
    }
    if let Some(mut video) = video_producer {
        video.stop();
    }

    info!("Client {} shut down cleanly", config.user_id);
    Ok(())
}
