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

//! Centralized Prometheus metrics for the videocall API

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram,
    Counter, CounterVec, Gauge, GaugeVec, Histogram,
};

lazy_static! {
    /// Total number of health reports received
    pub static ref HEALTH_REPORTS_TOTAL: Counter = register_counter!(
        "videocall_health_reports_total",
        "Total number of health reports received"
    )
    .expect("Failed to create health_reports_total metric");

    /// Whether peer can receive audio (1 = yes, 0 = no)
    pub static ref PEER_CAN_LISTEN: GaugeVec = register_gauge_vec!(
        "videocall_peer_can_listen",
        "Indicates if a peer can receive audio from another peer (1=yes, 0=no)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create peer_can_listen metric");

    /// Whether peer can receive video (1 = yes, 0 = no)
    pub static ref PEER_CAN_SEE: GaugeVec = register_gauge_vec!(
        "videocall_peer_can_see",
        "Indicates if a peer can receive video from another peer (1=yes, 0=no)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create peer_can_see metric");

    /// NetEQ audio buffer size in milliseconds
    pub static ref NETEQ_AUDIO_BUFFER_MS: GaugeVec = register_gauge_vec!(
        "videocall_neteq_audio_buffer_ms",
        "Audio data buffered for playback in milliseconds",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_audio_buffer_ms metric");

    /// NetEQ packets waiting for decode
    pub static ref NETEQ_PACKETS_AWAITING_DECODE: GaugeVec = register_gauge_vec!(
        "videocall_neteq_packets_awaiting_decode",
        "Number of encoded packets waiting to be decoded",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_packets_awaiting_decode metric");

    /// NetEQ packets received per second
    pub static ref NETEQ_PACKETS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_packets_per_sec",
        "Number of audio RTP packets received per second (rolling 1s window)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_packets_per_sec metric");

    /// NetEQ normal decode operations per second
    pub static ref NETEQ_NORMAL_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_normal_ops_per_sec",
        "Normal decode operations per second",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_normal_ops_per_sec metric");

    /// NetEQ expand operations per second
    pub static ref NETEQ_EXPAND_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_expand_ops_per_sec",
        "Expand operations per second (packet loss concealment)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_expand_ops_per_sec metric");

    /// NetEQ accelerate operations per second
    pub static ref NETEQ_ACCELERATE_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_accelerate_ops_per_sec",
        "Accelerate operations per second (time compression)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_accelerate_ops_per_sec metric");

    /// NetEQ fast accelerate operations per second
    pub static ref NETEQ_FAST_ACCELERATE_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_fast_accelerate_ops_per_sec",
        "Fast accelerate operations per second",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_fast_accelerate_ops_per_sec metric");

    /// NetEQ preemptive expand operations per second
    pub static ref NETEQ_PREEMPTIVE_EXPAND_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_preemptive_expand_ops_per_sec",
        "Preemptive expand operations per second (time expansion)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_preemptive_expand_ops_per_sec metric");

    /// NetEQ merge operations per second
    pub static ref NETEQ_MERGE_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_merge_ops_per_sec",
        "Merge operations per second (blending)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_merge_ops_per_sec metric");

    /// NetEQ comfort noise operations per second
    pub static ref NETEQ_COMFORT_NOISE_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_comfort_noise_ops_per_sec",
        "Comfort noise operations per second",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_comfort_noise_ops_per_sec metric");

    /// NetEQ DTMF operations per second
    pub static ref NETEQ_DTMF_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_dtmf_ops_per_sec",
        "DTMF operations per second",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_dtmf_ops_per_sec metric");

    /// NetEQ undefined operations per second
    pub static ref NETEQ_UNDEFINED_OPS_PER_SEC: GaugeVec = register_gauge_vec!(
        "videocall_neteq_undefined_ops_per_sec",
        "Undefined operations per second",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create neteq_undefined_ops_per_sec metric");

    /// Total number of active sessions
    pub static ref ACTIVE_SESSIONS_TOTAL: GaugeVec = register_gauge_vec!(
        "videocall_active_sessions_total",
        "Number of active sessions",
        &["meeting_id", "session_id"]
    )
    .expect("Failed to create active_sessions_total metric");

    /// Number of participants in each meeting
    pub static ref MEETING_PARTICIPANTS: GaugeVec = register_gauge_vec!(
        "videocall_meeting_participants",
        "Number of participants in a meeting",
        &["meeting_id"]
    )
    .expect("Failed to create meeting_participants metric");

    /// Total number of peer connections
    pub static ref PEER_CONNECTIONS_TOTAL: GaugeVec = register_gauge_vec!(
        "videocall_peer_connections_total",
        "Number of peer connections",
        &["meeting_id", "peer_id"]
    )
    .expect("Failed to create peer_connections_total metric");

    /// Overall session quality scores (0.0-1.0)
    pub static ref SESSION_QUALITY: Histogram = register_histogram!(
        "videocall_session_quality_score",
        "Overall session quality scores (0.0-1.0)",
        vec![0.1, 0.3, 0.5, 0.7, 0.9, 1.0]
    )
    .expect("Failed to create session_quality metric");

    /// Per-pair video packets buffered in the decoder/jitter buffer
    pub static ref VIDEO_PACKETS_BUFFERED: GaugeVec = register_gauge_vec!(
        "videocall_video_packets_buffered",
        "Number of video packets/frames currently buffered awaiting decode",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create video_packets_buffered metric");

    /// Per-pair video framerate as observed by the receiver
    pub static ref VIDEO_FPS: GaugeVec = register_gauge_vec!(
        "videocall_video_fps",
        "Video frames per second observed by the receiver",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create video_fps metric");

    /// Whether receiving peer reports audio enabled for the sender
    pub static ref PEER_AUDIO_ENABLED: GaugeVec = register_gauge_vec!(
        "videocall_peer_audio_enabled",
        "Indicates if sender's audio is enabled (1=yes, 0=no)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create peer_audio_enabled metric");

    /// Whether receiving peer reports video enabled for the sender
    pub static ref PEER_VIDEO_ENABLED: GaugeVec = register_gauge_vec!(
        "videocall_peer_video_enabled",
        "Indicates if sender's camera is enabled (1=yes, 0=no)",
        &["meeting_id", "session_id", "from_peer", "to_peer"]
    )
    .expect("Failed to create peer_video_enabled metric");

    /// Sender self-reported audio enabled (authoritative), per meeting and peer
    pub static ref SELF_AUDIO_ENABLED: GaugeVec = register_gauge_vec!(
        "videocall_self_audio_enabled",
        "Sender self-reported audio enabled (1=yes, 0=no)",
        &["meeting_id", "peer_id"]
    )
    .expect("Failed to create self_audio_enabled metric");

    /// Sender self-reported video enabled (authoritative), per meeting and peer
    pub static ref SELF_VIDEO_ENABLED: GaugeVec = register_gauge_vec!(
        "videocall_self_video_enabled",
        "Sender self-reported video enabled (1=yes, 0=no)",
        &["meeting_id", "peer_id"]
    )
    .expect("Failed to create self_video_enabled metric");

    /// Client-side measured active server RTT in milliseconds
    pub static ref CLIENT_ACTIVE_SERVER_RTT_MS: GaugeVec = register_gauge_vec!(
        "videocall_client_active_server_rtt_ms",
        "Client-side measured RTT to the elected server (ms)",
        &["meeting_id", "session_id", "peer_id", "server_url", "server_type"]
    )
    .expect("Failed to create client_active_server_rtt_ms metric");

    /// Marker that a client is connected to a given server (value = 1)
    pub static ref CLIENT_ACTIVE_SERVER: GaugeVec = register_gauge_vec!(
        "videocall_client_active_server",
        "Indicates which server a client is connected to (1)",
        &["meeting_id", "session_id", "peer_id", "server_url", "server_type"]
    )
    .expect("Failed to create client_active_server metric");

    // ===== SERVER-SIDE METRICS (via NATS) =====

    /// Active connections on servers by protocol and customer (now with unique session_id)
    pub static ref SERVER_CONNECTIONS_ACTIVE: GaugeVec = register_gauge_vec!(
        "videocall_server_connections_active",
        "Number of active connections on servers",
        &["session_id", "protocol", "customer_email", "meeting_id", "server_instance", "region"]
    )
    .expect("Failed to create server_connections_active metric");

    /// Active unique user connections (deduplicated by customer_email + meeting_id)
    pub static ref SERVER_UNIQUE_USERS_ACTIVE: GaugeVec = register_gauge_vec!(
        "videocall_server_unique_users_active",
        "Number of unique users active in meetings (deduplicated across protocols)",
        &["customer_email", "meeting_id", "region"]
    )
    .expect("Failed to create server_unique_users_active metric");

    /// Active protocol connections per unique user
    pub static ref SERVER_PROTOCOL_CONNECTIONS: GaugeVec = register_gauge_vec!(
        "videocall_server_protocol_connections",
        "Protocols used by active connections per user",
        &["protocol", "customer_email", "meeting_id", "region"]
    )
    .expect("Failed to create server_protocol_connections metric");

    /// Total data bytes transferred by servers per customer (now with unique session_id)
    pub static ref SERVER_DATA_BYTES_TOTAL: GaugeVec = register_gauge_vec!(
        "videocall_server_data_bytes_total",
        "Cumulative bytes transferred by servers per customer",
        &["direction", "session_id", "protocol", "customer_email", "meeting_id", "server_instance", "region"]
    )
    .expect("Failed to create server_data_bytes_total metric");

    /// Connection duration in seconds (when connection closes)
    pub static ref SERVER_CONNECTION_DURATION_SECONDS: Histogram = register_histogram!(
        "videocall_server_connection_duration_seconds",
        "Duration of server connections in seconds",
        vec![1.0, 10.0, 30.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0] // 1s to 2h buckets
    )
    .expect("Failed to create server_connection_duration_seconds metric");

    /// Connection lifecycle events counter
    pub static ref SERVER_CONNECTION_EVENTS_TOTAL: Counter = register_counter!(
        "videocall_server_connection_events_total",
        "Total connection lifecycle events on servers"
    )
    .expect("Failed to create server_connection_events_total metric");

    /// Reconnection tracking per customer and meeting
    pub static ref SERVER_RECONNECTIONS_TOTAL: GaugeVec = register_gauge_vec!(
        "videocall_server_reconnections_total",
        "Total reconnections per customer and meeting",
        &["protocol", "customer_email", "meeting_id", "server_instance", "region"]
    )
    .expect("Failed to create server_reconnections_total metric");

    // ===== SFU FORWARDER METRICS (p2-7) =====
    //
    // These metrics intentionally do NOT carry the `videocall_` prefix used by
    // the legacy/client metrics above; they are scoped to the SFU forwarder and
    // are exposed via the same Prometheus default registry (no endpoint
    // changes required).

    /// Total packets forwarded by `Forwarder::decide`, labeled by packet type.
    pub static ref SFU_FORWARDED_TOTAL: CounterVec = register_counter_vec!(
        "sfu_forwarded_total",
        "Total packets forwarded by the SFU forwarder, labeled by packet type",
        &["packet_type"]
    )
    .expect("Failed to create sfu_forwarded_total metric");

    /// Total packets dropped by `Forwarder::decide`, labeled by reason.
    ///
    /// Known label values (registered eagerly so they appear at 0 in
    /// `/metrics` even before any drop occurs):
    ///   - `self_skip`        — sender == receiver (wave-3 / current)
    ///   - `unsubscribed`     — placeholder, wired in P3
    ///   - `layer_budget`     — placeholder, wired in P4
    ///   - `kfr_unsubscribed` — KEYFRAME_REQUEST whose target sender is
    ///                          not in the requester's current
    ///                          `LayerSelection.forward` (p4-10).
    pub static ref SFU_DROPPED_TOTAL: CounterVec = {
        let cv = register_counter_vec!(
            "sfu_dropped_total",
            "Total packets dropped by the SFU forwarder, labeled by reason",
            &["reason"]
        )
        .expect("Failed to create sfu_dropped_total metric");
        // Touch each known label value at zero so it is visible in /metrics
        // before the first real increment in later phases.
        cv.with_label_values(&["self_skip"]).inc_by(0.0);
        cv.with_label_values(&["unsubscribed"]).inc_by(0.0);
        cv.with_label_values(&["layer_budget"]).inc_by(0.0);
        cv.with_label_values(&["kfr_unsubscribed"]).inc_by(0.0);
        cv
    };

    /// Current number of members in each SFU room, labeled by room id.
    /// Refreshed on every `Forwarder::decide` invocation.
    pub static ref SFU_ROOM_SIZE: GaugeVec = register_gauge_vec!(
        "sfu_room_size",
        "Current number of members in the SFU room",
        &["room_id"]
    )
    .expect("Failed to create sfu_room_size metric");

    /// Speaker changes per minute. Declared in p2-7; wired in P3.
    pub static ref SFU_SPEAKER_CHANGES_PER_MIN: Gauge = register_gauge!(
        "sfu_speaker_changes_per_min",
        "Speaker changes per minute (declared in p2-7; populated in P3)"
    )
    .expect("Failed to create sfu_speaker_changes_per_min metric");

    /// Wall-clock duration of `Forwarder::decide` in microseconds.
    pub static ref SFU_DECIDE_LATENCY_US: Histogram = register_histogram!(
        "sfu_decide_latency_us",
        "Wall-clock duration of Forwarder::decide in microseconds",
        vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0, 1000.0]
    )
    .expect("Failed to create sfu_decide_latency_us metric");

    /// Total packets dropped by `PrioritySender::send`, labeled by class.
    ///
    /// Companion to [`SFU_DROPPED_TOTAL`] (which labels by drop *reason* on
    /// the forwarder decide path). This metric labels by outbound priority
    /// *class* at the queue-admission boundary so operators can answer
    /// "which class is bleeding under the current load?". Label values are
    /// the `Debug` form of `priority_queue::Class`:
    ///   - `P0Control`    — should always remain 0 (NeverDrop policy).
    ///   - `P1Audio`
    ///   - `P2Keyframe`
    ///   - `P3VideoBase`
    ///   - `P4Enhancement`
    ///
    /// All five label values are touched at 0 on init so they appear in
    /// `/metrics` before the first real drop.
    pub static ref SFU_CLASS_DROPPED_TOTAL: CounterVec = {
        let cv = register_counter_vec!(
            "sfu_class_dropped_total",
            "Total packets dropped by the SFU PrioritySender, labeled by priority class",
            &["class"]
        )
        .expect("Failed to create sfu_class_dropped_total metric");
        for label in ["P0Control", "P1Audio", "P2Keyframe", "P3VideoBase", "P4Enhancement"] {
            cv.with_label_values(&[label]).inc_by(0.0);
        }
        cv
    };

    /// Total packets successfully enqueued by `PrioritySender::send`,
    /// labeled by class. Mirrors [`SFU_CLASS_DROPPED_TOTAL`] for sent
    /// outcomes so ops can compute per-class drop rate as
    /// `dropped / (sent + dropped)`.
    pub static ref SFU_CLASS_SENT_TOTAL: CounterVec = {
        let cv = register_counter_vec!(
            "sfu_class_sent_total",
            "Total packets successfully enqueued by the SFU PrioritySender, labeled by priority class",
            &["class"]
        )
        .expect("Failed to create sfu_class_sent_total metric");
        for label in ["P0Control", "P1Audio", "P2Keyframe", "P3VideoBase", "P4Enhancement"] {
            cv.with_label_values(&[label]).inc_by(0.0);
        }
        cv
    };

    /// Total base-layer (T0+S0) keyframes forwarded by `Forwarder::decide`.
    ///
    /// Per p4-8 invariant 1: a keyframe at `temporal_layer_id=0 AND
    /// spatial_layer_id=0` is the root of every dependent reference
    /// chain — dropping one breaks decode for every subsequent frame
    /// until the next keyframe arrives. The forwarder always passes
    /// these through regardless of the receiver's layer budget. This
    /// counter exposes the invariant in metrics so operators can verify
    /// keyframes are reaching every receiver.
    pub static ref SFU_KEYFRAME_FORWARDED_TOTAL: Counter = register_counter!(
        "sfu_keyframe_forwarded_total",
        "Base-layer (T0+S0) keyframes forwarded by the SFU forwarder (always forwarded, invariant 1)"
    )
    .expect("Failed to create sfu_keyframe_forwarded_total metric");
}
