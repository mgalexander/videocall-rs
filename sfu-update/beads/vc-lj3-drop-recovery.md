# SFU: drop-slope recovery distinct from the silence watchdog
Source: LATE-JOINER-INTEGRATION-ROOTCAUSE.md (lj-3). The vc-9eh watchdog only fires on
SILENCE; saturation-drop keeps `last_msg_at` advancing (`chat_server.rs:3443-3454`,
`nats_connect.rs:134-138`) so it's invisible. Add a drop-RATE/slope-based recovery
signal (resubscribe/alarm) keyed on `SFU_DISPATCHER_INBOUND_DROPPED_TOTAL` rising.
## P1. Acceptance: a sustained inbound-drop slope triggers recovery + alarm.
