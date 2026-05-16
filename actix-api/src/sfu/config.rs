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
