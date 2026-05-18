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

pub mod config;
pub use config::{SfuConfig, SfuMode};

pub mod affinity;
pub mod forwarder;
pub mod health_beacon;
pub mod layer_selector;
pub mod priority_queue;
pub mod room_state;
pub mod speaker;
pub mod spillover;
pub mod subscription;

#[cfg(test)]
mod tests;
