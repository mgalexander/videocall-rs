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

//! Bit constants for the `RoutingHeader.frame_marker` field — see ADR-0001.
//!
//! These are the single source of truth shared by the client (which sets them
//! when building a `RoutingHeader`) and the SFU (which reads them for routing
//! decisions). Keep them in sync with the ADR; never redefine elsewhere.

/// First packet of a frame.
pub const START_OF_FRAME: u32 = 1;

/// Last packet of a frame.
pub const END_OF_FRAME: u32 = 2;

/// Delta frame depends on a T0 picture in the same temporal chain.
pub const REFERENCES_T0: u32 = 4;
