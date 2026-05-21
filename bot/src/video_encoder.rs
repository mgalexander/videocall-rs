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

use anyhow::{anyhow, Result};
use std::mem::MaybeUninit;
use std::os::raw::{c_int, c_ulong};
use vpx_sys::*;

macro_rules! vpx {
    ($f:expr) => {{
        let res = unsafe { $f };
        let res_int = unsafe { std::mem::transmute::<vpx_sys::vpx_codec_err_t, i32>(res) };
        if res_int != 0 {
            return Err(anyhow!("vpx function error code ({}).", res_int));
        }
        res
    }};
}

macro_rules! vpx_ptr {
    ($f:expr) => {{
        let res = unsafe { $f };
        if res.is_null() {
            return Err(anyhow!("vpx function returned null pointer."));
        }
        res
    }};
}

pub struct VideoEncoderBuilder {
    pub min_quantizer: u32,
    pub max_quantizer: u32,
    pub bitrate_kbps: u32,
    pub fps: u32,
    pub resolution: (u32, u32),
    #[allow(dead_code)]
    pub cpu_used: u32,
    pub profile: u32,
}

impl VideoEncoderBuilder {
    pub fn new(fps: u32, cpu_used: u8) -> Self {
        Self {
            bitrate_kbps: 500,
            max_quantizer: 60,
            min_quantizer: 40,
            resolution: (640, 480),
            fps,
            cpu_used: cpu_used as u32,
            profile: 0,
        }
    }
}

impl VideoEncoderBuilder {
    pub fn set_resolution(mut self, width: u32, height: u32) -> Self {
        self.resolution = (width, height);
        self
    }

    pub fn build(&self) -> Result<VideoEncoder> {
        let (width, height) = self.resolution;
        if width % 2 != 0 || width == 0 {
            return Err(anyhow!("Width must be divisible by 2"));
        }
        if height % 2 != 0 || height == 0 {
            return Err(anyhow!("Height must be divisible by 2"));
        }
        let cfg_ptr = vpx_ptr!(vpx_codec_vp9_cx());
        let mut cfg = unsafe { MaybeUninit::zeroed().assume_init() };
        vpx!(vpx_codec_enc_config_default(cfg_ptr, &mut cfg, 0));

        cfg.g_w = width;
        cfg.g_h = height;
        cfg.g_timebase.num = 1;
        cfg.g_timebase.den = self.fps as c_int;
        cfg.rc_target_bitrate = self.bitrate_kbps;
        cfg.rc_min_quantizer = self.min_quantizer;
        cfg.rc_max_quantizer = self.max_quantizer;
        cfg.g_threads = 2;
        cfg.g_lag_in_frames = 1;
        cfg.g_error_resilient = VPX_ERROR_RESILIENT_DEFAULT;
        cfg.g_pass = vpx_enc_pass::VPX_RC_ONE_PASS;
        cfg.g_profile = self.profile;
        cfg.rc_end_usage = vpx_rc_mode::VPX_VBR;
        cfg.kf_max_dist = 150;
        cfg.kf_min_dist = 150;
        cfg.kf_mode = vpx_kf_mode::VPX_KF_AUTO;

        let ctx = MaybeUninit::zeroed();
        let mut ctx = unsafe { ctx.assume_init() };

        vpx!(vpx_codec_enc_init_ver(
            &mut ctx,
            cfg_ptr,
            &cfg,
            0,
            VPX_ENCODER_ABI_VERSION as i32
        ));
        unsafe {
            vpx_codec_control_(&mut ctx, vp8e_enc_control_id::VP8E_SET_CPUUSED as c_int, 5);
            vpx_codec_control_(
                &mut ctx,
                vp8e_enc_control_id::VP9E_SET_TILE_COLUMNS as c_int,
                4,
            );
            vpx_codec_control_(&mut ctx, vp8e_enc_control_id::VP9E_SET_ROW_MT as c_int, 1);
            vpx_codec_control_(
                &mut ctx,
                vp8e_enc_control_id::VP9E_SET_FRAME_PARALLEL_DECODING as c_int,
                1,
            );
            // vpx_codec_control_(&mut ctx, vp8e_enc_control_id::VP9E_SET_AQ_MODE as c_int, 3);
        }
        Ok(VideoEncoder {
            ctx,
            cfg,
            width: self.resolution.0,
            height: self.resolution.1,
        })
    }
}

pub struct VideoEncoder {
    ctx: vpx_codec_ctx_t,
    cfg: vpx_codec_enc_cfg_t,
    width: u32,
    height: u32,
}

impl VideoEncoder {
    pub fn update_bitrate_kbps(&mut self, bitrate: u32) -> anyhow::Result<()> {
        self.cfg.rc_target_bitrate = bitrate;
        vpx!(vpx_codec_enc_config_set(&mut self.ctx, &self.cfg));
        Ok(())
    }

    /// Encode one I420 source frame.
    ///
    /// When `force_keyframe` is `true`, `VPX_EFLAG_FORCE_KF` is OR-ed into the
    /// libvpx encode flags so the next emitted frame is a keyframe regardless
    /// of the periodic `kf_max_dist` cadence. The bot uses this to honor an
    /// inbound `KEYFRAME_REQUEST` from a mid-stream-joining listener (vc-7zjq):
    /// without it, a mid-stream joiner has to wait up to `kf_max_dist` frames
    /// for the next periodic keyframe, which under backpressure may itself be
    /// dropped, leaving the joiner with an undecodable GOP indefinitely.
    pub fn encode(
        &mut self,
        pts: i64,
        data: &[u8],
        force_keyframe: bool,
    ) -> anyhow::Result<Frames<'_>> {
        let image = MaybeUninit::zeroed();
        let mut image = unsafe { image.assume_init() };

        vpx_ptr!(vpx_img_wrap(
            &mut image,
            vpx_img_fmt::VPX_IMG_FMT_I420,
            self.width as _,
            self.height as _,
            1,
            data.as_ptr() as _,
        ));

        // `VPX_EFLAG_FORCE_KF` forces this encode to emit a keyframe. We keep
        // the periodic `kf_max_dist=150` cadence as the always-on fallback
        // (vc-7zjq fix spec item 3); this flag is purely additive and only set
        // when an inbound KEYFRAME_REQUEST targeted at us was observed.
        let flags: i64 = if force_keyframe {
            VPX_EFLAG_FORCE_KF as i64
        } else {
            0
        };

        vpx!(vpx_codec_encode(
            &mut self.ctx,
            &image,
            pts,
            1,     // Duration
            flags, // Flags
            VPX_DL_REALTIME as c_ulong,
        ));

        Ok(Frames {
            ctx: &mut self.ctx,
            iter: std::ptr::null(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Frame<'a> {
    /// Compressed data.
    pub data: &'a [u8],
    /// Whether this is a key frame.
    pub key: bool,
    #[allow(dead_code)]
    /// Whether this frame is invisible.
    pub invisible: bool,
}

pub struct Frames<'a> {
    ctx: &'a mut vpx_codec_ctx_t,
    iter: vpx_codec_iter_t,
}

impl<'a> Iterator for Frames<'a> {
    type Item = Frame<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            unsafe {
                let pkt = vpx_codec_get_cx_data(self.ctx, &mut self.iter);
                if pkt.is_null() {
                    return None;
                } else if (*pkt).kind == vpx_codec_cx_pkt_kind::VPX_CODEC_CX_FRAME_PKT {
                    let f = &(*pkt).data.frame;
                    return Some(Frame {
                        data: std::slice::from_raw_parts(f.buf as _, f.sz),
                        key: (f.flags & VPX_FRAME_IS_KEY) != 0,
                        invisible: (f.flags & VPX_FRAME_IS_INVISIBLE) != 0,
                    });
                }
            }
        }
    }
}

unsafe impl Send for VideoEncoder {}
unsafe impl Sync for VideoEncoder {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small encoder and feed it solid-grey I420 frames.
    fn solid_i420(width: u32, height: u32) -> Vec<u8> {
        // I420: full-res Y plane + quarter-res U/V planes. 0x80 is neutral.
        vec![0x80u8; (width * height * 3 / 2) as usize]
    }

    /// vc-7zjq: passing `force_keyframe = true` must cause the next emitted
    /// frame to be a keyframe even when the periodic cadence would not have
    /// produced one. We encode one source frame to get the encoder past its
    /// initial (always-key) frame, then a second delta-eligible frame WITHOUT
    /// the flag (expected: delta), then a third WITH the flag (expected: key).
    #[test]
    fn force_keyframe_flag_emits_keyframe_vc_7zjq() {
        let (w, h) = (160u32, 120u32);
        let frame = solid_i420(w, h);
        let mut enc = VideoEncoderBuilder::new(30, 5)
            .set_resolution(w, h)
            .build()
            .expect("build encoder");

        // Frame 0: the first encoded frame is always a keyframe.
        let first_was_key = enc
            .encode(0, &frame, false)
            .expect("encode 0")
            .any(|f| f.key);
        assert!(first_was_key, "first frame should be a keyframe");

        // Frame 1: no force flag. With a static scene and kf_max_dist=150 this
        // must be a delta frame (NOT a keyframe).
        let second_was_key = enc
            .encode(1, &frame, false)
            .expect("encode 1")
            .any(|f| f.key);
        assert!(
            !second_was_key,
            "second frame without force flag must be a delta frame"
        );

        // Frame 2: force the keyframe. The emitted frame MUST be a keyframe.
        let third_was_key = enc
            .encode(2, &frame, true)
            .expect("encode 2")
            .any(|f| f.key);
        assert!(
            third_was_key,
            "VPX_EFLAG_FORCE_KF must force a keyframe on the next encode"
        );
    }
}
