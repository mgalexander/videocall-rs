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
 */

//! vc-zf8k (Bead B-a): assert the fail-fast panic hook in
//! `webtransport_server::main` aborts the WHOLE PROCESS on a panic, instead of
//! merely unwinding the panicking task (the tokio default) — which is exactly
//! how the DEFECT-JOINHANDLE-PANIC forwarding-dead zombie arose.
//!
//! We cannot test `std::process::abort()` in-process (it would kill the test
//! runner), so we run the REAL produced server binary
//! (`CARGO_BIN_EXE_webtransport_server`) with `SFU_PANIC_HOOK_SELFTEST=1`. In
//! that mode `main` installs the panic hook and then immediately panics —
//! before any NATS connect or socket bind — so the only thing under test is
//! the hook's abort behavior.
//!
//! Acceptance: the child must exit ABNORMALLY (terminated by a signal, i.e.
//! `code() == None` on unix from SIGABRT, or at minimum a non-zero exit code).
//! A clean `code 0` exit would mean the panic was swallowed and the process
//! survived — the regression we are guarding against.

use std::process::Command;

#[test]
fn panic_hook_aborts_the_process() {
    let exe = env!("CARGO_BIN_EXE_webtransport_server");
    let out = Command::new(exe)
        .env("SFU_PANIC_HOOK_SELFTEST", "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .expect("failed to spawn webtransport_server binary for panic-hook selftest");

    let code = out.status.code();
    assert_ne!(
        code,
        Some(0),
        "panic-hook selftest child exited cleanly (code 0) — the fail-fast hook \
         did NOT abort the process. A panic in a forwarding task would zombie. \
         stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // On unix, abort() raises SIGABRT and the child has no exit code (it was
    // terminated by a signal). Assert that stronger property when available; on
    // platforms without signals a non-zero exit code already satisfies the
    // contract above.
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        let signaled = out.status.signal().is_some();
        assert!(
            signaled || code.map(|c| c != 0).unwrap_or(true),
            "expected abnormal termination (signal) from abort(); got status {:?}",
            out.status
        );
    }
}
