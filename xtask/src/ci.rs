//! The two processor-target gates CI runs on every push.
//!
//! Both exist to catch drift that a host-only build cannot see: `pcs-core` has
//! to keep compiling for `wasm32-wasip2` without its default features, and the
//! `arrow-ipc` pin has to keep meaning the same bytes on both sides of the
//! component boundary.

use crate::sh::{Ctx, Result};

const CHECK_USAGE: &str = "usage: cargo xtask check-wasm-processor";
const ROUNDTRIP_USAGE: &str = "usage: cargo xtask processor-ipc-roundtrip";

/// Check that `pcs-core` still compiles for the processor target.
///
/// A processor build has no tokio and no rayon: it runs the system DAG through
/// the `pollster` sync executor, which only the `processor` feature selects. A
/// host-only test run cannot catch a dependency that crept into that path.
pub fn check_wasm_processor(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("check-wasm-processor");
    if ctx.no_options(args, CHECK_USAGE)? {
        return Ok(());
    }

    ctx.cmd("rustup")?
        .args(["target", "add", "wasm32-wasip2"])
        .run()?;
    ctx.cargo()?
        .args([
            "check",
            "--manifest-path",
            "crates/pcs-core/Cargo.toml",
            "--target",
            "wasm32-wasip2",
            "--no-default-features",
            "--features",
            "processor",
        ])
        .run()
}

/// Drive the host to processor Arrow IPC round-trip regression test.
///
/// It catches `arrow-ipc` version drift between `pcs-core` (host) and
/// `pcs-processor` (processor) before drift can corrupt checkpoints in
/// production.
///
/// Steps:
///
/// 1. Ensure the `wasm32-wasip2` toolchain target is installed.
/// 2. Build the `pcs-processor-smoketest` component, release profile.
/// 3. Run the host-side `wasm_roundtrip` integration test, which loads the
///    `.wasm` through `WasmPipelineRuntime`, drives a `RecordBatch` through
///    `run-batch`, and asserts byte-exact IPC equality on the round-trip.
///
/// `rustc` links a `wasm32-wasip2` cdylib into a Component Model component
/// itself, so the artifact under `target/wasm32-wasip2/release/` is the finished
/// component: no preview1 core module and no adapter step in between.
///
/// The fixture is deliberately trivial: one component, a single u64 field, zero
/// systems, so the pipeline is an identity. Any byte difference between the
/// before and after IPC snapshots therefore means `arrow-ipc` drift, not a
/// processor logic bug.
///
/// Cold runs are slow: the first `wasm32-wasip2` build of `arrow-ipc`,
/// `pcs-core` and the `wit-bindgen` generator. CI should cache `target/` and
/// `~/.cargo/registry` between runs.
///
/// Exit codes: 3 build produced no artifact.
pub fn processor_ipc_roundtrip(args: &[String]) -> Result<()> {
    let ctx = Ctx::with_pins("processor-ipc-roundtrip", "crates/pcs-processor/PINS.md");
    if ctx.no_options(args, ROUNDTRIP_USAGE)? {
        return Ok(());
    }

    ctx.log(format!("repo: {}", ctx.root().display()));

    ctx.log("ensuring wasm32-wasip2 target is installed...");
    ctx.cmd("rustup")?
        .args(["target", "add", "wasm32-wasip2"])
        .run()?;

    // Release profile: the test asserts the canonical release output path.
    ctx.log("building pcs-processor-smoketest (release)...");
    ctx.cargo()?
        .args([
            "build",
            "--release",
            "-p",
            "pcs-processor-smoketest",
            "--target",
            "wasm32-wasip2",
        ])
        .run()?;

    let artifact = ctx.path("target/wasm32-wasip2/release/pcs_processor_smoketest.wasm");
    ctx.expect_artifact(&artifact, 3)?;
    ctx.log(format!("smoketest built: {} bytes", ctx.size(&artifact)?));

    // The test name and crate are pinned so later pcs-service test additions do
    // not silently join this gate.
    ctx.log("running host-side round-trip test...");
    ctx.cargo()?
        .args([
            "test",
            "--test",
            "wasm_roundtrip",
            "-p",
            "pcs-service",
            "--features",
            "wasm",
            "--",
            "--nocapture",
        ])
        .run()?;

    ctx.log("PASS");
    Ok(())
}
