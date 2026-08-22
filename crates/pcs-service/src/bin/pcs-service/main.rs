//! pcs-service — PCS distributed batch processing service binary.
//!
//! This binary is the reference implementation of the PCS service layer.
//! It is gated on the `service` feature flag.
//!
//! ## Usage
//!
//! ```text
//! pcs-service serve --config service.toml
//! pcs-service validate --config service.toml
//! pcs-service status --addr http://localhost:8080
//! pcs-service cluster init --config service.toml
//! pcs-service cluster status --addr http://localhost:8080
//! ```

use clap::Parser;

mod cli;
mod commands;

// A pipeline allocates and frees its output Arrow arrays once per batch, and
// those are routinely multi-megabyte. Windows' heap sends allocations above
// ~512 KB straight to `VirtualAlloc` and hands the pages back on free, so the
// next batch soft-faults all of them again; glibc's arena behaviour is milder
// but not free either. mimalloc retains large blocks instead, which measured
// 2.3x on `tpch_q6/narrow_pcs` and 2.6x on `wide_pcs`. See
// `performance-improvement.md`, round 1.
//
// Opt out with `--no-default-features` plus the feature set you want; the
// library itself never installs an allocator.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let parsed = cli::Cli::parse();
    let result = match &parsed.cmd {
        cli::Command::Serve(args) => commands::serve::run(&parsed.global, args).await,
        cli::Command::Validate(args) => commands::validate::run(&parsed.global, args).await,
        cli::Command::Status(args) => commands::status::run(&parsed.global, args).await,
        cli::Command::Cluster { cmd } => commands::cluster::run(&parsed.global, cmd).await,
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
