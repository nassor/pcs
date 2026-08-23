//! pcs-service: PCS distributed batch processing service binary.
//!
//! Reference implementation of the PCS service layer, gated on the `service`
//! feature flag.
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

// Pipelines allocate and free multi-megabyte Arrow arrays once per batch.
// System allocators return those large blocks to the OS on free, so the next
// batch soft-faults every page again; mimalloc retains them instead.
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
