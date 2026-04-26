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
