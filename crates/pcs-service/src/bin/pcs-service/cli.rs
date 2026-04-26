//! Command-line interface definitions for pcs-service.
//!
//! Uses clap 4 derive to define the full CLI shape. Every subcommand has its
//! own args struct; global options (config path, address, log overrides) are
//! collected in [`GlobalOpts`] and flattened into the root [`Cli`] struct.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// PCS (Pipeline Component System) distributed batch processing service.
#[derive(Parser, Debug)]
#[command(
    name = "pcs-service",
    version,
    about = "PCS (Pipeline Component System) distributed batch processing service"
)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalOpts,
    #[command(subcommand)]
    pub cmd: Command,
}

/// Options that apply to every subcommand.
#[derive(clap::Args, Debug, Clone)]
pub struct GlobalOpts {
    /// Path to the service config TOML file.
    #[arg(long, short = 'c', env = "PCS_CONFIG", global = true)]
    pub config: Option<PathBuf>,

    /// HTTP control-plane address to query (for status/cluster commands).
    #[arg(long, env = "PCS_ADDR", global = true)]
    pub addr: Option<String>,

    /// Log format override.
    #[arg(long, env = "PCS_LOG_FORMAT", global = true, value_enum)]
    pub log_format: Option<LogFormatArg>,

    /// Log level override applied to the tracing filter.
    #[arg(long, env = "PCS_LOG_LEVEL", global = true)]
    pub log_level: Option<String>,
}

/// Top-level subcommands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the service (standalone or cluster, determined by config).
    Serve(ServeArgs),
    /// Validate a config file without starting the service.
    Validate(ValidateArgs),
    /// Query the status of a running service instance via its HTTP API.
    Status(StatusArgs),
    /// Cluster management subcommands.
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
}

/// Arguments for the `serve` subcommand.
#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Override the node ID (useful when deploying the same config to all nodes).
    #[arg(long, env = "PCS_NODE_ID")]
    pub node_id: Option<u64>,

    /// Override the HTTP bind port (0 = OS-assigned ephemeral port).
    ///
    /// Takes precedence over the `http.bind` port in the config file.
    /// Useful for testing: pass `--port 0` and read the bound address from
    /// stdout (`pcs-service listening on <addr>`).
    #[arg(long, env = "PCS_HTTP_PORT")]
    pub port: Option<u16>,
}

/// Arguments for the `validate` subcommand.
#[derive(clap::Args, Debug)]
pub struct ValidateArgs {
    // Config path comes from GlobalOpts --config.
    /// Treat unknown factory types as errors rather than warnings.
    ///
    /// By default, factory types not in the built-in registry (e.g. user-defined
    /// systems) produce warnings and the command exits 0. With `--strict`, unknown
    /// types cause a non-zero exit.
    #[arg(long)]
    pub strict: bool,
}

/// Arguments for the `status` subcommand.
#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// Fetch the full /status JSON (default: show a summary).
    #[arg(long)]
    pub full: bool,
}

/// Cluster management subcommands.
#[derive(Subcommand, Debug)]
pub enum ClusterCmd {
    /// Initialize a new cluster on this node. Only run once per cluster.
    Init,
    /// Join an existing cluster by contacting the leader.
    Join {
        /// HTTP address of the leader node (e.g., http://10.0.0.1:8080).
        #[arg(long)]
        leader: String,
    },
    /// Leave the cluster gracefully. Removes this node from Raft membership.
    Leave,
    /// Show cluster status (membership, roles, commit index, etc.).
    Status,
}

/// Log format selection for CLI override.
#[derive(ValueEnum, Clone, Debug)]
pub enum LogFormatArg {
    Pretty,
    Json,
}
