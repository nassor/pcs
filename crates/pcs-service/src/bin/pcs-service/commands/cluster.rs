//! `pcs-service cluster` — cluster management subcommands.
//!
//! The entire body of this module is gated on `feature = "service-cluster"`.
//! In a `service`-only build (standalone) the CLI still recognises the
//! `cluster` subcommand (the `clap` types live in `cli.rs` unconditionally),
//! but invoking it returns a clear "not built with service-cluster" error
//! instead of a nonsense "unrecognised subcommand" failure.
//!
//! ## v1 limitations (when enabled)
//!
//! - `cluster join` and `cluster leave` are not yet wired to the HTTP control
//!   plane.  The service does not expose a `/cluster/membership` endpoint in
//!   v1.  Both commands print a manual workaround.
//! - `cluster init` is a usability shortcut: it validates the config, confirms
//!   `bootstrap = true`, and advises the operator to run `pcs-service serve`.
//! - `cluster status` delegates to the `/status` HTTP endpoint and extracts
//!   the `cluster` field.

use pcs_service::PcsError;
#[cfg(feature = "service-cluster")]
use pcs_service::service::config::{ServiceConfig, ServiceMode};

use crate::cli::{ClusterCmd, GlobalOpts};

/// Entry point for the `cluster` subcommand.
#[cfg(feature = "service-cluster")]
pub async fn run(global: &GlobalOpts, cmd: &ClusterCmd) -> Result<(), PcsError> {
    match cmd {
        ClusterCmd::Init => cmd_init(global).await,
        ClusterCmd::Join { leader } => cmd_join(global, leader).await,
        ClusterCmd::Leave => cmd_leave(global).await,
        ClusterCmd::Status => cmd_status(global).await,
    }
}

/// Fallback entry point used in `service`-only builds.
///
/// Returns a clear error explaining that cluster support was not compiled in.
/// Keeps the `cluster` subcommand visible in `--help` so operators don't
/// mistake its absence for a CLI typo.
#[cfg(not(feature = "service-cluster"))]
pub async fn run(_global: &GlobalOpts, _cmd: &ClusterCmd) -> Result<(), PcsError> {
    Err(PcsError::configuration(
        "this binary was built without the `service-cluster` feature — \
         rebuild with `--features service-cluster` to use cluster subcommands",
    ))
}

// ── cluster init ──────────────────────────────────────────────────────────────

/// Validate the config and confirm it is bootstrap-ready.
///
/// This command does NOT start the node.  It is a pre-flight check that:
/// 1. Loads and validates the TOML.
/// 2. Confirms `mode = cluster` and `bootstrap = true`.
/// 3. Prints next-step instructions.
///
/// To actually bootstrap the cluster, run `pcs-service serve --config <path>`
/// with the same file.
#[cfg(feature = "service-cluster")]
async fn cmd_init(global: &GlobalOpts) -> Result<(), PcsError> {
    let config_path = global
        .config
        .as_ref()
        .ok_or_else(|| PcsError::configuration("--config is required for cluster init"))?;

    let config = ServiceConfig::load(config_path)?;

    match &config.mode {
        ServiceMode::Cluster {
            config: cluster_cfg,
        } => {
            if !cluster_cfg.bootstrap {
                return Err(PcsError::configuration(
                    "cluster.bootstrap is false in the config. \
                     Set bootstrap: true to initialise a new cluster.",
                ));
            }
            println!("OK: config is valid and cluster.bootstrap = true");
            println!("  node.id:  {}", config.node.id);
            println!("  peers:    {}", cluster_cfg.peers.len());
            println!();
            println!(
                "To bootstrap the cluster, start this node with:\n  \
                 pcs-service serve --config {}",
                config_path.display()
            );
            println!();
            println!(
                "IMPORTANT: run `pcs-service serve` on ONE node first. \
                 After the leader is elected, start the remaining nodes \
                 with bootstrap: false."
            );
        }
        ServiceMode::Standalone { .. } => {
            return Err(PcsError::configuration(
                "config is in standalone mode. cluster init requires mode: cluster",
            ));
        }
    }

    Ok(())
}

// ── cluster join ──────────────────────────────────────────────────────────────

/// Join an existing cluster.
///
/// ## v1 limitation
///
/// The HTTP control plane does not expose a `/cluster/membership` endpoint in
/// v1.  Dynamic membership changes via the CLI are therefore not yet supported.
///
/// To add a node to an existing cluster in v1:
/// 1. Update the `peers` list in the config on ALL nodes.
/// 2. Set `bootstrap: false` on the new node.
/// 3. Restart all nodes.
#[cfg(feature = "service-cluster")]
async fn cmd_join(_global: &GlobalOpts, leader: &str) -> Result<(), PcsError> {
    eprintln!(
        "Note: cluster join via HTTP is not yet implemented in v1.\n\
         Leader address provided: {leader}\n\
         \n\
         To add a node to an existing cluster:\n\
         1. Add the new node's entry to the 'peers' list in the config on all nodes.\n\
         2. Set 'bootstrap: false' on the new node.\n\
         3. Restart all nodes."
    );
    Ok(())
}

// ── cluster leave ─────────────────────────────────────────────────────────────

/// Remove this node from the cluster gracefully.
///
/// ## v1 limitation
///
/// Same as `cluster join` — there is no HTTP membership endpoint in v1.
/// Use manual config updates to remove a node.
#[cfg(feature = "service-cluster")]
async fn cmd_leave(_global: &GlobalOpts) -> Result<(), PcsError> {
    eprintln!(
        "Note: cluster leave via HTTP is not yet implemented in v1.\n\
         \n\
         To remove a node from the cluster:\n\
         1. Stop the node process.\n\
         2. Remove the node's entry from the 'peers' list in the config on all remaining nodes.\n\
         3. Restart the remaining nodes."
    );
    Ok(())
}

// ── cluster status ────────────────────────────────────────────────────────────

/// Show cluster status from the running node's HTTP API.
///
/// Queries `/status` and extracts the `cluster` field.  If the node has no
/// cluster probe wired (v1 limitation), the `cluster` field will be null.
#[cfg(feature = "service-cluster")]
async fn cmd_status(global: &GlobalOpts) -> Result<(), PcsError> {
    let addr = global.addr.as_ref().ok_or_else(|| {
        PcsError::configuration(
            "--addr is required for cluster status (e.g., http://localhost:8080)",
        )
    })?;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/status"))
        .send()
        .await
        .map_err(|e| PcsError::generic(format!("failed to reach {addr}: {e}")))?;

    if !resp.status().is_success() {
        return Err(PcsError::generic(format!(
            "status endpoint returned HTTP {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| PcsError::generic(format!("failed to parse /status JSON: {e}")))?;

    let node_id = body.get("node_id").and_then(|v| v.as_u64()).unwrap_or(0);
    let mode = body
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    if mode != "cluster" {
        println!("node {node_id} is running in {mode} mode (not cluster)");
        return Ok(());
    }

    match body.get("cluster") {
        Some(cluster) if !cluster.is_null() => {
            println!(
                "{}",
                serde_json::to_string_pretty(cluster).unwrap_or_else(|_| cluster.to_string())
            );
        }
        _ => {
            // v1 limitation: cluster_probe is not yet wired in the serve command.
            println!("node {node_id}  mode=cluster");
            println!(
                "Note: cluster details are not available in v1. \
                 Full Raft metrics integration is planned for v1.1."
            );
        }
    }

    Ok(())
}
