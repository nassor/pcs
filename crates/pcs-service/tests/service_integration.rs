//! Integration tests for `pcs-service`.
//!
//! These tests spawn the `pcs-service` binary as a subprocess and interact
//! with it via HTTP or by observing its exit code.
//!
//! ## Prerequisites
//!
//! `cargo build --features service` must run before these tests so the binary
//! exists at `env!("CARGO_BIN_EXE_pcs-service")`.
//!
//! Run with:
//! ```text
//! cargo test --test service_integration --all-features -- --test-threads=4
//! ```
//!
//! ## Port allocation
//!
//! Tests pass `--port 0` to the binary so the OS assigns an ephemeral port.
//! The binary prints `pcs-service listening on <addr>` to stdout; tests parse
//! that line to discover the actual port.  No hardcoded ports, no collisions.
//!
//! ## Isolation
//!
//! Each test creates a fresh [`tempfile::TempDir`] for `node.data_dir`.
//! Directories are cleaned up automatically when the guard drops.
//!
//! ## Process cleanup
//!
//! A [`ChildGuard`] RAII wrapper ensures the child process is killed and
//! reaped even if the test panics.

#![cfg(feature = "service")]

use std::io::{BufRead, BufReader, Write};
use std::time::Duration;

use tempfile::NamedTempFile;

// Path to the compiled binary, set by cargo for integration tests.
const BIN: &str = env!("CARGO_BIN_EXE_pcs-service");

// ── RAII child-process guard ──────────────────────────────────────────────────

/// Wraps a [`std::process::Child`] and kills + waits it on drop.
///
/// This ensures no zombie processes leak from tests, even on panic.
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Best-effort kill; ignore errors (process may have already exited).
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

// ── Config helpers ────────────────────────────────────────────────────────────

/// Write a minimal standalone TOML config to a temp file.
///
/// `data_dir` is passed in so each test can use its own isolated directory.
/// The `http.bind` uses port 0; pass `--port 0` when calling `serve` and the
/// binary will assign an ephemeral port and print the address to stdout.
fn write_standalone_config(data_dir: &std::path::Path) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    write!(
        f,
        r#"
mode = "standalone"

[node]
id = 1
data_dir = "{data_dir}"

[run_mode]
kind = "continuous"

[pipeline]
systems = []

[http]
bind = "127.0.0.1:0"
"#,
        data_dir = data_dir.display()
    )
    .expect("write config");
    f
}

/// Write a deliberately malformed TOML to a temp file.
fn write_bad_config() -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    // Unclosed bracket — TOML parse will fail.
    writeln!(f, "this = [unclosed").expect("write bad config");
    f
}

// ── Signal helpers ────────────────────────────────────────────────────────────

/// Send SIGTERM to a child process on Unix; kill on other platforms.
fn terminate_child(child: &std::process::Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    #[cfg(not(unix))]
    {
        // Windows: no SIGTERM, kill directly via a separate Command.
        std::process::Command::new("taskkill")
            .args(["/PID", &child.id().to_string(), "/F"])
            .status()
            .ok();
    }
}

// ── Stdout port-readback helper ───────────────────────────────────────────────

/// Spawn the binary with `serve --config <path> --port 0`, read stdout until
/// the `pcs-service listening on <addr>` line appears, and return both the
/// child (wrapped in a [`ChildGuard`]) and the bound address string.
///
/// Fails with a panic if the line is not found within `timeout`.
fn spawn_serve_and_read_port(
    config_path: &std::path::Path,
    timeout: Duration,
) -> (ChildGuard, String) {
    let mut child = std::process::Command::new(BIN)
        .arg("serve")
        .arg("--config")
        .arg(config_path)
        .arg("--port")
        .arg("0")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn pcs-service");

    let stdout = child.stdout.take().expect("stdout piped");
    let reader = BufReader::new(stdout);

    let deadline = std::time::Instant::now() + timeout;
    let mut addr = None;

    for line in reader.lines() {
        let line = line.expect("read stdout line");
        // The binary prints: "pcs-service listening on 127.0.0.1:<port>"
        if let Some(rest) = line.strip_prefix("pcs-service listening on ") {
            addr = Some(rest.trim().to_string());
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for 'pcs-service listening on' line after {timeout:?}");
        }
    }

    let addr = addr.unwrap_or_else(|| {
        panic!("binary exited before printing bind address");
    });

    (ChildGuard(child), addr)
}

// ── Poll helper ───────────────────────────────────────────────────────────────

/// Poll `url` with GET until a 200 response or `timeout` elapses.
async fn poll_until_200(url: &str, timeout: Duration) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("timed out polling {url} after {timeout:?}"));
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// Wait for a child process to exit within `timeout`, polling every 100 ms.
/// Returns `true` if the process exited in time, `false` on timeout.
fn wait_child_timeout(child: &mut std::process::Child, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ── Test 1: --help output contains all subcommands ────────────────────────────

#[test]
fn test_help_output_contains_all_subcommands() {
    let output = std::process::Command::new(BIN)
        .arg("--help")
        .output()
        .expect("failed to run pcs-service --help");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");

    for cmd in &["serve", "validate", "status", "cluster"] {
        assert!(
            combined.contains(cmd),
            "help output missing '{cmd}': {combined}"
        );
    }
}

// ── Test 2: validate on a valid config returns exit 0 ────────────────────────

#[test]
fn test_validate_valid_config_exits_zero() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());
    let status = std::process::Command::new(BIN)
        .arg("validate")
        .arg("--config")
        .arg(config.path())
        .status()
        .expect("failed to run validate");

    assert!(status.success(), "validate should exit 0 on valid config");
}

// ── Test 3: validate on an invalid config returns nonzero ────────────────────

#[test]
fn test_validate_invalid_config_exits_nonzero() {
    let config = write_bad_config();
    let status = std::process::Command::new(BIN)
        .arg("validate")
        .arg("--config")
        .arg(config.path())
        .status()
        .expect("failed to run validate");

    assert!(
        !status.success(),
        "validate should exit nonzero on invalid config"
    );
}

// ── Test 4: validate prints node.id and mode ─────────────────────────────────

#[test]
fn test_validate_output_contains_node_info() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());
    let output = std::process::Command::new(BIN)
        .arg("validate")
        .arg("--config")
        .arg(config.path())
        .output()
        .expect("failed to run validate");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("node.id"),
        "validate output should contain node.id: {stdout}"
    );
    assert!(
        stdout.contains("standalone"),
        "validate output should contain mode: {stdout}"
    );
}

// ── Test 5: service starts, /health returns 200, then shuts down cleanly ─────

#[tokio::test]
async fn test_serve_health_endpoint_returns_200() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let (mut guard, addr) = spawn_serve_and_read_port(config.path(), Duration::from_secs(10));

    // Poll /health until 200 or 5-second timeout.
    let health_url = format!("http://{addr}/health");
    let poll_result = poll_until_200(&health_url, Duration::from_secs(5)).await;

    // Send SIGTERM / kill.
    terminate_child(&guard.0);

    // Wait up to 5 seconds for clean exit.
    let exited = wait_child_timeout(&mut guard.0, Duration::from_secs(5));
    if !exited {
        guard.0.kill().ok();
        guard.0.wait().ok();
    }

    poll_result.expect("/health should return 200 within 5 seconds of startup");
    assert!(exited, "service should exit within 5 seconds of SIGTERM");
}

// ── Test 6: status subcommand hits running service ────────────────────────────

#[tokio::test]
async fn test_status_subcommand_hits_running_service() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let (mut guard, addr) = spawn_serve_and_read_port(config.path(), Duration::from_secs(10));

    // Wait for service to be up.
    let health_url = format!("http://{addr}/health");
    poll_until_200(&health_url, Duration::from_secs(5))
        .await
        .expect("service did not start in time");

    // Run pcs-service status.
    let output = std::process::Command::new(BIN)
        .arg("status")
        .arg("--addr")
        .arg(format!("http://{addr}"))
        .output()
        .expect("failed to run status");

    let status_stdout = String::from_utf8_lossy(&output.stdout);
    let status_stderr = String::from_utf8_lossy(&output.stderr);

    // Shut down the service.
    terminate_child(&guard.0);
    wait_child_timeout(&mut guard.0, Duration::from_secs(5));
    guard.0.kill().ok();
    guard.0.wait().ok();

    assert!(
        output.status.success(),
        "status command should exit 0, stderr: {status_stderr}"
    );
    assert!(
        status_stdout.contains("node"),
        "status output should contain 'node': {status_stdout}"
    );
}
