//! Integration tests for `pcs-service`. Each test spawns the `pcs-service` binary
//! as a subprocess and drives it over HTTP or by its exit code.
//!
//! `cargo build --features service` must run first so the binary exists at
//! `env!("CARGO_BIN_EXE_pcs-service")`.
//!
//! ```text
//! cargo test --test service_integration --all-features -- --test-threads=4
//! ```
//!
//! Tests pass `--port 0` and parse the `pcs-service listening on <addr>` line from
//! stdout, so no port is hardcoded. Each test gets its own
//! [`tempfile::TempDir`] for `node.data_dir`, and [`ChildGuard`] kills and reaps
//! the child even if the test panics.

#![cfg(feature = "service")]

use std::io::{BufRead, BufReader, Write};
use std::time::Duration;

use tempfile::NamedTempFile;

// Path to the compiled binary, set by cargo for integration tests.
const BIN: &str = env!("CARGO_BIN_EXE_pcs-service");

/// Kills and reaps the child on drop, so no zombie process leaks out of a test.
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Best-effort kill; ignore errors (process may have already exited).
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

/// Writes a minimal standalone TOML config to a temp file. `data_dir` keeps each
/// test isolated, and `http.bind` uses port 0 so the binary picks an ephemeral
/// port and prints the address.
fn write_standalone_config(data_dir: &std::path::Path) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");
    write!(
        f,
        r#"
mode = "standalone"

[node]
id = 1
data_dir = "{data_dir_str}"

[run_mode]
kind = "continuous"

[http]
bind = "127.0.0.1:0"
"#
    )
    .expect("write config");
    f
}

/// Write a deliberately malformed TOML to a temp file.
fn write_bad_config() -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    // Unclosed bracket, so the TOML parse fails.
    writeln!(f, "this = [unclosed").expect("write bad config");
    f
}

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

/// Spawns the binary with `serve --config <path> --port 0`, reads stdout until the
/// `pcs-service listening on <addr>` line appears, and returns the
/// [`ChildGuard`] and the bound address. Panics if the line never arrives within
/// `timeout`.
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

#[tokio::test]
async fn test_serve_health_endpoint_returns_200() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let (mut guard, addr) = spawn_serve_and_read_port(config.path(), Duration::from_secs(10));

    let health_url = format!("http://{addr}/health");
    let poll_result = poll_until_200(&health_url, Duration::from_secs(5)).await;

    terminate_child(&guard.0);

    let exited = wait_child_timeout(&mut guard.0, Duration::from_secs(5));
    if !exited {
        guard.0.kill().ok();
        guard.0.wait().ok();
    }

    poll_result.expect("/health should return 200 within 5 seconds of startup");
    assert!(exited, "service should exit within 5 seconds of SIGTERM");
}

#[tokio::test]
async fn test_status_subcommand_hits_running_service() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let (mut guard, addr) = spawn_serve_and_read_port(config.path(), Duration::from_secs(10));

    let health_url = format!("http://{addr}/health");
    poll_until_200(&health_url, Duration::from_secs(5))
        .await
        .expect("service did not start in time");

    let output = std::process::Command::new(BIN)
        .arg("status")
        .arg("--addr")
        .arg(format!("http://{addr}"))
        .output()
        .expect("failed to run status");

    let status_stdout = String::from_utf8_lossy(&output.stdout);
    let status_stderr = String::from_utf8_lossy(&output.stderr);

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
