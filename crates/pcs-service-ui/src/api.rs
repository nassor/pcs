//! Typed fetches against the inspector's JSON API.
//!
//! One function per endpoint, each returning the shared wire type. Errors are
//! `String` because the only thing the UI does with one is show it: a typed
//! error enum would carry no information a viewer can act on differently.

use pcs_inspector_wire::{LogRecord, Snapshot, Topology, TraceDetail, TraceSummary};
use serde::de::DeserializeOwned;

/// GET `url` and decode the body.
async fn get_json<T: DeserializeOwned>(url: &str) -> Result<T, String> {
    let response = gloo_net::http::Request::get(url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if !response.ok() {
        return Err(format!("{url}: HTTP {}", response.status()));
    }
    response
        .json::<T>()
        .await
        .map_err(|e| format!("{url}: {e}"))
}

/// The static shape of the running workflow.
pub async fn topology() -> Result<Topology, String> {
    get_json("/api/topology").await
}

/// One frame of live numbers.
pub async fn snapshot(window_secs: u64) -> Result<Snapshot, String> {
    get_json(&format!("/api/snapshot?window_secs={window_secs}")).await
}

/// Newest-first trace list.
pub async fn traces(limit: usize) -> Result<Vec<TraceSummary>, String> {
    get_json(&format!("/api/traces?limit={limit}")).await
}

/// Spans and logs of one trace.
pub async fn trace(trace_id: u64) -> Result<TraceDetail, String> {
    get_json(&format!("/api/traces/{trace_id}")).await
}

/// Newest-first log tail, optionally filtered at or above `level`.
pub async fn logs(limit: usize, level: Option<&str>) -> Result<Vec<LogRecord>, String> {
    match level {
        Some(level) => get_json(&format!("/api/logs?limit={limit}&level={level}")).await,
        None => get_json(&format!("/api/logs?limit={limit}")).await,
    }
}
