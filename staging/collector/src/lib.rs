//! STAGING ONLY. NOT a production collector or an OTLP conformance claim.
//! Narrow OTLP/HTTP JSON admission -> single approved exporter with fail-closed errors.
//! gRPC, protobuf, gzip, durable buffering, multi-tenant policy and redaction are not built.
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use reqwest::{redirect::Policy, Client, Url};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

const MAX_BODY: usize = 64 * 1024;
const MAX_NODES: usize = 4096;
const MAX_RECORDS: usize = 256;

#[derive(Clone)]
pub struct CollectorState {
    expected_bearer: Arc<str>,
    tenant: Arc<str>,
    exporter_bearer: Arc<str>,
    exporter: Url,
    http: Client,
    permits: Arc<Semaphore>,
}

impl CollectorState {
    pub fn new(
        tenant: String, bearer: String, exporter: Url, exporter_bearer: String,
    ) -> Result<Self, &'static str> {
        if tenant.len() > 128 || tenant.is_empty() || !tenant.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')) {
            return Err("invalid tenant identity");
        }
        if bearer.len() < 24 || exporter_bearer.len() < 24 || bearer.chars().any(char::is_whitespace)
            || exporter_bearer.chars().any(char::is_whitespace) {
            return Err("test bearer configuration invalid");
        }
        if exporter.username() != "" || exporter.password().is_some() || exporter.query().is_some()
            || exporter.fragment().is_some() || !matches!(exporter.scheme(), "https" | "http")
            || (exporter.scheme() == "http" && !exporter.host_str().is_some_and(|h| matches!(h, "127.0.0.1" | "localhost" | "[::1]" | "::1"))) {
            return Err("exporter must be HTTPS or loopback HTTP with no embedded credentials");
        }
        let http = Client::builder()
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(1))
            .redirect(Policy::none())
            .build().map_err(|_| "exporter client configuration failed")?;
        Ok(Self {
            expected_bearer: bearer.into(), tenant: tenant.into(),
            exporter_bearer: exporter_bearer.into(), exporter, http,
            permits: Arc::new(Semaphore::new(8)),
        })
    }
}

pub fn router(state: CollectorState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { (StatusCode::OK, Json(json!({"ok": true}))) }))
        .route("/readyz", get(|| async { (StatusCode::OK, Json(json!({"ready": true}))) }))
        .route("/v1/traces", post(ingest_traces))
        .route("/v1/logs", post(ingest_logs))
        .route("/v1/metrics", post(ingest_metrics))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(state)
}

async fn ingest_traces(
    State(state): State<CollectorState>, headers: HeaderMap, body: Bytes,
) -> (StatusCode, Json<Value>) {
    ingest(state, headers, body, "traces").await
}
async fn ingest_logs(
    State(state): State<CollectorState>, headers: HeaderMap, body: Bytes,
) -> (StatusCode, Json<Value>) {
    ingest(state, headers, body, "logs").await
}
async fn ingest_metrics(
    State(state): State<CollectorState>, headers: HeaderMap, body: Bytes,
) -> (StatusCode, Json<Value>) {
    ingest(state, headers, body, "metrics").await
}

// Length-independent error values are never reflected in responses.
fn constant_time_equal(actual: &str, expected: &str) -> bool {
    let a = actual.as_bytes();
    let b = expected.as_bytes();
    let mut difference = a.len() ^ b.len();
    for idx in 0..a.len().max(b.len()) {
        difference |= (a.get(idx).copied().unwrap_or(0) ^ b.get(idx).copied().unwrap_or(0)) as usize;
    }
    difference == 0
}

fn denied(status: StatusCode) -> (StatusCode, Json<Value>) {
    (status, Json(json!({})))
}

async fn ingest(
    state: CollectorState, headers: HeaderMap, body: Bytes, signal: &'static str,
) -> (StatusCode, Json<Value>) {
    // Workload identity is derived only from the authenticated receiver credential.
    // A client-supplied tenant header cannot override or be forwarded as identity.
    let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or("");
    if !constant_time_equal(auth, &format!("Bearer {}", state.expected_bearer)) {
        return denied(StatusCode::UNAUTHORIZED);
    }
    if headers.contains_key("x-ores-tenant-id") {
        return denied(StatusCode::FORBIDDEN);
    }
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    if !matches!(ct, "application/json" | "application/json; charset=utf-8") ||
        headers.get(header::CONTENT_ENCODING).is_some_and(|v| v != "identity") {
        return denied(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    if body.is_empty() {
        return denied(StatusCode::BAD_REQUEST);
    }
    if body.len() > MAX_BODY {
        return denied(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return denied(StatusCode::TOO_MANY_REQUESTS),
    };
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(_) => return denied(StatusCode::BAD_REQUEST),
    };
    if !valid_payload(&payload, signal) {
        return denied(StatusCode::BAD_REQUEST);
    }
    // No untrusted target override, redirects, implicit retries or in-memory queues.
    // 503 signals that admission was NOT durable, including exporter failure.
    let mut endpoint = state.exporter.clone();
    let mut prefix = endpoint.path().trim_end_matches('/').to_owned();
    prefix.push_str("/v1/");
    prefix.push_str(signal);
    endpoint.set_path(&prefix);
    let result = state.http.post(endpoint)
        .header("x-ores-tenant-id", state.tenant.as_ref())
        .header(header::CONTENT_TYPE, "application/json")
        .bearer_auth(state.exporter_bearer.as_ref())
        .body(body)
        .send().await;
    match result {
        Ok(mut response) if response.status() == reqwest::StatusCode::OK => {
            // A 200 may contain OTLP partial-success rejection. Do not acknowledge it.
            // The exporter response body is also a bounded, untrusted input.
            let mut receipt = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) if receipt.len().saturating_add(chunk.len()) <= 8192 =>
                        receipt.extend_from_slice(&chunk),
                    Ok(None) => break,
                    _ => return denied(StatusCode::SERVICE_UNAVAILABLE),
                }
            }
            if exporter_accepted(&receipt, signal) {
                (StatusCode::OK, Json(json!({})))
            } else {
                denied(StatusCode::SERVICE_UNAVAILABLE)
            }
        }
        _ => denied(StatusCode::SERVICE_UNAVAILABLE),
    }
}

fn exporter_accepted(receipt: &[u8], signal: &str) -> bool {
    if receipt.is_empty() { return true; }
    let Ok(message) = serde_json::from_slice::<Value>(receipt) else { return false; };
    let Some(obj) = message.as_object() else { return false; };
    let Some(partial) = obj.get("partialSuccess") else { return true; };
    let Some(data) = partial.as_object() else { return false; };
    let rejected_name = match signal {
        "traces" => "rejectedSpans",
        "logs" => "rejectedLogRecords",
        "metrics" => "rejectedDataPoints",
        _ => return false,
    };
    if let Some(rejected) = data.get(rejected_name) {
        if !matches!(rejected, Value::Number(n) if n.as_u64() == Some(0))
            && !matches!(rejected, Value::String(s) if s == "0") {
            return false;
        }
    }
    if let Some(error) = data.get("errorMessage") {
        if !matches!(error, Value::String(s) if s.is_empty()) {
            return false;
        }
    }
    true
}

fn forbidden_attribute(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    ["password", "authorization", "cookie", "secret", "credential", "api_key", "private_key", "access_token", "refresh_token"]
        .iter().any(|needle| k.contains(needle))
}

fn bounded(value: &Value, depth: usize, nodes: &mut usize) -> bool {
    *nodes += 1;
    if *nodes > MAX_NODES || depth > 24 {
        return false;
    }
    match value {
        Value::String(s) => s.len() <= 4096,
        Value::Array(items) => items.len() <= 256 && items.iter().all(|item| bounded(item, depth + 1, nodes)),
        Value::Object(map) => {
            if map.len() > 128 { return false; }
            if let Some(Value::String(key)) = map.get("key") {
                if key.len() > 128 || forbidden_attribute(key) { return false; }
            }
            map.iter().all(|(key, v)| key.len() <= 128 && bounded(v, depth + 1, nodes))
        }
        _ => true,
    }
}

fn valid_payload(value: &Value, signal: &str) -> bool {
    let mut nodes = 0;
    if !bounded(value, 0, &mut nodes) { return false; }
    let (root, scope, list) = match signal {
        "traces" => ("resourceSpans", "scopeSpans", "spans"),
        "logs" => ("resourceLogs", "scopeLogs", "logRecords"),
        "metrics" => ("resourceMetrics", "scopeMetrics", "metrics"),
        _ => return false,
    };
    let Some(resources) = value.get(root).and_then(Value::as_array) else { return false; };
    if resources.is_empty() || resources.len() > 16 { return false; }
    let mut records = 0;
    for resource in resources {
        let Some(scopes) = resource.get(scope).and_then(Value::as_array) else { return false; };
        if scopes.is_empty() || scopes.len() > 32 { return false; }
        for s in scopes {
            let Some(items) = s.get(list).and_then(Value::as_array) else { return false; };
            if items.is_empty() { return false; }
            records += items.len();
            if records > MAX_RECORDS { return false; }
            for item in items {
                if !item.is_object() { return false; }
                if signal == "metrics" && !item.get("name").is_some_and(Value::is_string) { return false; }
                if signal == "traces" && !valid_span(item) { return false; }
            }
        }
    }
    records > 0
}

fn valid_span(value: &Value) -> bool {
    let hex = |key: &str, len: usize| {
        value.get(key).and_then(Value::as_str)
            .is_some_and(|s| s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit()) && s.bytes().any(|b| b != b'0'))
    };
    hex("traceId", 32) && hex("spanId", 16) && value.get("name").is_some_and(Value::is_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refuse_partial_exporter_acknowledgments() {
        assert!(exporter_accepted(b"{}", "logs"));
        assert!(exporter_accepted(b"", "logs"));
        assert!(exporter_accepted(br#"{"partialSuccess":{"rejectedSpans":0}}"#, "traces"));
        assert!(!exporter_accepted(br#"{"partialSuccess":{"rejectedSpans":1}}"#, "traces"));
        assert!(!exporter_accepted(br#"{"partialSuccess":{"rejectedLogRecords":"2"}}"#, "logs"));
        assert!(!exporter_accepted(br#"{"partialSuccess":{"rejectedDataPoints":1}}"#, "metrics"));
        assert!(!exporter_accepted(br#"{"partialSuccess":{"errorMessage":"partial failure"}}"#, "logs"));
        assert!(!exporter_accepted(b"not-json", "traces"));
    }
    #[test]
    fn reject_secret_and_unbounded_payloads() {
        let base = json!({"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"synthetic"}}]}]}]});
        assert!(valid_payload(&base, "logs"));
        let bad = json!({"resourceLogs":[{"scopeLogs":[{"logRecords":[{"attributes":[{"key":"http.request.header.authorization","value":{"stringValue":"never export"}}]}]}]}]});
        assert!(!valid_payload(&bad, "logs"));
        assert!(!valid_payload(&json!({"resourceSpans":[]}), "traces"));
        assert!(!constant_time_equal("Bearer fake", "Bearer expected"));
    }
}
