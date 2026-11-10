use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    routing::post,
    Router,
};
use ores_otel_collector_staging::{router, CollectorState};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::{net::TcpListener, task::JoinHandle};

const CLIENT_BEARER: &str = "this-is-a-synthetic-test-ingest-bearer";
const EXPORT_BEARER: &str = "this-is-a-synthetic-test-export-bearer";

#[derive(Clone)]
struct ExportState {
    calls: Arc<Mutex<Vec<(String, String, Value)>>>,
    fail: bool,
}
async fn exported(
    State(state): State<ExportState>,
    uri: Uri, headers: HeaderMap, body: Bytes,
) -> StatusCode {
    let tenant = headers.get("x-ores-tenant-id").and_then(|v| v.to_str().ok()).unwrap_or("");
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
    if tenant != "tenant_a" || auth != format!("Bearer {EXPORT_BEARER}") {
        return StatusCode::UNAUTHORIZED;
    }
    let value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    state.calls.lock().unwrap().push((uri.path().into(), tenant.into(), value));
    if state.fail { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK }
}
async fn serve(app: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    (url, task)
}
async fn setup(fail: bool) -> (String, ExportState, JoinHandle<()>, JoinHandle<()>) {
    let sink = ExportState { calls: Arc::new(Mutex::new(Vec::new())), fail };
    let exporter = Router::new()
        .route("/v1/logs", post(exported))
        .route("/v1/traces", post(exported))
        .route("/v1/metrics", post(exported))
        .with_state(sink.clone());
    let (export_url, export_task) = serve(exporter).await;
    let state = CollectorState::new(
        "tenant_a".into(), CLIENT_BEARER.into(), Url::parse(&export_url).unwrap(), EXPORT_BEARER.into(),
    ).unwrap();
    let (url, collector_task) = serve(router(state)).await;
    (url, sink, export_task, collector_task)
}
fn event(signal: &str) -> Value {
    match signal {
        "logs" => json!({"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"synthetic message"}}]}]}]}),
        "traces" => json!({"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","name":"staging.test"}]}]}]}),
        "metrics" => json!({"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"staging_test_total","sum":{"dataPoints":[{"asInt":"1"}],"aggregationTemporality":2,"isMonotonic":true}}]}]}]}),
        _ => panic!("invalid test signal"),
    }
}
async fn post(client: &Client, url: &str, signal: &str, bearer: Option<&str>, body: String) -> reqwest::Response {
    let mut req = client.post(format!("{url}/v1/{signal}")).header("content-type", "application/json").body(body);
    if let Some(token) = bearer { req = req.bearer_auth(token); }
    req.send().await.unwrap()
}

#[tokio::test]
async fn real_tcp_all_signals_forward_once_with_trusted_tenant() {
    let (base, sink, export_task, collector_task) = setup(false).await;
    let client = Client::new();
    for signal in ["logs", "traces", "metrics"] {
        let res = post(&client, &base, signal, Some(CLIENT_BEARER), event(signal).to_string()).await;
        assert_eq!(res.status(), StatusCode::OK);
    }
    let calls = sink.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls.iter().map(|(path, _, _)| path.as_str()).collect::<Vec<_>>(),
        vec!["/v1/logs", "/v1/traces", "/v1/metrics"]);
    assert!(calls.iter().all(|(_, tenant, _)| tenant == "tenant_a"));
    export_task.abort();
    collector_task.abort();
}

#[tokio::test]
async fn real_tcp_denies_anonymous_forged_tenant_secret_and_malformed() {
    let (base, sink, export_task, collector_task) = setup(false).await;
    let client = Client::new();
    let body = event("logs").to_string();
    assert_eq!(post(&client, &base, "logs", None, body.clone()).await.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(post(&client, &base, "logs", Some("wrong-token"), body.clone()).await.status(), StatusCode::UNAUTHORIZED);
    let spoof = client.post(format!("{base}/v1/logs"))
        .bearer_auth(CLIENT_BEARER).header("content-type", "application/json")
        .header("x-ores-tenant-id", "tenant_b").body(body.clone()).send().await.unwrap();
    assert_eq!(spoof.status(), StatusCode::FORBIDDEN);
    assert_eq!(post(&client, &base, "logs", Some(CLIENT_BEARER), "{".into()).await.status(), StatusCode::BAD_REQUEST);
    let bad = json!({"resourceLogs":[{"scopeLogs":[{"logRecords":[{
        "attributes":[{"key":"http.request.header.authorization","value":{"stringValue":"should-not-leak"}}]
    }]}]}]});
    assert_eq!(post(&client, &base, "logs", Some(CLIENT_BEARER), bad.to_string()).await.status(), StatusCode::BAD_REQUEST);
    assert_eq!(sink.calls.lock().unwrap().len(), 0);
    export_task.abort();
    collector_task.abort();
}

#[tokio::test]
async fn real_tcp_does_not_ack_on_export_failure_or_unsupported_encoding() {
    let (base, sink, export_task, collector_task) = setup(true).await;
    let client = Client::new();
    let res = post(&client, &base, "traces", Some(CLIENT_BEARER), event("traces").to_string()).await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    let compressed = client.post(format!("{base}/v1/logs"))
        .bearer_auth(CLIENT_BEARER).header("content-type", "application/json")
        .header("content-encoding", "gzip").body(event("logs").to_string()).send().await.unwrap();
    assert_eq!(compressed.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let giant = json!({"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"a".repeat(70000)}}]}]}]});
    let reject = post(&client, &base, "logs", Some(CLIENT_BEARER), giant.to_string()).await;
    assert_eq!(reject.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(sink.calls.lock().unwrap().len(), 1);
    export_task.abort();
    collector_task.abort();
}

#[test]
fn rejects_insecure_remote_exporter_and_short_credentials() {
    let remote = Url::parse("http://example.com").unwrap();
    let local = Url::parse("http://127.0.0.1:4318").unwrap();
    assert!(CollectorState::new("tenant_a".into(), CLIENT_BEARER.into(), remote, EXPORT_BEARER.into()).is_err());
    assert!(CollectorState::new("tenant_a".into(), "short".into(), local, EXPORT_BEARER.into()).is_err());
}
