use std::{
    env,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use axum::{
    body::{to_bytes, Body},
    extract::{OriginalUri, State},
    http::{
        header::{CONTENT_ENCODING, CONTENT_TYPE},
        HeaderMap, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use reqwest::Url;
use serde::Serialize;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::sync::Semaphore;

pub const DEFAULT_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
pub const HARD_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENT: usize = 64;
const MAX_UPSTREAM_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub upstream_base: Url,
    pub internal_auth: Option<String>,
    pub max_body_bytes: usize,
    pub max_concurrent: usize,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid bind address")]
    InvalidBind,
    #[error("ORES_OTEL_UPSTREAM_URL is required and must be an approved http(s) endpoint")]
    InvalidUpstream,
    #[error("non-loopback bind requires ORES_OTEL_INTERNAL_AUTH")]
    MissingAuth,
    #[error("invalid numeric limit: {0}")]
    InvalidLimit(&'static str),
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind: SocketAddr = env::var("ORES_OTEL_BIND")
            .unwrap_or_else(|_| "127.0.0.1:4318".into())
            .parse()
            .map_err(|_| ConfigError::InvalidBind)?;
        let upstream_raw =
            env::var("ORES_OTEL_UPSTREAM_URL").map_err(|_| ConfigError::InvalidUpstream)?;
        let upstream_base = validate_upstream(&upstream_raw)?;
        let internal_auth = env::var("ORES_OTEL_INTERNAL_AUTH")
            .ok()
            .filter(|v| !v.is_empty());
        if !bind.ip().is_loopback() && internal_auth.is_none() {
            return Err(ConfigError::MissingAuth);
        }
        let max_body_bytes = parse_bounded_env(
            "ORES_OTEL_MAX_BODY_BYTES",
            DEFAULT_MAX_BODY_BYTES,
            1,
            HARD_MAX_BODY_BYTES,
        )?;
        let max_concurrent =
            parse_bounded_env("ORES_OTEL_MAX_CONCURRENT", DEFAULT_MAX_CONCURRENT, 1, 4096)?;
        Ok(Self {
            bind,
            upstream_base,
            internal_auth,
            max_body_bytes,
            max_concurrent,
        })
    }
}

fn parse_bounded_env(
    name: &'static str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, ConfigError> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(default);
    };
    let parsed = raw
        .parse::<usize>()
        .map_err(|_| ConfigError::InvalidLimit(name))?;
    if !(min..=max).contains(&parsed) {
        return Err(ConfigError::InvalidLimit(name));
    }
    Ok(parsed)
}

pub fn validate_upstream(raw: &str) -> Result<Url, ConfigError> {
    let url = Url::parse(raw).map_err(|_| ConfigError::InvalidUpstream)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::InvalidUpstream);
    }
    match url.scheme() {
        "https" => Ok(url),
        "http" if is_loopback_host(&url) => Ok(url),
        _ => Err(ConfigError::InvalidUpstream),
    }
}

fn is_loopback_host(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false),
        None => false,
    }
}

#[derive(Clone)]
struct CollectorState {
    config: Config,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
}

pub fn router(config: Config) -> Router {
    let state = CollectorState {
        permits: Arc::new(Semaphore::new(config.max_concurrent)),
        config,
        client: reqwest::Client::new(),
    };
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(|| async { StatusCode::OK }))
        .route("/v1/traces", post(ingest))
        .route("/v1/metrics", post(ingest))
        .route("/v1/logs", post(ingest))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: &'static str,
}

async fn ingest(
    State(state): State<CollectorState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (tenant_id, workload_id) = match authorize(&state.config, &headers) {
        Ok(scope) => scope,
        Err(response) => return response,
    };

    let content_type = match headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) {
        Some(value)
            if value.starts_with("application/x-protobuf")
                || value.starts_with("application/json") =>
        {
            value.to_owned()
        }
        _ => {
            return error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_content_type",
            )
        }
    };
    if headers.get(CONTENT_ENCODING).is_some() {
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "compressed_otlp_not_yet_supported",
        );
    }

    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return error(StatusCode::TOO_MANY_REQUESTS, "collector_overloaded"),
    };

    let bytes = match to_bytes(body, state.config.max_body_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
    };

    let upstream = match state
        .config
        .upstream_base
        .join(uri.path().trim_start_matches('/'))
    {
        Ok(url) => url,
        Err(_) => return error(StatusCode::BAD_GATEWAY, "invalid_upstream_route"),
    };

    let response = match state
        .client
        .post(upstream)
        .header(CONTENT_TYPE, content_type)
        .header("x-ores-tenant-id", tenant_id)
        .header("x-ores-workload-id", workload_id)
        .body(bytes)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return error(StatusCode::BAD_GATEWAY, "upstream_unavailable"),
    };

    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let response_content_type = response.headers().get(CONTENT_TYPE).cloned();
    let response_bytes = match response.bytes().await {
        Ok(bytes) if bytes.len() <= MAX_UPSTREAM_RESPONSE_BYTES => bytes,
        _ => return error(StatusCode::BAD_GATEWAY, "invalid_upstream_response"),
    };

    let mut builder = Response::builder().status(status);
    if let Some(value) = response_content_type {
        builder = builder.header(CONTENT_TYPE, value);
    }
    builder
        .body(Body::from(response_bytes))
        .unwrap_or_else(|_| error(StatusCode::BAD_GATEWAY, "response_build_failed"))
}

fn authorize(config: &Config, headers: &HeaderMap) -> Result<(String, String), Response> {
    if let Some(expected) = config.internal_auth.as_deref() {
        let Some(actual) = headers
            .get("x-ores-internal-auth")
            .and_then(|v| v.to_str().ok())
        else {
            return Err(error(StatusCode::UNAUTHORIZED, "missing_internal_auth"));
        };
        if expected.as_bytes().ct_eq(actual.as_bytes()).unwrap_u8() != 1 {
            return Err(error(StatusCode::UNAUTHORIZED, "invalid_internal_auth"));
        }
    }

    let tenant_id = required_identity(headers, "x-ores-tenant-id")?;
    let workload_id = required_identity(headers, "x-ores-workload-id")?;
    Ok((tenant_id, workload_id))
}

fn required_identity(headers: &HeaderMap, name: &'static str) -> Result<String, Response> {
    let value = headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing_scope_header"))?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_scope_header"));
    }
    Ok(value.to_owned())
}

fn error(status: StatusCode, code: &'static str) -> Response {
    (status, axum::Json(ErrorBody { error: code })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn config(secret: Option<&str>) -> Config {
        Config {
            bind: "127.0.0.1:4318".parse().unwrap(),
            upstream_base: validate_upstream("http://127.0.0.1:4319/").unwrap(),
            internal_auth: secret.map(str::to_owned),
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_concurrent: 2,
        }
    }

    #[test]
    fn remote_plaintext_upstream_is_rejected() {
        assert!(validate_upstream("http://collector.example/v1/").is_err());
        assert!(validate_upstream("https://collector.example/v1/").is_ok());
    }

    #[test]
    fn credential_bearing_upstream_is_rejected() {
        assert!(validate_upstream("https://user:pass@collector.example/").is_err());
        assert!(validate_upstream("https://collector.example/?token=secret").is_err());
    }

    #[test]
    fn authenticated_scope_is_header_authoritative() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ores-internal-auth", HeaderValue::from_static("secret"));
        headers.insert("x-ores-tenant-id", HeaderValue::from_static("tenant_1"));
        headers.insert("x-ores-workload-id", HeaderValue::from_static("api_1"));
        assert_eq!(
            authorize(&config(Some("secret")), &headers).unwrap(),
            ("tenant_1".into(), "api_1".into())
        );
    }

    #[test]
    fn wrong_auth_and_bad_identity_fail_closed() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ores-internal-auth", HeaderValue::from_static("wrong"));
        headers.insert("x-ores-tenant-id", HeaderValue::from_static("tenant_1"));
        headers.insert("x-ores-workload-id", HeaderValue::from_static("api_1"));
        assert!(authorize(&config(Some("secret")), &headers).is_err());

        headers.insert("x-ores-internal-auth", HeaderValue::from_static("secret"));
        headers.insert("x-ores-tenant-id", HeaderValue::from_static("tenant ü"));
        assert!(authorize(&config(Some("secret")), &headers).is_err());
    }
}
