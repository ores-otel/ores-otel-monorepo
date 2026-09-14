use std::time::Duration;

use reqwest::{Client, Response, StatusCode, Url};
use thiserror::Error;

pub const MAX_RETRY_BACKOFF_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardPolicy {
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ForwardError {
    #[error("upstream request timed out")]
    Timeout,
    #[error("upstream request failed")]
    Network,
}

pub async fn post_with_retry(
    client: &Client,
    upstream: Url,
    content_type: &str,
    tenant_id: &str,
    workload_id: &str,
    body: Vec<u8>,
    policy: ForwardPolicy,
) -> Result<Response, ForwardError> {
    for attempt in 1..=policy.max_attempts {
        let result = client
            .post(upstream.clone())
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .header("x-ores-tenant-id", tenant_id)
            .header("x-ores-workload-id", workload_id)
            .body(body.clone())
            .send()
            .await;

        match result {
            Ok(response)
                if attempt < policy.max_attempts && retryable_status(response.status()) =>
            {
                tokio::time::sleep(retry_delay(policy.retry_backoff_ms, attempt)).await;
            }
            Ok(response) => return Ok(response),
            Err(error) if attempt < policy.max_attempts => {
                tokio::time::sleep(retry_delay(policy.retry_backoff_ms, attempt)).await;
                if error.is_builder() || error.is_redirect() {
                    return Err(ForwardError::Network);
                }
            }
            Err(error) if error.is_timeout() => return Err(ForwardError::Timeout),
            Err(_) => return Err(ForwardError::Network),
        }
    }

    Err(ForwardError::Network)
}

pub fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

pub fn retry_delay(base_ms: u64, attempt: u8) -> Duration {
    Duration::from_millis(
        base_ms
            .saturating_mul(u64::from(attempt))
            .min(MAX_RETRY_BACKOFF_MS),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_http_statuses_are_retried() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert!(retryable_status(status), "did not retry {status}");
        }
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            assert!(!retryable_status(status), "retried terminal {status}");
        }
    }

    #[test]
    fn retry_backoff_is_bounded() {
        assert_eq!(retry_delay(100, 1), Duration::from_millis(100));
        assert_eq!(retry_delay(100, 3), Duration::from_millis(300));
        assert_eq!(retry_delay(10_000, 5), Duration::from_millis(5_000));
    }
}
