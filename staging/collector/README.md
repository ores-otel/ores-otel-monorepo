# OTLP/HTTP collector — UNRELEASED staging source

This is a **temporary Rust source bootstrap** attached to an app-of-apps repository because the [dedicated collector repository](https://github.com/ores-otel/ores-otel-monorepo/issues/3) does not yet exist. **Do not deploy this as the ORES OTEL production collector or treat the monorepo as its permanent source home.** Extract reviewed code and tests into `ores-otel/ores-otel-collector.rs` before release. [Architecture ADR-0001](https://github.com/ores-otel/ores-otel-docs/blob/main/docs/architecture/0001-otel-collector-plane.md) is authoritative.

## Supported narrow behavior

- **Real HTTP listener** with `POST /v1/logs`, `POST /v1/traces`, `POST /v1/metrics`, `GET /healthz`, `GET /readyz`.
- Receives **OTLP/HTTP JSON only** (not protobuf or gRPC). It performs minimal shape/size admission, not complete OTLP semantic-convention or protocol validation.
- Uses an **ingest-only, static test bearer** to derive one configured tenant; client-provided `x-ores-tenant-id` is rejected. For production: replace the static bearer with authenticated, rotating workload/tenant identity and per-tenant quotas.
- Body maximum 64 KiB, shallow structure limits, at most 256 signal records, at most eight in-flight exports, 3s exporter timeout and no network redirect. Rejects known secret-bearing attribute names before forwarding. This is **rejection**, not comprehensive policy-based secret redaction.
- Forwards to exactly one **configured approved HTTPS endpoint** (or test loopback HTTP) with a separate export bearer and an authenticated `x-ores-tenant-id` header; no client-supplied destination accepted.
- Only responds HTTP 200 after the downstream exporter responds HTTP 200; otherwise 503, without claiming durable acceptance. No queue or retry: callers must own any retry and its idempotency contract.

## Execution

Requires Rust and Cargo; use `cargo test --manifest-path staging/collector/Cargo.toml --all-targets` to execute real-loopback-TCP integration tests.

Explicit local-only smoke configuration:

```sh
export ORES_OTEL_STAGING_BIND='127.0.0.1:4318'
export ORES_OTEL_STAGING_TENANT='synthetic_test_tenant'
export ORES_OTEL_STAGING_BEARER='<scoped-synthetic-ingest-test-token-24+ chars>'
export ORES_OTEL_STAGING_EXPORTER='http://127.0.0.1:4319'
export ORES_OTEL_STAGING_EXPORTER_BEARER='<independent-scoped-synthetic-export-test-token-24+ chars>'
cargo run --manifest-path staging/collector/Cargo.toml
```

The exporter must be started separately on loopback. The production deployment must use workload identity and secret files/service-bound credentials; **do not commit tokens or expose the prototype to the Internet**.

## Explicitly NOT IMPLEMENTED or NOT CERTIFIED

- OTLP/gRPC, OTLP protobuf, gzip streaming/decompression/size budgets; standard OTLP partial-success negotiation; full schema/semantic-convention validation.
- Multi-tenant identity control plane, credential rotation, mTLS, workload binding, quota/rate budgets, exporter fan-out, per-tenant durability or long-term storage.
- Complete redaction, retention, query APIs, indexes, analytics, dashboards, automatic instrumentation or Cloudflare Workers native telemetry integration.
- Retry, persistent queue, shutdown drain, crash recovery, at-least-once/exactly-once delivery, exporter deduplication, secure metrics/health deployment.
- AttoLang compiler-produced guest execution and Java/LLVM/Forge/Wasm/.NET parity.

CI success here is **NATIVE_HOST_STAGING_ONLY**. For release, create the dedicated repository, commit and review a deterministic Cargo.lock, run negative controls with two real tenants, execute standard OTLP interoperability fixtures and independent export/storage read-back, and capture exact source SHA, stepful jobs and deployment digests.

See [draft observability parity ADR](https://github.com/ores-otel/ores-otel-docs/pull/8) and [e2e OTLP admission gate](https://github.com/ores-otel/ores-otel-e2e/pull/8).
