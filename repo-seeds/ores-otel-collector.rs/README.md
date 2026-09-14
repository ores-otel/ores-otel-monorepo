# ores-otel-collector.rs repository seed

Initial repository seed for the dedicated ORES OTEL ingest/data plane tracked by `ores-otel-monorepo#3` and `ores-otel-interfaces#9`.

This is **not** a dashboard/API microservice. It owns OTLP/HTTP receiving, authenticated tenant/workload admission, bounded concurrency/body sizes and forwarding to an operator-approved upstream collector/exporter. `ores-otel-sidecar.rs` remains the colocated workload helper.

## Implemented

- Standard OTLP/HTTP paths: `/v1/traces`, `/v1/metrics`, `/v1/logs`.
- Lowercase authored `x-ores-*` headers; HTTP matching remains case-insensitive.
- Tenant/workload scope comes only from authenticated headers, never telemetry attributes.
- Constant-time internal-auth comparison when configured.
- Non-loopback bind fails closed without `ORES_OTEL_INTERNAL_AUTH`.
- Remote upstream must use HTTPS; plaintext is accepted only for loopback development.
- Credential-bearing upstream URLs (userinfo/query credentials) are rejected.
- Request bytes and concurrent in-flight requests are bounded; saturation returns `429`.
- Compression is rejected for now rather than accepting unbounded decompression.
- Internal auth is never forwarded upstream.
- Upstream failures return `502`; the collector never reports successful ingestion when forwarding failed.

## Configuration

Secrets remain environment-only. The seed accepts no CLI arguments, so it does not create a second option parser ahead of `flags-2-env`.

- `ORES_OTEL_BIND` (default `127.0.0.1:4318`)
- `ORES_OTEL_UPSTREAM_URL` (required)
- `ORES_OTEL_INTERNAL_AUTH` (required for non-loopback bind)
- `ORES_OTEL_MAX_BODY_BYTES` (default 4 MiB, hard ceiling 16 MiB)
- `ORES_OTEL_MAX_CONCURRENT` (default 64, hard ceiling 4096)

Next slices: OTLP/gRPC, bounded gzip decompression, per-tenant token buckets, exporter fan-out/redaction/enrichment, and the peer TypeSpec + JSON Schema control contracts.
