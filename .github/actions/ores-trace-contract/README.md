# ORES static trace and routine identifiers

This action validates static, grep-stable observability identifiers used alongside runtime OpenTelemetry context.

## Contract

- New static IDs use a 21-character nanoid suffix.
- Compatibility accepts 12..=64 characters from `[A-Za-z0-9_-]`.
- Routine IDs are function-scoped and declared immediately inside the function/method body.
- Trace IDs are call-site-scoped and remain hardcoded inline on the individual logging statement.
- Never hoist a static trace ID into a variable or constant.
- Runtime/distributed OpenTelemetry trace/span IDs remain dynamic and are separate from these static source-location markers.

## Rust pattern

```rust
pub fn reconcile() -> Result<(), Error> {
    const ROUTINE_ID: &str = "ores-routine-V1StGXR8_Z5jdHi6B-myT";

    logger
        .info(vec![json!("reconcile started")])
        .add_trace("ores-trace-cW7Kq3_nR9fX2mP8AzL4H", false)
        .add_routine_id(ROUTINE_ID)
        .send()?;

    Ok(())
}
```

Each additional log event gets its own static inline `ores-trace-*` literal but reuses the function's `ROUTINE_ID`.

## JavaScript / TypeScript pattern

```ts
function reconcile() {
  const routineId = 'ores-routine-V1StGXR8_Z5jdHi6B-myT';

  log.info('reconcile started')
    .addTraceId('ores-trace-cW7Kq3_nR9fX2mP8AzL4H')
    .addRoutineId(routineId)
    .send();
}
```
