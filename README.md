# OtelTap

**OtelTap** is a Rust-based, embeddable OTLP (OpenTelemetry Protocol) receiver designed to be driven from **any language via FFI**. It's meant to be dropped straight into your end-to-end / integration test suites so tests can **await and assert on real telemetry** (traces, logs, metrics) emitted by the system under test — instead of guessing timing or mocking the OTel SDK.

## Why

When testing a service that emits OpenTelemetry data, you usually want to:

1. Spin up a lightweight OTLP endpoint the service can point at.
2. Wait for a specific span/log/metric to show up, then assert on its contents.
3. Still let the data flow through to your normal observability stack, so you don't lose visibility while testing.
4. See what's coming through, right in the test/CI console, without attaching a separate viewer.

OtelTap does all of this in a single embeddable component:

- **Receives** OTLP/HTTP (protobuf) traces, logs, and metrics on a local port.
- **Exposes them via a C-compatible FFI**, so tests written in Python, Node.js, Java, .NET, Go, etc. can poll for individual spans/logs/metrics by handle — no Rust required on the caller's side.
- **Prints incoming telemetry to the console as NDJSON**, one compact JSON object per line — handy for humans, CI logs, and AI coding agents ("copilots") tailing test output.
- **Re-emits** everything it receives to another OTLP/HTTP endpoint, so your usual pipelines/visualizers keep working unmodified while the tap is attached.

## How it works

Each receiver listens on `127.0.0.1:<port>` for standard OTLP/HTTP protobuf requests (`/v1/traces`, `/v1/logs`, `/v1/metrics`), decodes them, and fans each item out three ways: into a channel polled via FFI, to a background NDJSON printer thread, and (optionally, per signal type) to another OTLP/HTTP endpoint for re-emission. An opaque `u64` handle identifies each running receiver, so one process can host multiple independent taps.

> **Note:** only the **OTLP/HTTP protobuf** transport is supported (`Content-Type: application/x-protobuf`) — this is the recommended encoding for OTLP over HTTP (the OTLP spec treats HTTP/JSON as debug-only). OTLP/gRPC is not implemented.

## FFI surface

All exported functions use the C ABI (`extern "C"`, `#[no_mangle]`) and return an `i32` status code (`0` = success, negative = error).

| Function | Purpose |
|---|---|
| `oteltap_start_receiving_http_protobuf(port, reemit_traces_to, reemit_logs_to, reemit_metrics_to, out_handle)` | Starts a receiver on `port`. Re-emit arguments are optional (nullable) C strings — a URL per signal type, or `NULL` to skip re-emission for that signal. Writes the new handle to `out_handle`. |
| `oteltap_stop_receiving(handle)` | Stops the receiver for `handle`, closes the listening socket, and waits for any in-flight printing to finish. |
| `oteltap_poll_trace(handle, timeout_ms, out_buf, out_len)` | Blocks up to `timeout_ms` for the next received `Span`, returned as a protobuf-encoded buffer (`out_buf`/`out_len`). |
| `oteltap_poll_log(handle, timeout_ms, out_buf, out_len)` | Same as above, for `LogRecord`. |
| `oteltap_poll_metric(handle, timeout_ms, out_buf, out_len)` | Same as above, for `Metric`. |

Polled data is returned as raw OTLP protobuf bytes (the same wire format `opentelemetry-proto` defines), so any caller with standard OTLP protobuf bindings for their language can decode it directly.

> This crate builds as both an `rlib` (for use from Rust) and a `cdylib` (for consumption from other languages through FFI) — see `Cargo.toml`.

This repo provides the core Rust engine and its raw FFI surface only. Idiomatic, language-specific wrappers (Python, Node.js, .NET, etc.) for consuming it in tests are published as separate repos.

## Prerequisites & building

- [Rust toolchain](https://rustup.rs/) — edition 2024, so **Rust 1.85 or newer** is required (`rustup update` if unsure).

Build from `oteltap-core/`:

```sh
cargo build --release
```

This produces both artifacts declared in `Cargo.toml`'s `crate-type`:

- an `rlib`, for consuming `oteltap-core` directly from other Rust code;
- a `cdylib` (`oteltap_core.dll` / `liboteltap_core.so` / `liboteltap_core.dylib`, depending on OS) under `target/release/`, which is the shared library other languages load via FFI to call the `oteltap_*` functions.

## Project layout

```
oteltap-core/
├── src/
│   ├── lib.rs             # Crate root, re-exports OtelReceiver
│   ├── otel_receiver.rs   # Embedded hyper/tokio OTLP/HTTP server
│   ├── otel_reemitter.rs  # Forwards received OTLP payloads to another endpoint
│   └── ffi_functions.rs   # extern "C" FFI surface (start/stop/poll)
└── Cargo.toml
```

## Status

This is an early-stage project; the FFI surface and error codes may still change.

## Contributing

Is very much welcomed.