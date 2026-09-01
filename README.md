# OtelTap

**OtelTap** is a Rust-based, embeddable OTLP (OpenTelemetry Protocol) receiver designed to be driven from **any language via FFI**. It's meant to be dropped straight into your end-to-end / integration test suites so tests can **await and assert on real telemetry** (traces, logs, metrics) emitted by the system under test — instead of guessing timing or mocking the OTel SDK.

## Built for agentic AI development

OtelTap is designed with **AI coding agents ("copilots") as first-class users**, not just an afterthought. Telemetry is one of the richest sources of ground truth about what a system actually did — far more reliable than logs alone or guessing from source code — and OtelTap is built so an agent can close the loop on its own, without a human relaying data back and forth:

1. **Run** an integration/e2e test that exercises the system under test.
2. **See telemetry instantly** in the console as NDJSON, right in the same tool-call output the agent already reads — no separate viewer, no polling a dashboard, no screenshots.
3. **Infer** what actually happened — which spans fired, what attributes/status/errors show up, what got logged, what metrics moved — directly from that structured output.
4. **Apply code changes** based on that evidence, immediately, and re-run to verify — all within the same agentic loop, with re-emission ensuring humans watching the normal observability stack still see the exact same picture.

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

Each receiver listens on `127.0.0.1:<port>` by default (or `0.0.0.0:<port>`, all interfaces, if the `OTELTAP_LISTEN_ON_ALL_INTERFACES` flag is set) for standard OTLP/HTTP protobuf requests (`/v1/traces`, `/v1/logs`, `/v1/metrics`), decodes them, and fans each item out three ways: into a channel polled via FFI, to a background NDJSON printer thread, and (optionally, per signal type) to another OTLP/HTTP endpoint for re-emission. An opaque `u64` handle identifies each running receiver, so one process can host multiple independent taps.

> **Note:** only the **OTLP/HTTP protobuf** transport is supported (`Content-Type: application/x-protobuf`) — this is the recommended encoding for OTLP over HTTP (the OTLP spec treats HTTP/JSON as debug-only). OTLP/gRPC is not implemented.

## FFI surface

All exported functions use the C ABI (`extern "C"`, `#[no_mangle]`) and return an `i32` status code (`0` = success, negative = error).

| Function | Purpose |
|---|---|
| `oteltap_start_receiving_http_protobuf(port, flags, reemit_traces_to, reemit_logs_to, reemit_metrics_to, out_handle)` | Starts a receiver on `port`. `flags` is a bitmask (see below) controlling console printing. Re-emit arguments are optional (nullable) C strings — a URL per signal type, or `NULL` to skip re-emission for that signal. Writes the new handle to `out_handle`. |
| `oteltap_stop_receiving(handle)` | Stops the receiver for `handle`, closes the listening socket, and waits for any in-flight printing to finish. |
| `oteltap_poll_trace(handle, timeout_ms, out_buf, out_len)` | Blocks up to `timeout_ms` for the next received `Span`, returned as a protobuf-encoded buffer (`out_buf`/`out_len`). |
| `oteltap_poll_log(handle, timeout_ms, out_buf, out_len)` | Same as above, for `LogRecord`. |
| `oteltap_poll_metric(handle, timeout_ms, out_buf, out_len)` | Same as above, for `Metric`. |

Polled data is returned as raw OTLP protobuf bytes (the same wire format `opentelemetry-proto` defines), so any caller with standard OTLP protobuf bindings for their language can decode it directly.

### Flags

`flags` is a bitmask passed to `oteltap_start_receiving_http_protobuf`, controlling which signal types get printed to the console as NDJSON, as well as other receiver behavior. Combine values with bitwise OR; pass `0` for the default behavior (no console printing, listen on `127.0.0.1` only).

| Constant | Value | Effect |
|---|---|---|
| `OTELTAP_PRINT_TRACES_AS_NDJSON` | `1 << 0` | Print each received span as one NDJSON line. |
| `OTELTAP_PRINT_LOGS_AS_NDJSON` | `1 << 1` | Print each received log record as one NDJSON line. |
| `OTELTAP_PRINT_METRICS_AS_NDJSON` | `1 << 2` | Print each received metric as one NDJSON line. |
| `OTELTAP_LISTEN_ON_ALL_INTERFACES` | `1 << 8` | Bind the receiver to `0.0.0.0:<port>` (all network interfaces) instead of the default `127.0.0.1:<port>`. |

These constants are defined in `src/ffi_functions_flags.rs` and are reserved as a growing, extensible bitset — future flags (e.g. printing traces as human-readable Gantt charts) can be added as new bits without breaking existing callers or changing the function signature.

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