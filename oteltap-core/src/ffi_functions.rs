use std::collections::BTreeMap;
use std::ffi::CStr;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Mutex, mpsc};
use std::time::Duration;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use std::os::raw::c_char;
use std::sync::atomic::{AtomicU64, Ordering};
use prost::Message;

use crate::OtelReceiver;
use crate::ffi_functions_flags::*;

// Atomic counter for generating unique handles for OtelReceivers
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

// Static map of OtelReceivers, so that we can return opaque handles (safety)
static OTEL_RECEIVERS: Mutex<BTreeMap<u64, Mutex<OtelReceiver>>> = Mutex::new(BTreeMap::new());

// Stores the channel receiver and its associated data buffer.
pub(crate) struct ReceiverAndBuf<T> {
    receiver: Receiver<T>,
    buf: Option<Vec<u8>>,
    print_thread: Option<std::thread::JoinHandle<()>>,
}

// Separate mutexes for traces, logs and metrics, so that we can poll them independently.
static TRACE_BUFS: Mutex<BTreeMap<u64, Mutex<ReceiverAndBuf<Span>>>> = Mutex::new(BTreeMap::new());
static LOG_BUFS: Mutex<BTreeMap<u64, Mutex<ReceiverAndBuf<LogRecord>>>> = Mutex::new(BTreeMap::new());
static METRIC_BUFS: Mutex<BTreeMap<u64, Mutex<ReceiverAndBuf<Metric>>>> = Mutex::new(BTreeMap::new());

// Starts OtelTap receiver on the specified port, expecting http/protobuf format, with optional re-emission endpoints for traces, logs, and metrics.
// Returns opaque handle to the created receiver via out_handle, or an error code if failed.
#[unsafe(no_mangle)]
pub extern "C" fn oteltap_start_receiving_http_protobuf(
    port: u16,
    flags: u32,
    reemit_traces_to: *const c_char,
    reemit_logs_to: *const c_char,
    reemit_metrics_to: *const c_char,
    out_handle: *mut u64
) -> i32
{
    if out_handle.is_null() {
        return -3; // Invalid output pointer
    }

    let reemit_traces_to = match unsafe { c_style_string_to_rust_string(reemit_traces_to) } {
        Ok(v) => v,
        Err(code) => return code,
    };

    let reemit_logs_to = match unsafe { c_style_string_to_rust_string(reemit_logs_to) } {
        Ok(v) => v,
        Err(code) => return code,
    };

    let reemit_metrics_to = match unsafe { c_style_string_to_rust_string(reemit_metrics_to) } {
        Ok(v) => v,
        Err(code) => return code,
    };

    // FFI channels
    let (trace_sender, trace_receiver) = mpsc::channel();
    let (log_sender, log_receiver) = mpsc::channel();
    let (metric_sender, metric_receiver) = mpsc::channel();

    let mut traces_senders = vec![trace_sender];
    let mut logs_senders = vec![log_sender];
    let mut metrics_senders = vec![metric_sender];

    // Shared opaque handle
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);

    // Printing traces as NDJSON if the corresponding flag is set
    let print_traces_thread = if flags & OTELTAP_PRINT_TRACES_AS_NDJSON != 0 {

        // Starting trace printer thread
        let (traces_printer_sender, trace_printer_receiver) = mpsc::channel();
        traces_senders.push(traces_printer_sender);

        Some(std::thread::spawn(move || print_receiver(trace_printer_receiver, |span| serde_json::to_string(&span))))

    } else {
        None
    };

    // Printing logs as NDJSON if the corresponding flag is set
    let print_logs_thread = if flags & OTELTAP_PRINT_LOGS_AS_NDJSON != 0 {

        // Starting log printer thread
        let (logs_printer_sender, log_printer_receiver) = mpsc::channel();
        logs_senders.push(logs_printer_sender);

        Some(std::thread::spawn(move || print_receiver(log_printer_receiver, |log| serde_json::to_string(&log))))

    } else {
        None
    };

    // Printing metrics as NDJSON if the corresponding flag is set
    let print_metrics_thread = if flags & OTELTAP_PRINT_METRICS_AS_NDJSON != 0 {

        // Starting metric printer thread
        let (metrics_printer_sender, metric_printer_receiver) = mpsc::channel();
        metrics_senders.push(metrics_printer_sender);

        Some(std::thread::spawn(move || print_receiver(metric_printer_receiver, |metric| serde_json::to_string(&metric))))

    } else {
        None
    };

    // Starting the OtelReceiver and storing it in the global map
    let otel_receiver = match OtelReceiver::start(
        port,
        traces_senders,
        logs_senders,
        metrics_senders,
        reemit_traces_to.as_deref(),
        reemit_logs_to.as_deref(),
        reemit_metrics_to.as_deref()
    ) {
        Ok(receiver) => receiver,
        Err(_code) => return -1, // Failed to start the receiver
    };
    OTEL_RECEIVERS.lock().unwrap().insert(handle, Mutex::new(otel_receiver));

    // Storing buffers, receivers, and printer threads in the global maps
    TRACE_BUFS.lock().unwrap().insert(handle, Mutex::new(ReceiverAndBuf { receiver: trace_receiver, buf: None, print_thread: print_traces_thread }));
    LOG_BUFS.lock().unwrap().insert(handle, Mutex::new(ReceiverAndBuf { receiver: log_receiver, buf: None, print_thread: print_logs_thread }));
    METRIC_BUFS.lock().unwrap().insert(handle, Mutex::new(ReceiverAndBuf { receiver: metric_receiver, buf: None, print_thread: print_metrics_thread }));

    unsafe { *out_handle = handle };

    0 // Successfully started the receiver
}

// Stops the OtelTap receiver associated with the given handle, cleaning up resources.
#[unsafe(no_mangle)]
pub extern "C" fn oteltap_stop_receiving(handle: u64) -> i32 {

    let mut receivers = OTEL_RECEIVERS.lock().unwrap();
    if receivers.remove(&handle).is_some() {
        
        // Receiver is stopped by now. Joining printer threads.

        let trace_buf = TRACE_BUFS.lock().unwrap().remove(&handle);
        if let Some(t) = trace_buf {
            if let Some(print_thread) = t.lock().unwrap().print_thread.take() {
                let _ = print_thread.join();
            }
        }

        let log_buf = LOG_BUFS.lock().unwrap().remove(&handle);
        if let Some(l) = log_buf {
            if let Some(print_thread) = l.lock().unwrap().print_thread.take() {
                let _ = print_thread.join();
            }
        }

        let metric_buf = METRIC_BUFS.lock().unwrap().remove(&handle);
        if let Some(m) = metric_buf {
            if let Some(print_thread) = m.lock().unwrap().print_thread.take() {
                let _ = print_thread.join();
            }
        }

        0 // Successfully stopped the receiver
    } else {
        -2 // Handle not found
    }
}

// Polls for a trace span with a specified timeout in milliseconds.
// If received, encodes it into a protobuf byte vector and returns a pointer to it and its length.
#[unsafe(no_mangle)]
pub extern "C" fn oteltap_poll_trace(
    handle: u64,
    timeout_ms: u64,
    out_buf: *mut *mut u8,
    out_len: *mut usize
) -> i32 {

    internal_poll(handle, timeout_ms, out_buf, out_len, &TRACE_BUFS)
}

// Polls for a log record with a specified timeout in milliseconds.
// If received, encodes it into a protobuf byte vector and returns a pointer to it and its length.
#[unsafe(no_mangle)]
pub extern "C" fn oteltap_poll_log(
    handle: u64,
    timeout_ms: u64,
    out_buf: *mut *mut u8,
    out_len: *mut usize
) -> i32 {

    internal_poll(handle, timeout_ms, out_buf, out_len, &LOG_BUFS)
}

// Polls for a metric with a specified timeout in milliseconds.
// If received, encodes it into a protobuf byte vector and returns a pointer to it and its length.
#[unsafe(no_mangle)]
pub extern "C" fn oteltap_poll_metric(
    handle: u64,
    timeout_ms: u64,
    out_buf: *mut *mut u8,
    out_len: *mut usize
) -> i32 {

    internal_poll(handle, timeout_ms, out_buf, out_len, &METRIC_BUFS)
}

// Converts a C-style null-terminated string to Rust String (which involves copying the data).
// Returns Ok(None) if the pointer is null, or Err(-3) if the string is not valid UTF-8.
unsafe fn c_style_string_to_rust_string(cs: *const c_char) -> Result<Option<String>, i32> {
    if cs.is_null() {
        return Ok(None);
    }
    match unsafe { CStr::from_ptr(cs).to_str() } {
        Ok(s) => Ok(Some(s.to_string())),
        Err(_) => Err(-3),
    }
}

fn internal_poll<T: Message>(
    handle: u64,
    timeout_ms: u64,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
    bufs: &Mutex<BTreeMap<u64, Mutex<ReceiverAndBuf<T>>>>
) -> i32 {

    if out_buf.is_null() || out_len.is_null() {
        return -3; // Invalid output pointers
    }

    let bufs = bufs.lock().unwrap();
    
    let mut buf = match bufs.get(&handle) {
        Some(r) => r.lock().unwrap(),
        None => return -2, // Handle not found
    };

    unsafe {
        *out_len = 0;
        *out_buf = std::ptr::null_mut();
    }
    
    match buf.receiver.recv_timeout(Duration::from_millis(timeout_ms)) {
        Ok(v) => {

            let mut data = v.encode_to_vec();

            unsafe {
                *out_len = data.len();
                *out_buf = data.as_mut_ptr();
            }

            // To be dropped when we're called again.
            buf.buf = Some(data);
        },
        Err(RecvTimeoutError::Timeout) => {
        }, // Timeout occurred
        Err(RecvTimeoutError::Disconnected) => {
        } // Channel disconnected
    }

    0 // Successfully returned data or indicated no data available
}


fn print_receiver<T: Message, F>(receiver: Receiver<T>, serialize_fn: F)
where
    F: Fn(&T) -> Result<String, serde_json::Error>,
{
    // Continuously receive spans from the channel and print them as NDJSON
    for span in receiver {
        match serialize_fn(&span) {
            Ok(json) => println!("{json}"),
            Err(e) => eprintln!("Failed to serialize span to JSON: {}", e),
        }
    }
}
