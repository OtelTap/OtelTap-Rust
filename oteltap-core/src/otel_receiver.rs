use std::io::{Error, ErrorKind};
use std::sync::{Arc};
use std::sync::mpsc::Sender;
use std::{convert::Infallible, net::SocketAddr};

use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use prost::Message;
use tokio::net::TcpListener;
use tokio::runtime::{Handle, Runtime};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest,
    ExportTraceServiceResponse
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest,
    ExportLogsServiceResponse
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse
};

use crate::otel_reemitter::OtelReemitter;

// Embedded OTLP receiver. Listens for http/protobuf requests on a given port and forwards telemetry data to the provided senders.
// Optionally, re-emits the data to another endpoint.
pub struct OtelReceiver {

    // Our own tokio  runtime
    #[allow(dead_code)]
    runtime: Runtime,
}

impl OtelReceiver {

    pub fn start(
        port: u16,
        traces_senders: Vec<Sender<Span>>,
        logs_senders: Vec<Sender<LogRecord>>,
        metrics_senders: Vec<Sender<Metric>>,
        reemit_traces_to: Option<&str>,
        reemit_logs_to: Option<&str>,
        reemit_metrics_to: Option<&str>
    ) -> std::io::Result<Self> {

        let trace_reemitter = reemit_traces_to.map(OtelReemitter::new).transpose()?;
        let logs_reemitter = reemit_logs_to.map(OtelReemitter::new).transpose()?;
        let metrics_reemitter = reemit_metrics_to.map(OtelReemitter::new).transpose()?;

        if Handle::try_current().is_ok() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "OtelReceiver::start() should be called from a non-async context",
            ));
        }

        let runtime = Runtime::new()?;

        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        // Synchronously try to start listening, return an error if it fails (e.g., port already in use)
        let listener = runtime.block_on(TcpListener::bind(addr))?;

        // Serving requests in the thread pool
        runtime.spawn(Self::serve(
            listener,
            Arc::from(traces_senders),
            Arc::from(logs_senders),
            Arc::from(metrics_senders),
            trace_reemitter.map(Arc::new),
            logs_reemitter.map(Arc::new),
            metrics_reemitter.map(Arc::new),
        ));

        Ok(Self { runtime })
    }

    // Serve incoming connections and spawn a new task for each connection
    async fn serve(
        listener: TcpListener,
        traces_senders: Arc<[Sender<Span>]>,
        logs_senders: Arc<[Sender<LogRecord>]>,
        metrics_senders: Arc<[Sender<Metric>]>,
        trace_reemitter: Option<Arc<OtelReemitter>>,
        logs_reemitter: Option<Arc<OtelReemitter>>,
        metrics_reemitter: Option<Arc<OtelReemitter>>,
    ) {

        loop {

            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                },
            };
            let io = TokioIo::new(stream);
            let traces_senders = traces_senders.clone();
            let logs_senders = logs_senders.clone();
            let metrics_senders = metrics_senders.clone();
            let trace_reemitter = trace_reemitter.clone();
            let logs_reemitter = logs_reemitter.clone();
            let metrics_reemitter = metrics_reemitter.clone();

            tokio::task::spawn(async move {

                if let Err(err) = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(
                        io,
                        service_fn(|r| Self::handle(
                            r,
                            traces_senders.clone(),
                            logs_senders.clone(),
                            metrics_senders.clone(),
                            trace_reemitter.clone(),
                            logs_reemitter.clone(),
                            metrics_reemitter.clone())
                        )
                    )
                    .await
                {
                    eprintln!("Connection error: {}", err);
                }
            });
        }
    }

    // Handle incoming HTTP requests and route them to the appropriate handler based on the request path
    async fn handle(
        req: Request<Incoming>,
        traces_senders: Arc<[Sender<Span>]>,
        logs_senders: Arc<[Sender<LogRecord>]>,
        metrics_senders: Arc<[Sender<Metric>]>,
        trace_reemitter: Option<Arc<OtelReemitter>>,
        logs_reemitter: Option<Arc<OtelReemitter>>,
        metrics_reemitter: Option<Arc<OtelReemitter>>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {

        match(req.method(), req.uri().path()) {
            (&hyper::Method::POST, "/v1/traces") => 
                Self::handle_traces(req, traces_senders.clone(), trace_reemitter.clone()).await,
            (&hyper::Method::POST, "/v1/logs") => 
                Self::handle_logs(req, logs_senders.clone(), logs_reemitter.clone()).await,
            (&hyper::Method::POST, "/v1/metrics") => 
                Self::handle_metrics(req, metrics_senders.clone(), metrics_reemitter.clone()).await,
            _ =>
                Ok(Self::string_response(StatusCode::NOT_FOUND, "Not Found"))
        }
    }

    // Handle incoming trace requests, decode the request body, and send the spans to the provided senders
    async fn handle_traces(
        req: Request<Incoming>,
        senders: Arc<[Sender<Span>]>,
        reemitter: Option<Arc<OtelReemitter>>
    ) -> Result<Response<Full<Bytes>>, Infallible> {

        let request_bytes = match req.collect().await {
            Ok(c) => c.to_bytes(),
            Err(_) => {
                return Ok(Self::string_response(StatusCode::BAD_REQUEST, "Failed to read request body"));
            }
        };

        let trace_request = match ExportTraceServiceRequest::decode(request_bytes.clone()) {
            Ok(req) => req,
            Err(_) => {
                return Ok(Self::string_response(StatusCode::BAD_REQUEST, "Failed to decode request body"));
            }
        };

        // Flattening the nested structure of resource spans and scope spans to get all spans
        let traces: Vec<_> = trace_request
            .resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|sc| &sc.spans)
            .collect();

        // Pushing the batch to all senders
        for span in traces {
            for sender in senders.iter() {
                let _ = sender.send(span.clone());
            }
        }

        // If a reemitter is provided, spawn a task to re-emit the request bytes. Intentionally non-blocking.
        if let Some(reemitter) = reemitter {
            tokio::spawn(async move {
                if let Err(e) = reemitter.reemit(request_bytes).await {
                    eprintln!("Failed to re-emit traces: {}", e);
                }
            });
        }

        Ok(Self::message_response(&ExportTraceServiceResponse::default()))
    }

    // Handle incoming log requests, decode the request body, and send the log records to the provided senders
    async fn handle_logs(
        req: Request<Incoming>,
        senders: Arc<[Sender<LogRecord>]>,
        reemitter: Option<Arc<OtelReemitter>>
    ) -> Result<Response<Full<Bytes>>, Infallible> {

        let request_bytes = match req.collect().await {
            Ok(c) => c.to_bytes(),
            Err(_) => {
                return Ok(Self::string_response(StatusCode::BAD_REQUEST, "Failed to read request body"));
            }
        };

        let log_request = match ExportLogsServiceRequest::decode(request_bytes.clone()) {
            Ok(req) => req,
            Err(_) => {
                return Ok(Self::string_response(StatusCode::BAD_REQUEST, "Failed to decode request body"));
            }
        };

        let logs: Vec<_> = log_request
            .resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .flat_map(|sl| &sl.log_records)
            .collect();

        // Pushing the batch to all senders
        for log in logs {
            for sender in senders.iter() {
                let _ = sender.send(log.clone());
            }
        }

        // If a reemitter is provided, spawn a task to re-emit the request bytes. Intentionally non-blocking.
        if let Some(reemitter) = reemitter {
            tokio::spawn(async move {
                if let Err(e) = reemitter.reemit(request_bytes).await {
                    eprintln!("Failed to re-emit logs: {}", e);
                }
            });
        }

        Ok(Self::message_response(&ExportLogsServiceResponse::default()))
    }

    // Handle incoming metric requests, decode the request body, and send the metrics to the provided senders
    async fn handle_metrics(
        req: Request<Incoming>,
        senders: Arc<[Sender<Metric>]>,
        reemitter: Option<Arc<OtelReemitter>>
    ) -> Result<Response<Full<Bytes>>, Infallible> {

        let request_bytes = match req.collect().await {
            Ok(c) => c.to_bytes(),
            Err(_) => {
                return Ok(Self::string_response(StatusCode::BAD_REQUEST, "Failed to read request body"));
            }
        };

        let metric_request = match ExportMetricsServiceRequest::decode(request_bytes.clone()) {
            Ok(req) => req,
            Err(_) => {
                return Ok(Self::string_response(StatusCode::BAD_REQUEST, "Failed to decode request body"));
            }
        };

        let metrics: Vec<_> = metric_request
            .resource_metrics
            .iter()
            .flat_map(|rm| &rm.scope_metrics)
            .flat_map(|sc| &sc.metrics)
            .collect();

        // Pushing the batch to all senders
        for metric in metrics {
            for sender in senders.iter() {
                let _ = sender.send(metric.clone());
            }
        }

        // If a reemitter is provided, spawn a task to re-emit the request bytes. Intentionally non-blocking.
        if let Some(reemitter) = reemitter {
            tokio::spawn(async move {
                if let Err(e) = reemitter.reemit(request_bytes).await {
                    eprintln!("Failed to re-emit metrics: {}", e);
                }
            });
        }

        Ok(Self::message_response(&ExportMetricsServiceResponse::default()))
    }

    // Helper function to create a response with a protobuf message body
    fn message_response<T: Message>(body: &T) -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/x-protobuf")
            .body(Full::new(Bytes::from(body.encode_to_vec())))
            .unwrap()
    }

    // Helper function to create a response with a string body
    fn string_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap()
    }
}

