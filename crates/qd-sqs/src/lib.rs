//! AWS SQS-compatible HTTP frontend for the qd engine.
//!
//! Serves both SQS wire protocols on any path, so clients can use either the service endpoint
//! (`http://host:port/`) or a queue URL (`http://host:port/<account>/<queue>`) as the target.

pub mod auth;
pub mod handlers;
pub mod wire;

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use qd_engine::names::QueueRef;
use qd_engine::{Engine, ErrorCode, SqsError};
use serde_json::Value;

pub use auth::{AuthMode, Authenticator, Credential};

const JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.0";
const TARGET_PREFIX: &str = "AmazonSQS.";

#[derive(Debug, Clone)]
pub struct SqsConfig {
    /// Base URL advertised in queue URLs. When unset it is derived from the request's
    /// `X-Forwarded-Proto`/`X-Forwarded-Host`/`Host` headers.
    pub public_url: Option<String>,
    /// Allow addressing queues of other accounts (there is no IAM policy evaluation).
    pub allow_cross_account: bool,
    /// Maximum accepted request body size.
    pub max_body_bytes: usize,
}

impl Default for SqsConfig {
    fn default() -> Self {
        Self { public_url: None, allow_cross_account: false, max_body_bytes: 8 * 1024 * 1024 }
    }
}

pub struct SqsState {
    pub engine: Engine,
    pub auth: Authenticator,
    pub config: SqsConfig,
}

/// Builds the SQS API router.
pub fn router(state: Arc<SqsState>) -> Router {
    let limit = state.config.max_body_bytes;
    Router::new().fallback(handle).layer(DefaultBodyLimit::max(limit)).with_state(state)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Json,
    Query,
}

impl Protocol {
    fn label(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Query => "query",
        }
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn base_url(config: &SqsConfig, headers: &HeaderMap, uri: &Uri) -> String {
    if let Some(url) = &config.public_url {
        return url.trim_end_matches('/').to_string();
    }
    let scheme =
        header(headers, "x-forwarded-proto").and_then(|s| s.split(',').next()).map(str::trim).unwrap_or("http");
    let host = header(headers, "x-forwarded-host")
        .and_then(|s| s.split(',').next())
        .map(str::trim)
        .or_else(|| header(headers, "host"))
        .or_else(|| uri.authority().map(|a| a.as_str()))
        .unwrap_or("localhost");
    format!("{scheme}://{host}")
}

fn form_pairs(raw: &[u8]) -> Vec<(String, String)> {
    form_urlencoded::parse(raw).map(|(k, v)| (k.into_owned(), v.into_owned())).collect()
}

struct Parsed {
    protocol: Protocol,
    action: String,
    input: Value,
}

fn parse_request(method: &Method, uri: &Uri, headers: &HeaderMap, body: &[u8]) -> Result<Parsed, (Protocol, SqsError)> {
    if let Some(target) = header(headers, "x-amz-target") {
        let protocol = Protocol::Json;
        let action = target
            .strip_prefix(TARGET_PREFIX)
            .ok_or_else(|| (protocol, SqsError::new(ErrorCode::InvalidAction, format!("Unknown target {target}."))))?;
        let input = if body.iter().all(u8::is_ascii_whitespace) {
            Value::Object(Default::default())
        } else {
            serde_json::from_slice(body)
                .map_err(|e| (protocol, SqsError::invalid_parameter(format!("Request body is not valid JSON: {e}"))))?
        };
        return Ok(Parsed { protocol, action: action.to_string(), input });
    }

    let protocol = Protocol::Query;
    let mut params = uri.query().map(|q| form_pairs(q.as_bytes())).unwrap_or_default();
    let is_form = header(headers, "content-type")
        .map(|ct| ct.starts_with("application/x-www-form-urlencoded"))
        .unwrap_or(*method == Method::POST);
    if *method == Method::POST && is_form {
        params.extend(form_pairs(body));
    }
    let action = params
        .iter()
        .find(|(k, _)| k == "Action")
        .map(|(_, v)| v.clone())
        .ok_or_else(|| (protocol, SqsError::new(ErrorCode::InvalidAction, "Missing Action parameter.")))?;
    let input = wire::query_to_json(&action, &params);
    Ok(Parsed { protocol, action, input })
}

async fn handle(
    State(state): State<Arc<SqsState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let request_id = uuid::Uuid::new_v4().to_string();

    let parsed = match parse_request(&method, &uri, &headers, &body) {
        Ok(p) => p,
        Err((protocol, err)) => return error_response(protocol, &err, &request_id),
    };
    let protocol = parsed.protocol;

    let parts = auth::RequestParts {
        method: method.as_str(),
        raw_path: uri.path(),
        raw_query: uri.query(),
        headers: &headers,
        body: &body,
    };
    let caller = match state.auth.authenticate(&parts) {
        Ok(c) => c,
        Err(err) => {
            tracing::info!(action = %parsed.action, code = err.code.query_code(), "authentication failed");
            record(&parsed.action, protocol, err.code.query_code(), started);
            return error_response(protocol, &err, &request_id);
        }
    };

    let base = base_url(&state.config, &headers, &uri);
    let ctx = handlers::Ctx {
        engine: &state.engine,
        caller: &caller,
        base_url: &base,
        path_queue: QueueRef::from_path(uri.path()),
        allow_cross_account: state.config.allow_cross_account,
    };
    let result = handlers::dispatch(&ctx, &parsed.action, parsed.input).await;
    match result {
        Ok(value) => {
            record(&parsed.action, protocol, "OK", started);
            tracing::debug!(action = %parsed.action, elapsed_ms = started.elapsed().as_millis() as u64, "request served");
            success_response(protocol, &parsed.action, &value, &request_id)
        }
        Err(err) => {
            record(&parsed.action, protocol, err.code.query_code(), started);
            if err.code.sender_fault() {
                tracing::debug!(action = %parsed.action, code = err.code.query_code(), message = %err.message, "request rejected");
            } else {
                tracing::error!(action = %parsed.action, code = err.code.query_code(), message = %err.message, "request failed");
            }
            error_response(protocol, &err, &request_id)
        }
    }
}

fn record(action: &str, protocol: Protocol, code: &str, started: Instant) {
    let action = if handlers::ACTIONS.contains(&action) { action.to_string() } else { "Unknown".to_string() };
    metrics::counter!("qd_requests_total", "action" => action.clone(), "protocol" => protocol.label(), "code" => code.to_string())
        .increment(1);
    metrics::histogram!("qd_request_duration_seconds", "action" => action, "protocol" => protocol.label())
        .record(started.elapsed().as_secs_f64());
}

fn with_request_id(mut resp: Response, request_id: &str) -> Response {
    if let Ok(v) = HeaderValue::from_str(request_id) {
        resp.headers_mut().insert("x-amzn-requestid", v);
    }
    resp
}

fn success_response(protocol: Protocol, action: &str, value: &Value, request_id: &str) -> Response {
    let resp = match protocol {
        Protocol::Json => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, JSON_CONTENT_TYPE)],
            Body::from(serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec())),
        )
            .into_response(),
        Protocol::Query => {
            (StatusCode::OK, [(header::CONTENT_TYPE, "text/xml")], wire::json_to_xml(action, value, request_id))
                .into_response()
        }
    };
    with_request_id(resp, request_id)
}

fn error_response(protocol: Protocol, err: &SqsError, request_id: &str) -> Response {
    let status = StatusCode::from_u16(err.code.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
    let fault = if err.code.sender_fault() { "Sender" } else { "Receiver" };
    let resp = match protocol {
        Protocol::Json => {
            let body = serde_json::json!({
                "__type": format!("com.amazonaws.sqs#{}", err.code.json_type()),
                "message": err.message,
            });
            let mut resp = (status, [(header::CONTENT_TYPE, JSON_CONTENT_TYPE)], body.to_string()).into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("{};{fault}", err.code.query_code())) {
                resp.headers_mut().insert("x-amzn-query-error", v);
            }
            resp
        }
        Protocol::Query => (
            status,
            [(header::CONTENT_TYPE, "text/xml")],
            wire::error_xml(err.code.query_code(), &err.message, err.code.sender_fault(), request_id),
        )
            .into_response(),
    };
    with_request_id(resp, request_id)
}
