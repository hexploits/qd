//! Request authentication: AWS Signature Version 4 verification.
//!
//! Two modes are supported:
//!
//! * `none` — signatures are not checked. The access key id (if any) is used to pick the
//!   account from the configured credentials, falling back to the default account.
//! * `sigv4` — every request must carry a valid SigV4 signature (header or presigned query)
//!   made with one of the configured credentials. The credential's account becomes the caller.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::Duration;

use chrono::{NaiveDateTime, TimeZone, Utc};
use hmac::{Hmac, Mac};
use http::HeaderMap;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use qd_engine::{Caller, ErrorCode, SqsError};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// RFC 3986 unreserved characters are left alone; everything else is percent-encoded.
const SIGV4_ENCODE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    None,
    SigV4,
}

impl std::str::FromStr for AuthMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "off" | "disabled" => Ok(Self::None),
            "sigv4" | "aws4" => Ok(Self::SigV4),
            other => Err(format!("unknown auth mode {other:?} (expected none|sigv4)")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Credential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub account_id: String,
}

impl Credential {
    /// Parses `ACCESS_KEY_ID:SECRET_ACCESS_KEY[:ACCOUNT_ID]`.
    pub fn parse(spec: &str, default_account: &str) -> Result<Self, String> {
        let parts: Vec<&str> = spec.trim().splitn(3, ':').collect();
        match parts.as_slice() {
            [id, secret] if !id.is_empty() && !secret.is_empty() => Ok(Self {
                access_key_id: (*id).into(),
                secret_access_key: (*secret).into(),
                account_id: default_account.into(),
            }),
            [id, secret, account] if !id.is_empty() && !secret.is_empty() && !account.is_empty() => Ok(Self {
                access_key_id: (*id).into(),
                secret_access_key: (*secret).into(),
                account_id: (*account).into(),
            }),
            _ => Err("credential must look like ACCESS_KEY_ID:SECRET_ACCESS_KEY[:ACCOUNT_ID]".into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Authenticator {
    mode: AuthMode,
    default_account: String,
    credentials: HashMap<String, Credential>,
    max_skew: Duration,
}

/// The parts of an HTTP request needed for authentication.
pub struct RequestParts<'a> {
    pub method: &'a str,
    pub raw_path: &'a str,
    pub raw_query: Option<&'a str>,
    pub headers: &'a HeaderMap,
    pub body: &'a [u8],
}

struct SignatureInput {
    access_key_id: String,
    date: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
    amz_date: String,
    presigned: bool,
}

fn auth_error(code: ErrorCode, message: &str) -> SqsError {
    SqsError::new(code, message)
}

impl Authenticator {
    pub fn new(mode: AuthMode, default_account: impl Into<String>, credentials: Vec<Credential>) -> Self {
        Self {
            mode,
            default_account: default_account.into(),
            credentials: credentials.into_iter().map(|c| (c.access_key_id.clone(), c)).collect(),
            max_skew: Duration::from_secs(15 * 60),
        }
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    pub fn default_account(&self) -> &str {
        &self.default_account
    }

    pub fn authenticate(&self, req: &RequestParts<'_>) -> Result<Caller, SqsError> {
        let parsed = parse_signature(req);
        match self.mode {
            AuthMode::None => {
                let akid = parsed.ok().flatten().map(|s| s.access_key_id);
                let account = akid
                    .as_ref()
                    .and_then(|k| self.credentials.get(k))
                    .map(|c| c.account_id.clone())
                    .unwrap_or_else(|| self.default_account.clone());
                let sender = akid.unwrap_or_else(|| account.clone());
                Ok(Caller::new(account, sender))
            }
            AuthMode::SigV4 => {
                let sig = parsed?.ok_or_else(|| {
                    auth_error(ErrorCode::MissingAuthenticationToken, "Request is missing Authentication Token")
                })?;
                let cred = self.credentials.get(&sig.access_key_id).ok_or_else(|| {
                    auth_error(
                        ErrorCode::InvalidClientTokenId,
                        "The security token included in the request is invalid.",
                    )
                })?;
                self.verify(req, &sig, cred)?;
                Ok(Caller::new(cred.account_id.clone(), cred.access_key_id.clone()))
            }
        }
    }

    fn verify(&self, req: &RequestParts<'_>, sig: &SignatureInput, cred: &Credential) -> Result<(), SqsError> {
        if sig.service != "sqs" {
            return Err(auth_error(
                ErrorCode::SignatureDoesNotMatch,
                &format!("Credential should be scoped to correct service: 'sqs', not '{}'.", sig.service),
            ));
        }
        let ts = NaiveDateTime::parse_from_str(&sig.amz_date, "%Y%m%dT%H%M%SZ")
            .map(|t| Utc.from_utc_datetime(&t))
            .map_err(|_| auth_error(ErrorCode::IncompleteSignature, "Invalid X-Amz-Date."))?;
        let skew = (Utc::now() - ts).num_seconds().unsigned_abs();
        if !sig.presigned && skew > self.max_skew.as_secs() {
            return Err(auth_error(
                ErrorCode::RequestExpired,
                "Signature expired: the request time is too far from the server time.",
            ));
        }
        if sig.presigned {
            let expires: i64 = query_param(req.raw_query, "X-Amz-Expires").and_then(|v| v.parse().ok()).unwrap_or(900);
            if Utc::now().timestamp() > ts.timestamp() + expires {
                return Err(auth_error(ErrorCode::RequestExpired, "Request has expired."));
            }
        }
        if !sig.date.is_empty() && !sig.amz_date.starts_with(&sig.date) {
            return Err(auth_error(ErrorCode::SignatureDoesNotMatch, "Credential date does not match X-Amz-Date."));
        }

        let canonical = canonical_request(req, sig);
        let scope = format!("{}/{}/{}/aws4_request", sig.date, sig.region, sig.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            sig.amz_date,
            scope,
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        let key = signing_key(&cred.secret_access_key, &sig.date, &sig.region, &sig.service);
        let expected = hex::encode(hmac(&key, string_to_sign.as_bytes()));
        if !constant_time_eq(expected.as_bytes(), sig.signature.to_ascii_lowercase().as_bytes()) {
            tracing::debug!(canonical_request = %canonical, "signature mismatch");
            return Err(auth_error(
                ErrorCode::SignatureDoesNotMatch,
                "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.",
            ));
        }
        Ok(())
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn query_pairs(raw_query: Option<&str>) -> Vec<(String, String)> {
    raw_query
        .map(|q| {
            q.split('&')
                .filter(|p| !p.is_empty())
                .map(|pair| {
                    let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                    let decode = |s: &str| percent_decode_str(&s.replace('+', " ")).decode_utf8_lossy().into_owned();
                    (decode(k), decode(v))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn query_param(raw_query: Option<&str>, name: &str) -> Option<String> {
    query_pairs(raw_query).into_iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Extracts signature parameters from the Authorization header or a presigned query string.
/// `Ok(None)` means the request is unsigned.
fn parse_signature(req: &RequestParts<'_>) -> Result<Option<SignatureInput>, SqsError> {
    let incomplete = |m: &str| auth_error(ErrorCode::IncompleteSignature, m);
    if let Some(auth) = header_str(req.headers, "authorization") {
        let rest = auth
            .trim()
            .strip_prefix("AWS4-HMAC-SHA256")
            .ok_or_else(|| incomplete("Only AWS4-HMAC-SHA256 (Signature Version 4) authorization is supported."))?;
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;
        for part in rest.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("Credential=") {
                credential = Some(v.trim().to_string());
            } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
                signed_headers = Some(v.trim().to_string());
            } else if let Some(v) = part.strip_prefix("Signature=") {
                signature = Some(v.trim().to_string());
            }
        }
        let credential =
            credential.ok_or_else(|| incomplete("Authorization header requires 'Credential' parameter."))?;
        let signed_headers =
            signed_headers.ok_or_else(|| incomplete("Authorization header requires 'SignedHeaders' parameter."))?;
        let signature = signature.ok_or_else(|| incomplete("Authorization header requires 'Signature' parameter."))?;
        let amz_date = header_str(req.headers, "x-amz-date").map(str::to_string).ok_or_else(|| {
            incomplete("Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header.")
        })?;
        return build(credential, signed_headers, signature, amz_date, false).map(Some);
    }
    let pairs = query_pairs(req.raw_query);
    let get = |name: &str| pairs.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    if get("X-Amz-Algorithm").as_deref() == Some("AWS4-HMAC-SHA256") {
        let credential = get("X-Amz-Credential").ok_or_else(|| incomplete("Missing X-Amz-Credential."))?;
        let signed_headers = get("X-Amz-SignedHeaders").ok_or_else(|| incomplete("Missing X-Amz-SignedHeaders."))?;
        let signature = get("X-Amz-Signature").ok_or_else(|| incomplete("Missing X-Amz-Signature."))?;
        let amz_date = get("X-Amz-Date").ok_or_else(|| incomplete("Missing X-Amz-Date."))?;
        return build(credential, signed_headers, signature, amz_date, true).map(Some);
    }
    Ok(None)
}

fn build(
    credential: String,
    signed_headers: String,
    signature: String,
    amz_date: String,
    presigned: bool,
) -> Result<SignatureInput, SqsError> {
    let parts: Vec<&str> = credential.split('/').collect();
    let [akid, date, region, service, "aws4_request"] = parts.as_slice() else {
        return Err(auth_error(ErrorCode::IncompleteSignature, "Credential is malformed."));
    };
    Ok(SignatureInput {
        access_key_id: (*akid).to_string(),
        date: (*date).to_string(),
        region: (*region).to_string(),
        service: (*service).to_string(),
        signed_headers: signed_headers.split(';').map(|h| h.trim().to_ascii_lowercase()).collect(),
        signature,
        amz_date,
        presigned,
    })
}

/// Canonical URI for non-S3 services: normalise dot segments, then encode each segment again.
fn canonical_uri(raw_path: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();
    for seg in raw_path.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            s => segments.push(s),
        }
    }
    let mut out = String::new();
    for seg in &segments {
        out.push('/');
        out.push_str(&utf8_percent_encode(seg, SIGV4_ENCODE).to_string());
    }
    if out.is_empty() || (raw_path.ends_with('/') && !segments.is_empty()) {
        out.push('/');
    }
    out
}

fn canonical_query(raw_query: Option<&str>, presigned: bool) -> String {
    let mut pairs: Vec<(String, String)> = query_pairs(raw_query)
        .into_iter()
        .filter(|(k, _)| !(presigned && k == "X-Amz-Signature"))
        .map(|(k, v)| {
            (utf8_percent_encode(&k, SIGV4_ENCODE).to_string(), utf8_percent_encode(&v, SIGV4_ENCODE).to_string())
        })
        .collect();
    pairs.sort();
    pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

fn canonical_request(req: &RequestParts<'_>, sig: &SignatureInput) -> String {
    let mut headers = String::new();
    for name in &sig.signed_headers {
        let values: Vec<String> = req
            .headers
            .get_all(name.as_str())
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect();
        let _ = writeln!(headers, "{name}:{}", values.join(","));
    }
    let payload_hash = match header_str(req.headers, "x-amz-content-sha256") {
        Some(h) => h.to_string(),
        None if sig.presigned => "UNSIGNED-PAYLOAD".to_string(),
        None => hex::encode(Sha256::digest(req.body)),
    };
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        canonical_uri(req.raw_path),
        canonical_query(req.raw_query, sig.presigned),
        headers,
        sig.signed_headers.join(";"),
        payload_hash
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    /// Signs a request the way an SDK would, for round-trip testing.
    #[allow(clippy::too_many_arguments)]
    fn sign(
        headers: &mut HeaderMap,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: &[u8],
        akid: &str,
        secret: &str,
        amz_date: &str,
    ) {
        headers.insert("x-amz-date", HeaderValue::from_str(amz_date).unwrap());
        let date = &amz_date[..8];
        let signed = vec!["content-type".to_string(), "host".to_string(), "x-amz-date".to_string()];
        let sig = SignatureInput {
            access_key_id: akid.into(),
            date: date.into(),
            region: "us-east-1".into(),
            service: "sqs".into(),
            signed_headers: signed.clone(),
            signature: String::new(),
            amz_date: amz_date.into(),
            presigned: false,
        };
        let req = RequestParts { method, raw_path: path, raw_query: query, headers, body };
        let canonical = canonical_request(&req, &sig);
        let scope = format!("{date}/us-east-1/sqs/aws4_request");
        let sts =
            format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", hex::encode(Sha256::digest(canonical.as_bytes())));
        let signature = hex::encode(hmac(&signing_key(secret, date, "us-east-1", "sqs"), sts.as_bytes()));
        let value = format!(
            "AWS4-HMAC-SHA256 Credential={akid}/{scope}, SignedHeaders={}, Signature={signature}",
            signed.join(";")
        );
        headers.insert("authorization", HeaderValue::from_str(&value).unwrap());
    }

    fn auth() -> Authenticator {
        Authenticator::new(
            AuthMode::SigV4,
            "000000000000",
            vec![Credential::parse("AKIDEXAMPLE:secret:123456789012", "000000000000").unwrap()],
        )
    }

    fn base_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("host", HeaderValue::from_static("localhost:9324"));
        h.insert("content-type", HeaderValue::from_static("application/x-amz-json-1.0"));
        h
    }

    #[test]
    fn signing_key_matches_aws_docs() {
        // Example from the AWS SigV4 documentation.
        let key = signing_key("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20120215", "us-east-1", "iam");
        assert_eq!(hex::encode(key), "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d");
    }

    #[test]
    fn verifies_valid_signature() {
        let mut headers = base_headers();
        let now = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let body = br#"{"QueueName":"q"}"#;
        sign(&mut headers, "POST", "/", None, body, "AKIDEXAMPLE", "secret", &now);
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body };
        let caller = auth().authenticate(&req).unwrap();
        assert_eq!(caller.account_id, "123456789012");
        assert_eq!(caller.sender_id, "AKIDEXAMPLE");
    }

    #[test]
    fn rejects_bad_secret_tampering_and_unknown_key() {
        let now = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let body = br#"{"QueueName":"q"}"#;

        let mut headers = base_headers();
        sign(&mut headers, "POST", "/", None, body, "AKIDEXAMPLE", "wrong", &now);
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body };
        assert_eq!(auth().authenticate(&req).unwrap_err().code, ErrorCode::SignatureDoesNotMatch);

        let mut headers = base_headers();
        sign(&mut headers, "POST", "/", None, body, "AKIDEXAMPLE", "secret", &now);
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body: b"{}" };
        assert_eq!(auth().authenticate(&req).unwrap_err().code, ErrorCode::SignatureDoesNotMatch);

        let mut headers = base_headers();
        sign(&mut headers, "POST", "/", None, body, "NOPE", "secret", &now);
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body };
        assert_eq!(auth().authenticate(&req).unwrap_err().code, ErrorCode::InvalidClientTokenId);
    }

    #[test]
    fn rejects_stale_and_unsigned() {
        let body = b"{}";
        let mut headers = base_headers();
        sign(&mut headers, "POST", "/", None, body, "AKIDEXAMPLE", "secret", "20200101T000000Z");
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body };
        assert_eq!(auth().authenticate(&req).unwrap_err().code, ErrorCode::RequestExpired);

        let headers = base_headers();
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body };
        assert_eq!(auth().authenticate(&req).unwrap_err().code, ErrorCode::MissingAuthenticationToken);
    }

    #[test]
    fn none_mode_maps_accounts() {
        let a = Authenticator::new(
            AuthMode::None,
            "000000000000",
            vec![Credential::parse("AKIDX:s:111111111111", "000000000000").unwrap()],
        );
        let mut headers = base_headers();
        let now = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        sign(&mut headers, "POST", "/", None, b"", "AKIDX", "whatever", &now);
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body: b"" };
        assert_eq!(a.authenticate(&req).unwrap().account_id, "111111111111");
        let headers = base_headers();
        let req = RequestParts { method: "POST", raw_path: "/", raw_query: None, headers: &headers, body: b"" };
        assert_eq!(a.authenticate(&req).unwrap().account_id, "000000000000");
    }

    #[test]
    fn canonical_pieces() {
        assert_eq!(canonical_uri(""), "/");
        assert_eq!(canonical_uri("/"), "/");
        assert_eq!(canonical_uri("/123/my-queue"), "/123/my-queue");
        assert_eq!(canonical_uri("/a/./b/../c"), "/a/c");
        assert_eq!(canonical_uri("/a%20b"), "/a%2520b");
        assert_eq!(canonical_query(Some("b=2&a=1&a=0&c=x%20y"), false), "a=0&a=1&b=2&c=x%20y");
    }
}
