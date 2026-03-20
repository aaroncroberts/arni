//! Shared HTTP transport for Cloudflare D1 and KV adapters.
//!
//! [`CloudflareClient`] wraps `reqwest` with Bearer token authentication,
//! Cloudflare API error-envelope parsing, and exponential-backoff retry on
//! rate-limit responses (HTTP 429 with `Retry-After` header).

use std::time::Duration;

pub(crate) use bytes::Bytes;
use reqwest::{header, Client};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::DataError;

type Result<T> = std::result::Result<T, DataError>;

/// Maximum retry attempts on HTTP 429 rate-limit responses.
const MAX_RETRIES: u32 = 3;

/// Fallback base delay when no `Retry-After` header is present.
const BASE_RETRY_DELAY_SECS: u64 = 1;

// ── Cloudflare response envelope ─────────────────────────────────────────────

#[derive(Deserialize)]
struct CfResponse<T> {
    success: bool,
    errors: Vec<CfError>,
    result: Option<T>,
}

#[derive(Deserialize)]
struct CfError {
    code: u32,
    message: String,
}

// ── CloudflareClient ─────────────────────────────────────────────────────────

/// Shared HTTP client for Cloudflare D1 and KV REST APIs.
///
/// Handles:
/// - Bearer token auth injection on every request
/// - Cloudflare `{success, errors, result}` envelope parsing
/// - Retry on HTTP 429 with `Retry-After` backoff
pub(crate) struct CloudflareClient {
    client: Client,
    api_token: String,
    account_id: String,
}

impl CloudflareClient {
    const BASE_URL: &'static str = "https://api.cloudflare.com/client/v4";

    /// Create a new client with the given Cloudflare API token and account ID.
    pub(crate) fn new(api_token: String, account_id: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| DataError::Connection(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            client,
            api_token,
            account_id,
        })
    }

    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.api_token)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", Self::BASE_URL, path)
    }

    // ── High-level envelope helpers ───────────────────────────────────────────

    /// GET a path and unwrap the Cloudflare response envelope, returning `T`.
    #[allow(dead_code)] // Reserved for future D1/KV metadata endpoints
    pub(crate) async fn cf_get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let resp = self.get_with_retry(path).await?;
        self.unwrap_envelope(resp).await
    }

    /// POST JSON body to a path and unwrap the Cloudflare response envelope.
    pub(crate) async fn cf_post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let resp = self.post_with_retry(path, body).await?;
        self.unwrap_envelope(resp).await
    }

    // ── KV raw-value helpers (no envelope) ───────────────────────────────────

    /// GET a path and return the raw response bytes (used for KV value reads).
    pub(crate) async fn get_bytes(&self, path: &str) -> Result<Bytes> {
        let resp = self.get_with_retry(path).await?;
        if resp.status() == 404 {
            return Err(DataError::Query("key not found".to_string()));
        }
        if !resp.status().is_success() {
            return Err(DataError::Query(format!(
                "HTTP {} from Cloudflare",
                resp.status()
            )));
        }
        resp.bytes()
            .await
            .map_err(|e| DataError::Query(format!("failed to read response bytes: {e}")))
    }

    /// PUT raw bytes to a path with the given content-type (used for KV value writes).
    pub(crate) async fn put_bytes(
        &self,
        path: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<()> {
        let url = self.url(path);
        let resp = self
            .client
            .put(&url)
            .header(header::AUTHORIZATION, self.auth_header())
            .header(header::CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await
            .map_err(|e| DataError::Connection(format!("HTTP PUT failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(DataError::Query(format!(
                "HTTP {} from Cloudflare PUT",
                resp.status()
            )));
        }
        Ok(())
    }

    /// DELETE a path (used for KV and D1 operations that return the envelope).
    pub(crate) async fn cf_delete(&self, path: &str) -> Result<()> {
        let url = self.url(path);
        let resp = self
            .client
            .delete(&url)
            .header(header::AUTHORIZATION, self.auth_header())
            .send()
            .await
            .map_err(|e| DataError::Connection(format!("HTTP DELETE failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(DataError::Query(format!(
                "HTTP {} from Cloudflare DELETE",
                resp.status()
            )));
        }
        Ok(())
    }

    // ── Retry primitives ─────────────────────────────────────────────────────

    async fn get_with_retry(&self, path: &str) -> Result<reqwest::Response> {
        let url = self.url(path);
        self.send_with_retry(|| {
            self.client
                .get(&url)
                .header(header::AUTHORIZATION, self.auth_header())
        })
        .await
    }

    async fn post_with_retry<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<reqwest::Response> {
        let url = self.url(path);
        let body_bytes = serde_json::to_vec(body)
            .map_err(|e| DataError::Query(format!("failed to serialize request body: {e}")))?;

        self.send_with_retry(|| {
            self.client
                .post(&url)
                .header(header::AUTHORIZATION, self.auth_header())
                .header(header::CONTENT_TYPE, "application/json")
                .body(body_bytes.clone())
        })
        .await
    }

    async fn send_with_retry<F>(&self, make_req: F) -> Result<reqwest::Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut attempt = 0u32;
        loop {
            let resp = make_req()
                .send()
                .await
                .map_err(|e| DataError::Connection(format!("HTTP request failed: {e}")))?;

            if resp.status() != 429 || attempt >= MAX_RETRIES {
                return Ok(resp);
            }

            // Read Retry-After header, fall back to exponential backoff
            let delay = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(BASE_RETRY_DELAY_SECS << attempt);

            tokio::time::sleep(Duration::from_secs(delay)).await;
            attempt += 1;
        }
    }

    // ── Envelope unwrapping ───────────────────────────────────────────────────

    async fn unwrap_envelope<T: DeserializeOwned>(&self, resp: reqwest::Response) -> Result<T> {
        if !resp.status().is_success() {
            // Try to decode the envelope for a meaningful error message
            if let Ok(cf) = resp.json::<CfResponse<serde_json::Value>>().await {
                let msg = cf
                    .errors
                    .first()
                    .map(|e| format!("[{}] {}", e.code, e.message))
                    .unwrap_or_else(|| "unknown Cloudflare error".to_string());
                return Err(DataError::Query(msg));
            }
            return Err(DataError::Connection(
                "non-success HTTP response from Cloudflare".to_string(),
            ));
        }

        let cf: CfResponse<T> = resp
            .json()
            .await
            .map_err(|e| DataError::Query(format!("failed to parse Cloudflare response: {e}")))?;

        if !cf.success {
            let msg = cf
                .errors
                .first()
                .map(|e| format!("[{}] {}", e.code, e.message))
                .unwrap_or_else(|| {
                    "Cloudflare returned success=false with no error details".to_string()
                });
            return Err(DataError::Query(msg));
        }

        cf.result.ok_or_else(|| {
            DataError::Query("Cloudflare returned success=true but empty result".to_string())
        })
    }
}
