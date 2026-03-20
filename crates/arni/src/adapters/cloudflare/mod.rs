//! Cloudflare storage adapters: D1 (SQL), KV (key-value), R2 (object storage).
//!
//! All three products use the Cloudflare REST API. D1 and KV share a common
//! HTTP transport client ([`http::CloudflareClient`]) with Bearer token auth,
//! exponential-backoff retry, and Cloudflare error-envelope mapping. R2 uses
//! the S3-compatible API via `aws-sdk-s3` with a custom endpoint.
//!
//! # Feature flags
//!
//! | Feature | Adapter |
//! |---|---|
//! | `cloudflare-d1` | [`d1::D1Adapter`] |
//! | `cloudflare-kv` | [`kv::KVAdapter`] |
//! | `cloudflare-r2` | [`r2::R2Adapter`] |
//! | `cloudflare` | all three |
//!
//! # Authentication
//!
//! D1 and KV require a Cloudflare API token (`parameters["api_token"]`).
//! R2 uses separate S3-compatible credentials (`parameters["r2_access_key_id"]`
//! and `parameters["r2_secret_access_key"]`).
//!
//! See `docs/cloudflare.md` for full auth setup and `ConnectionConfig` field mapping.

#[cfg(any(feature = "cloudflare-d1", feature = "cloudflare-kv"))]
pub(crate) mod http;

#[cfg(feature = "cloudflare-d1")]
pub mod d1;

#[cfg(feature = "cloudflare-kv")]
pub mod kv;

#[cfg(feature = "cloudflare-r2")]
pub mod r2;
