use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum S3ClientError {
    #[error("HTTP transport error: {0}")]
    Transport(String),

    #[error("S3 service error (HTTP {status}, code={code}): {message}")]
    Service {
        status: u16,
        code: String,
        message: String,
    },

    #[error("XML parse error: {0}")]
    Xml(String),

    #[error("Invalid configuration or URL: {0}")]
    InvalidConfig(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

impl S3ClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            S3ClientError::Transport(_) => true,
            S3ClientError::Service { status, code, .. } => {
                matches!(*status, 429 | 500 | 502 | 503 | 504)
                    || code == "SlowDown"
                    || code == "InternalError"
                    || code == "RequestTimeout"
            }
            _ => false,
        }
    }

    pub fn is_not_found(&self) -> bool {
        match self {
            S3ClientError::NotFound(_) => true,
            S3ClientError::Service { status, code, .. } => {
                *status == 404
                    || code == "NoSuchKey"
                    || code == "NoSuchBucket"
                    || code == "NotFound"
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub creation_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
    pub version_id: Option<String>,
    pub is_latest: Option<bool>,
    pub is_delete_marker: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListObjectsPage {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub next_version_id_marker: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectHead {
    pub content_length: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
    pub storage_class: Option<String>,
    pub version_id: Option<String>,
    pub metadata: HashMap<String, String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PutObjectOptions {
    pub storage_class: Option<String>,
    pub acl: Option<String>,
    pub sse: Option<String>,
    pub sse_kms_key_id: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub metadata: HashMap<String, String>,
    pub metadata_directive: Option<String>,
    pub checksum_crc32c: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPartInfo {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectsOutput {
    pub deleted: Vec<String>,
    pub errors: Vec<(String, String)>,
}
