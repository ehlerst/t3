use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UriError {
    #[error("Invalid S3 URL '{0}': bucket name is required")]
    MissingBucket(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageLocation {
    S3 { bucket: String, key: String },
    Local { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitPattern {
    pub root_prefix: String,
    pub glob_pattern: Option<String>,
    pub has_glob: bool,
    pub can_fanout_delimiter: bool,
}

impl StorageLocation {
    pub fn parse(raw: &str) -> Result<Self, UriError> {
        if let Some(rest) = raw.strip_prefix("s3://") {
            let trimmed = rest.trim_start_matches('/');
            if trimmed.is_empty() {
                return Err(UriError::MissingBucket(raw.to_string()));
            }
            let mut parts = trimmed.splitn(2, '/');
            let bucket = parts.next().unwrap_or("").to_string();
            if bucket.is_empty() {
                return Err(UriError::MissingBucket(raw.to_string()));
            }
            let key = parts.next().unwrap_or("").to_string();
            Ok(StorageLocation::S3 { bucket, key })
        } else {
            Ok(StorageLocation::Local {
                path: PathBuf::from(raw),
            })
        }
    }

    pub fn is_s3(&self) -> bool {
        matches!(self, StorageLocation::S3 { .. })
    }
}

pub fn contains_glob_meta(s: &str) -> bool {
    s.bytes().any(|b| matches!(b, b'*' | b'?' | b'['))
}

/// Splits an S3 key or local path at the first wildcard meta-character (`*`, `?`, `[`).
/// Returns the static prefix prior to the wildcard, the full glob pattern, and whether
/// speculative `Delimiter=/` prefix fan-out can accelerate listing.
pub fn split_wildcard(pattern: &str, raw_mode: bool) -> SplitPattern {
    if raw_mode || !contains_glob_meta(pattern) {
        return SplitPattern {
            root_prefix: pattern.to_string(),
            glob_pattern: None,
            has_glob: false,
            can_fanout_delimiter: false,
        };
    }

    let first_meta = pattern.find(['*', '?', '[']).unwrap_or(pattern.len());
    let before_meta = &pattern[..first_meta];
    let after_meta = &pattern[first_meta..];

    let root_prefix = match before_meta.rfind('/') {
        Some(slash_idx) => before_meta[..=slash_idx].to_string(),
        None => before_meta.to_string(),
    };

    // If the wildcard matches a single path segment followed by `/` (e.g. `logs/2026/*/*.parquet`),
    // we can speculatively list `Prefix="logs/2026/", Delimiter="/"` and fan out across sub-prefixes!
    let can_fanout_delimiter = root_prefix.ends_with('/')
        && after_meta.starts_with('*')
        && !after_meta.starts_with("**")
        && after_meta[1..].contains('/');

    SplitPattern {
        root_prefix,
        glob_pattern: Some(pattern.to_string()),
        has_glob: true,
        can_fanout_delimiter,
    }
}

pub fn compute_destination_key(
    base_dst_key: &str,
    rel_path: &str,
    flatten: bool,
    src_was_multi: bool,
) -> String {
    let effective_rel = if flatten {
        rel_path.rsplit('/').next().unwrap_or(rel_path)
    } else {
        rel_path.trim_start_matches('/')
    };

    if base_dst_key.is_empty() {
        return effective_rel.to_string();
    }

    if base_dst_key.ends_with('/') || src_was_multi {
        let prefix = base_dst_key.trim_end_matches('/');
        if effective_rel.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix}/{effective_rel}")
        }
    } else {
        base_dst_key.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_storage_location() {
        let loc = StorageLocation::parse("s3://my-bucket/dir/file.txt").unwrap();
        assert_eq!(
            loc,
            StorageLocation::S3 {
                bucket: "my-bucket".to_string(),
                key: "dir/file.txt".to_string()
            }
        );

        let bucket_only = StorageLocation::parse("s3://my-bucket").unwrap();
        assert_eq!(
            bucket_only,
            StorageLocation::S3 {
                bucket: "my-bucket".to_string(),
                key: String::new()
            }
        );
    }

    #[test]
    fn test_split_wildcard_and_fanout() {
        let sp = split_wildcard("logs/2026/*/*.parquet", false);
        assert_eq!(sp.root_prefix, "logs/2026/");
        assert!(sp.has_glob);
        assert!(sp.can_fanout_delimiter);

        let sp_raw = split_wildcard("logs/2026/*/*.parquet", true);
        assert!(!sp_raw.has_glob);
    }
}
