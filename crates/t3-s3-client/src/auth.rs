use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub credentials: Option<AwsCredentials>,
    pub region: String,
    pub endpoints: Vec<String>,
    pub no_sign_request: bool,
    pub no_verify_ssl: bool,
    pub use_list_objects_v1: bool,
    pub request_payer: Option<String>,
    pub retry_count: u32,
    pub unsigned_payload: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub endpoint_url: Option<String>,
    pub profile: Option<String>,
    pub credentials_file: Option<PathBuf>,
    pub region: Option<String>,
    pub no_sign_request: bool,
    pub no_verify_ssl: bool,
    pub use_list_objects_v1: bool,
    pub request_payer: Option<String>,
    pub retry_count: u32,
    pub unsigned_payload: bool,
}

impl ResolvedConfig {
    pub fn resolve(overrides: &ConfigOverrides) -> Self {
        let profile = overrides
            .profile
            .clone()
            .or_else(|| env::var("AWS_PROFILE").ok())
            .or_else(|| env::var("AWS_DEFAULT_PROFILE").ok())
            .unwrap_or_else(|| "default".to_string());

        let home = env::var("HOME").ok().map(PathBuf::from);
        let creds_path = overrides
            .credentials_file
            .clone()
            .or_else(|| {
                env::var("AWS_SHARED_CREDENTIALS_FILE")
                    .ok()
                    .map(PathBuf::from)
            })
            .or_else(|| home.as_ref().map(|h| h.join(".aws").join("credentials")));

        let config_path = env::var("AWS_CONFIG_FILE")
            .ok()
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|h| h.join(".aws").join("config")));

        let file_creds = creds_path
            .as_deref()
            .and_then(|p| parse_ini_section(p, &profile));
        let file_config = config_path.as_deref().and_then(|p| {
            parse_ini_section(p, &format!("profile {profile}"))
                .or_else(|| parse_ini_section(p, &profile))
        });

        let credentials = if overrides.no_sign_request {
            None
        } else if let (Ok(ak), Ok(sk)) = (
            env::var("AWS_ACCESS_KEY_ID"),
            env::var("AWS_SECRET_ACCESS_KEY"),
        ) {
            Some(AwsCredentials {
                access_key_id: ak,
                secret_access_key: sk,
                session_token: env::var("AWS_SESSION_TOKEN").ok(),
            })
        } else if let Some(sec) = file_creds.as_ref().or(file_config.as_ref()) {
            if let (Some(ak), Some(sk)) = (
                sec.get("aws_access_key_id"),
                sec.get("aws_secret_access_key"),
            ) {
                Some(AwsCredentials {
                    access_key_id: ak.clone(),
                    secret_access_key: sk.clone(),
                    session_token: sec.get("aws_session_token").cloned(),
                })
            } else {
                Some(AwsCredentials {
                    access_key_id: "test".to_string(),
                    secret_access_key: "test".to_string(),
                    session_token: None,
                })
            }
        } else {
            Some(AwsCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                session_token: None,
            })
        };

        let region = overrides
            .region
            .clone()
            .or_else(|| env::var("AWS_REGION").ok())
            .or_else(|| env::var("AWS_DEFAULT_REGION").ok())
            .or_else(|| file_config.as_ref().and_then(|c| c.get("region").cloned()))
            .or_else(|| file_creds.as_ref().and_then(|c| c.get("region").cloned()))
            .unwrap_or_else(|| "us-east-1".to_string());

        let raw_endpoint = overrides
            .endpoint_url
            .clone()
            .or_else(|| env::var("S3_ENDPOINT_URL").ok())
            .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
            .or_else(|| env::var("AWS_ENDPOINT_URL_S3").ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|c| c.get("endpoint_url").cloned())
            })
            .or_else(|| {
                file_creds
                    .as_ref()
                    .and_then(|c| c.get("endpoint_url").cloned())
            });

        let endpoints = match raw_endpoint {
            Some(ep_str) => ep_str
                .split(',')
                .map(|s| s.trim().trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            None => vec![format!("https://s3.{region}.amazonaws.com")],
        };

        Self {
            credentials,
            region,
            endpoints,
            no_sign_request: overrides.no_sign_request,
            no_verify_ssl: overrides.no_verify_ssl,
            use_list_objects_v1: overrides.use_list_objects_v1,
            request_payer: overrides.request_payer.clone(),
            retry_count: overrides.retry_count,
            unsigned_payload: overrides.unsigned_payload,
        }
    }
}

pub fn parse_ini_section(path: &Path, target_section: &str) -> Option<HashMap<String, String>> {
    let content = fs::read_to_string(path).ok()?;
    let mut current_section = String::new();
    let mut map = HashMap::new();
    let mut found = false;

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            current_section = line[1..line.len() - 1].trim().to_string();
            if current_section == target_section {
                found = true;
            }
            continue;
        }
        if current_section == target_section {
            if let Some((k, v)) = line.split_once('=') {
                map.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }

    if found {
        Some(map)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_ini_section_and_endpoint_url() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            tmp,
            r#"
[default]
aws_access_key_id = AKIA_DEFAULT
aws_secret_access_key = SECRET_DEFAULT

[profile ruststack]
aws_access_key_id = AKIA_RS
aws_secret_access_key = SECRET_RS
region = us-west-2
endpoint_url = http://localhost:4566
"#
        )
        .unwrap();

        let sec = parse_ini_section(tmp.path(), "profile ruststack").unwrap();
        assert_eq!(sec.get("aws_access_key_id").unwrap(), "AKIA_RS");
        assert_eq!(sec.get("region").unwrap(), "us-west-2");
        assert_eq!(sec.get("endpoint_url").unwrap(), "http://localhost:4566");
    }
}
