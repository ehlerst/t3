use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Deserialize)]
struct ContainerCredentialsResponse {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "Token", default)]
    token: Option<String>,
}

impl ResolvedConfig {
    /// Resolves AWS credentials, region, and S3 endpoints following the standard Amazon AWS
    /// credential provider chain:
    /// 1. `--no-sign-request` (anonymous access)
    /// 2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` / `AWS_SECURITY_TOKEN`)
    /// 3. Web Identity Token / EKS IRSA (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE` via STS `AssumeRoleWithWebIdentity`)
    /// 4. Shared credentials and config files (`~/.aws/credentials`, `~/.aws/config`, including profile `role_arn` + `web_identity_token_file`)
    /// 5. EKS Pod Identity / ECS Container Credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI` / `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`)
    /// 6. Local S3 emulator default (`test` / `test`)
    pub fn resolve(overrides: &ConfigOverrides) -> Self {
        let profile = overrides
            .profile
            .clone()
            .or_else(|| non_empty_env("AWS_PROFILE"))
            .or_else(|| non_empty_env("AWS_DEFAULT_PROFILE"))
            .unwrap_or_else(|| "default".to_string());

        let home = env::var("HOME").ok().map(PathBuf::from);
        let creds_path = overrides
            .credentials_file
            .clone()
            .or_else(|| non_empty_env("AWS_SHARED_CREDENTIALS_FILE").map(PathBuf::from))
            .or_else(|| home.as_ref().map(|h| h.join(".aws").join("credentials")));

        let config_path = non_empty_env("AWS_CONFIG_FILE")
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|h| h.join(".aws").join("config")));

        let file_creds = creds_path
            .as_deref()
            .and_then(|p| parse_ini_section(p, &profile));
        let file_config = config_path.as_deref().and_then(|p| {
            parse_ini_section(p, &format!("profile {profile}"))
                .or_else(|| parse_ini_section(p, &profile))
        });

        let region = overrides
            .region
            .clone()
            .or_else(|| non_empty_env("AWS_REGION"))
            .or_else(|| non_empty_env("AWS_DEFAULT_REGION"))
            .or_else(|| file_config.as_ref().and_then(|c| c.get("region").cloned()))
            .or_else(|| file_creds.as_ref().and_then(|c| c.get("region").cloned()))
            .unwrap_or_else(|| "us-east-1".to_string());

        let sts_endpoint = non_empty_env("AWS_ENDPOINT_URL_STS")
            .or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|c| c.get("sts_endpoint").cloned())
            })
            .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));

        let credentials = if overrides.no_sign_request {
            None
        } else if let Some(env_creds) = resolve_env_credentials() {
            Some(env_creds)
        } else if let Some(irsa_creds) =
            resolve_irsa_from_env(&sts_endpoint, overrides.no_verify_ssl)
        {
            Some(irsa_creds)
        } else if let Some(prof_creds) = resolve_profile_credentials(
            file_creds.as_ref(),
            file_config.as_ref(),
            &sts_endpoint,
            overrides.no_verify_ssl,
        ) {
            Some(prof_creds)
        } else if let Some(container_creds) = resolve_container_credentials(overrides.no_verify_ssl)
        {
            Some(container_creds)
        } else {
            Some(AwsCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                session_token: None,
            })
        };

        let raw_endpoint = overrides
            .endpoint_url
            .clone()
            .or_else(|| non_empty_env("S3_ENDPOINT_URL"))
            .or_else(|| non_empty_env("AWS_ENDPOINT_URL_S3"))
            .or_else(|| non_empty_env("AWS_ENDPOINT_URL"))
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

fn non_empty_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 1. Standard AWS environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` / `AWS_SECURITY_TOKEN`).
pub fn resolve_env_credentials() -> Option<AwsCredentials> {
    let ak = non_empty_env("AWS_ACCESS_KEY_ID")?;
    let sk = non_empty_env("AWS_SECRET_ACCESS_KEY")?;
    let token = non_empty_env("AWS_SESSION_TOKEN").or_else(|| non_empty_env("AWS_SECURITY_TOKEN"));
    Some(AwsCredentials {
        access_key_id: ak,
        secret_access_key: sk,
        session_token: token,
    })
}

/// 2. Amazon EKS IRSA (IAM Roles for Service Accounts) via `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`.
pub fn resolve_irsa_from_env(sts_endpoint: &str, no_verify_ssl: bool) -> Option<AwsCredentials> {
    let role_arn = non_empty_env("AWS_ROLE_ARN")?;
    let token_file = non_empty_env("AWS_WEB_IDENTITY_TOKEN_FILE")?;
    let session_name = non_empty_env("AWS_ROLE_SESSION_NAME").unwrap_or_else(default_session_name);
    assume_role_with_web_identity(
        sts_endpoint,
        &role_arn,
        &session_name,
        Path::new(&token_file),
        no_verify_ssl,
    )
}

/// 3. Shared credentials / config profile (static keys or `role_arn` + `web_identity_token_file`).
fn resolve_profile_credentials(
    file_creds: Option<&HashMap<String, String>>,
    file_config: Option<&HashMap<String, String>>,
    sts_endpoint: &str,
    no_verify_ssl: bool,
) -> Option<AwsCredentials> {
    for sec in [file_creds, file_config].into_iter().flatten() {
        if let (Some(ak), Some(sk)) = (
            sec.get("aws_access_key_id").filter(|s| !s.is_empty()),
            sec.get("aws_secret_access_key").filter(|s| !s.is_empty()),
        ) {
            let token = sec
                .get("aws_session_token")
                .or_else(|| sec.get("aws_security_token"))
                .filter(|s| !s.is_empty())
                .cloned();
            return Some(AwsCredentials {
                access_key_id: ak.clone(),
                secret_access_key: sk.clone(),
                session_token: token,
            });
        }

        if let (Some(role_arn), Some(token_file)) = (
            sec.get("role_arn").filter(|s| !s.is_empty()),
            sec.get("web_identity_token_file").filter(|s| !s.is_empty()),
        ) {
            let session_name = sec
                .get("role_session_name")
                .filter(|s| !s.is_empty())
                .cloned()
                .unwrap_or_else(default_session_name);
            if let Some(creds) = assume_role_with_web_identity(
                sts_endpoint,
                role_arn,
                &session_name,
                Path::new(token_file),
                no_verify_ssl,
            ) {
                return Some(creds);
            }
        }
    }
    None
}

/// 4. EKS Pod Identity & ECS Container Credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI` / `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`).
pub fn resolve_container_credentials(no_verify_ssl: bool) -> Option<AwsCredentials> {
    let uri = if let Some(full) = non_empty_env("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
        full
    } else {
        let rel = non_empty_env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")?;
        format!("http://169.254.170.2{rel}")
    };

    let auth_token =
        if let Some(token_path) = non_empty_env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
            fs::read_to_string(token_path)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        } else {
            non_empty_env("AWS_CONTAINER_AUTHORIZATION_TOKEN")
        };

    run_blocking_http(async move {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(no_verify_ssl)
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        let mut req = client.get(&uri);
        if let Some(tok) = auth_token {
            req = req.header("Authorization", tok);
        }
        let resp = req.send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let parsed: ContainerCredentialsResponse = resp.json().await.ok()?;
        Some(AwsCredentials {
            access_key_id: parsed.access_key_id,
            secret_access_key: parsed.secret_access_key,
            session_token: parsed.token.filter(|t| !t.is_empty()),
        })
    })
}

/// Executes STS `AssumeRoleWithWebIdentity` using the projected JWT file (`AWS_WEB_IDENTITY_TOKEN_FILE`).
pub fn assume_role_with_web_identity(
    sts_endpoint: &str,
    role_arn: &str,
    role_session_name: &str,
    token_file: &Path,
    no_verify_ssl: bool,
) -> Option<AwsCredentials> {
    let web_identity_token = fs::read_to_string(token_file)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;

    let endpoint = sts_endpoint.trim_end_matches('/').to_string();
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("Action", "AssumeRoleWithWebIdentity")
        .append_pair("Version", "2011-06-15")
        .append_pair("RoleArn", role_arn)
        .append_pair("RoleSessionName", role_session_name)
        .append_pair("WebIdentityToken", &web_identity_token)
        .finish();

    run_blocking_http(async move {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(no_verify_ssl)
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;
        let resp = client
            .post(&endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let xml_body = resp.text().await.ok()?;
        parse_sts_credentials_xml(&xml_body)
    })
}

/// Parses `<Credentials>` (`<AccessKeyId>`, `<SecretAccessKey>`, `<SessionToken>`) from an AWS STS XML response.
pub fn parse_sts_credentials_xml(xml: &str) -> Option<AwsCredentials> {
    let creds_block = extract_xml_tag(xml, "Credentials").unwrap_or(xml);
    let access_key_id = extract_xml_tag(creds_block, "AccessKeyId")?
        .trim()
        .to_string();
    let secret_access_key = extract_xml_tag(creds_block, "SecretAccessKey")?
        .trim()
        .to_string();
    let session_token = extract_xml_tag(creds_block, "SessionToken")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return None;
    }

    Some(AwsCredentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}

fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

fn default_session_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("t3-session-{millis}")
}

fn run_blocking_http<F, T>(fut: F) -> Option<T>
where
    F: std::future::Future<Output = Option<T>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        rt.block_on(fut)
    })
    .join()
    .ok()
    .flatten()
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

    #[test]
    fn test_parse_sts_credentials_xml() {
        let sample_xml = r#"
<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAIOSFODNN7EXAMPLE</AccessKeyId>
      <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>
      <SessionToken>AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQ==</SessionToken>
      <Expiration>2026-11-09T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
"#;
        let creds = parse_sts_credentials_xml(sample_xml).expect("valid STS XML");
        assert_eq!(creds.access_key_id, "ASIAIOSFODNN7EXAMPLE");
        assert_eq!(
            creds.secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY"
        );
        assert_eq!(
            creds.session_token.as_deref(),
            Some("AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQ==")
        );
    }

    #[tokio::test]
    async fn test_irsa_assume_role_with_web_identity_e2e() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("Action=AssumeRoleWithWebIdentity"));
                assert!(req_str.contains("WebIdentityToken=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"));

                let xml_resp = r#"<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIA_IRSA_TEST_KEY</AccessKeyId>
      <SecretAccessKey>IRSA_SECRET_KEY_VALUE</SecretAccessKey>
      <SessionToken>IRSA_SESSION_TOKEN_VALUE</SessionToken>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;
                let http_resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    xml_resp.len(),
                    xml_resp
                );
                let _ = stream.write_all(http_resp.as_bytes());
            }
        });

        let mut token_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(token_file, "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9").unwrap();

        let sts_endpoint = format!("http://{addr}");
        let creds = assume_role_with_web_identity(
            &sts_endpoint,
            "arn:aws:iam::123456789012:role/eks-irsa-s3-role",
            "t3-test-session",
            token_file.path(),
            false,
        )
        .expect("IRSA AssumeRoleWithWebIdentity should succeed");

        assert_eq!(creds.access_key_id, "ASIA_IRSA_TEST_KEY");
        assert_eq!(creds.secret_access_key, "IRSA_SECRET_KEY_VALUE");
        assert_eq!(
            creds.session_token.as_deref(),
            Some("IRSA_SESSION_TOKEN_VALUE")
        );
    }
}
