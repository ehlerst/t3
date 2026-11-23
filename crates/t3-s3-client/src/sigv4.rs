use crate::auth::AwsCredentials;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use http::HeaderMap;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Default)]
pub struct SigV4Signer {
    cached_key: RwLock<Option<(String, String, [u8; 32])>>,
}

impl SigV4Signer {
    pub fn new() -> Self {
        Self {
            cached_key: RwLock::new(None),
        }
    }

    fn derive_signing_key(&self, secret: &str, date_stamp: &str, region: &str) -> [u8; 32] {
        {
            let guard = self.cached_key.read();
            if let Some((ref d, ref r, key)) = *guard {
                if d == date_stamp && r == region {
                    return key;
                }
            }
        }

        let k_secret = format!("AWS4{secret}");
        let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");

        let mut guard = self.cached_key.write();
        *guard = Some((date_stamp.to_string(), region.to_string(), k_signing));
        k_signing
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_request(
        &self,
        method: &str,
        url: &Url,
        headers: &mut HeaderMap,
        payload_hash: &str,
        creds: &AwsCredentials,
        region: &str,
        now: DateTime<Utc>,
    ) {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let host = match url.port() {
            Some(port) => format!("{}:{}", url.host_str().unwrap_or("localhost"), port),
            None => url.host_str().unwrap_or("localhost").to_string(),
        };

        headers.insert("host", host.parse().unwrap());
        headers.insert("x-amz-date", amz_date.parse().unwrap());
        headers.insert("x-amz-content-sha256", payload_hash.parse().unwrap());

        if let Some(ref token) = creds.session_token {
            if let Ok(val) = token.parse() {
                headers.insert("x-amz-security-token", val);
            }
        }

        let mut signed_headers: Vec<(&str, String)> = headers
            .iter()
            .filter_map(|(k, v)| {
                let name = k.as_str();
                if name == "host"
                    || name.starts_with("x-amz-")
                    || name == "content-type"
                    || name == "content-md5"
                    || name == "if-match"
                    || name == "if-none-match"
                {
                    v.to_str().ok().map(|s| (name, s.trim().to_string()))
                } else {
                    None
                }
            })
            .collect();
        signed_headers.sort_by(|a, b| a.0.cmp(b.0));

        let canonical_headers: String = signed_headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect();
        let signed_headers_str: String = signed_headers
            .iter()
            .map(|(k, _)| *k)
            .collect::<Vec<_>>()
            .join(";");

        let canonical_uri = if url.path().is_empty() {
            "/"
        } else {
            url.path()
        };

        let canonical_query = canonicalize_query(url.query());

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
        );

        let credential_scope = format!("{date_stamp}/{region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        let signing_key = self.derive_signing_key(&creds.secret_access_key, &date_stamp, region);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            creds.access_key_id, credential_scope, signed_headers_str, signature
        );
        headers.insert("authorization", auth_header.parse().unwrap());
    }

    pub fn presign_url(
        &self,
        method: &str,
        base_url: &Url,
        creds: &AwsCredentials,
        region: &str,
        expires_secs: u64,
        now: DateTime<Utc>,
    ) -> String {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();
        let credential_scope = format!("{date_stamp}/{region}/s3/aws4_request");
        let credential = format!("{}/{credential_scope}", creds.access_key_id);

        let host = match base_url.port() {
            Some(port) => format!("{}:{}", base_url.host_str().unwrap_or("localhost"), port),
            None => base_url.host_str().unwrap_or("localhost").to_string(),
        };

        let mut url = base_url.clone();
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
            qp.append_pair("X-Amz-Credential", &credential);
            qp.append_pair("X-Amz-Date", &amz_date);
            qp.append_pair("X-Amz-Expires", &expires_secs.to_string());
            qp.append_pair("X-Amz-SignedHeaders", "host");
            if let Some(ref token) = creds.session_token {
                qp.append_pair("X-Amz-Security-Token", token);
            }
        }

        let canonical_uri = if url.path().is_empty() {
            "/"
        } else {
            url.path()
        };
        let canonical_query = canonicalize_query(url.query());
        let canonical_headers = format!("host:{host}\n");
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\nhost\nUNSIGNED-PAYLOAD"
        );

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        let signing_key = self.derive_signing_key(&creds.secret_access_key, &date_stamp, region);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        url.query_pairs_mut()
            .append_pair("X-Amz-Signature", &signature);
        url.to_string()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn verify_request_signature(
        &self,
        method: &str,
        uri_path: &str,
        raw_query: Option<&str>,
        headers: &HeaderMap,
        body: &[u8],
        creds: &AwsCredentials,
        region: &str,
    ) -> Result<(), String> {
        let auth_val = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| "Missing Authorization header".to_string())?;
        let rest = auth_val
            .strip_prefix("AWS4-HMAC-SHA256 ")
            .ok_or_else(|| "Unsupported Authorization scheme".to_string())?;

        let mut cred_part = None;
        let mut signed_headers_part = None;
        let mut sig_part = None;
        for part in rest.split(',') {
            let p = part.trim();
            if let Some(v) = p.strip_prefix("Credential=") {
                cred_part = Some(v);
            } else if let Some(v) = p.strip_prefix("SignedHeaders=") {
                signed_headers_part = Some(v);
            } else if let Some(v) = p.strip_prefix("Signature=") {
                sig_part = Some(v);
            }
        }

        let cred_str = cred_part.ok_or_else(|| "Missing Credential".to_string())?;
        let signed_headers_str =
            signed_headers_part.ok_or_else(|| "Missing SignedHeaders".to_string())?;
        let expected_sig = sig_part.ok_or_else(|| "Missing Signature".to_string())?;

        let amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| "Missing x-amz-date header".to_string())?;
        let payload_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| "Missing x-amz-content-sha256 header".to_string())?;

        if payload_hash != "UNSIGNED-PAYLOAD" {
            let actual_hash = sha256_hex(body);
            if payload_hash != actual_hash {
                return Err(format!(
                    "x-amz-content-sha256 mismatch: header={payload_hash}, actual={actual_hash}"
                ));
            }
        }

        let date_stamp = amz_date
            .get(..8)
            .ok_or_else(|| "Invalid x-amz-date".to_string())?;
        let expected_scope = format!("{date_stamp}/{region}/s3/aws4_request");
        let expected_cred = format!("{}/{expected_scope}", creds.access_key_id);
        if cred_str != expected_cred {
            return Err(format!(
                "Credential scope mismatch: got={cred_str}, expected={expected_cred}"
            ));
        }

        let mut canonical_headers = String::new();
        for h_name in signed_headers_str.split(';') {
            let val = headers
                .get(h_name)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| format!("Missing signed header '{h_name}'"))?;
            canonical_headers.push_str(&format!("{}:{}\n", h_name, val.trim()));
        }

        let canonical_uri = if uri_path.is_empty() { "/" } else { uri_path };
        let canonical_query = canonicalize_query(raw_query);
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{expected_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let signing_key = self.derive_signing_key(&creds.secret_access_key, date_stamp, region);
        let computed_sig = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        if computed_sig != expected_sig {
            return Err("SignatureDoesNotMatch".to_string());
        }
        Ok(())
    }
}

fn canonicalize_query(query: Option<&str>) -> String {
    let Some(q) = query else {
        return String::new();
    };
    if q.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = form_urlencoded::parse(q.as_bytes())
        .map(|(k, v)| (aws_uri_encode(&k), aws_uri_encode(&v)))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn aws_uri_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 8);
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[inline]
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[inline]
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let res = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&res);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_sigv4_signing_and_presign() {
        let signer = SigV4Signer::new();
        let creds = AwsCredentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };
        let url = Url::parse("http://localhost:4566/my-bucket/test.txt").unwrap();
        let mut headers = HeaderMap::new();
        let now = Utc.with_ymd_and_hms(2026, 11, 8, 12, 0, 0).unwrap();

        signer.sign_request(
            "GET",
            &url,
            &mut headers,
            "UNSIGNED-PAYLOAD",
            &creds,
            "us-east-1",
            now,
        );

        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20261108/us-east-1/s3/aws4_request"
        ));

        let presigned = signer.presign_url("GET", &url, &creds, "us-east-1", 3600, now);
        assert!(presigned.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(presigned.contains("X-Amz-Expires=3600"));
        assert!(presigned.contains("X-Amz-Signature="));
    }
}
