use crate::types::{
    BucketInfo, CompletedPartInfo, DeleteObjectsOutput, ListObjectsPage, ObjectInfo, S3ClientError,
};
use chrono::{DateTime, Utc};
use quick_xml::events::Event;
use quick_xml::Reader;

pub fn parse_list_buckets(xml: &[u8]) -> Result<Vec<BucketInfo>, S3ClientError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut buckets = Vec::new();
    let mut in_bucket = false;
    let mut current_tag = String::new();
    let mut name = String::new();
    let mut creation_date = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if tag == "Bucket" {
                    in_bucket = true;
                    name.clear();
                    creation_date = None;
                } else if in_bucket {
                    current_tag = tag;
                }
            }
            Ok(Event::Text(e)) if in_bucket => {
                let txt = e.unescape().unwrap_or_default().to_string();
                match current_tag.as_str() {
                    "Name" => name = txt,
                    "CreationDate" => creation_date = parse_s3_timestamp(&txt),
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if tag == "Bucket" {
                    in_bucket = false;
                    if !name.is_empty() {
                        buckets.push(BucketInfo {
                            name: name.clone(),
                            creation_date,
                        });
                    }
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(S3ClientError::Xml(e.to_string())),
            _ => {}
        }
        buf.clear();
    }
    Ok(buckets)
}

pub fn parse_list_objects(xml: &[u8]) -> Result<ListObjectsPage, S3ClientError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut page = ListObjectsPage::default();

    let mut section = "";
    let mut current_tag = String::new();
    let mut key = String::new();
    let mut size = 0u64;
    let mut last_modified = None;
    let mut etag = None;
    let mut storage_class = None;
    let mut prefix_val = String::new();
    let mut last_key = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "Contents" => {
                        section = "Contents";
                        key.clear();
                        size = 0;
                        last_modified = None;
                        etag = None;
                        storage_class = None;
                    }
                    "CommonPrefixes" => {
                        section = "CommonPrefixes";
                        prefix_val.clear();
                    }
                    _ => current_tag = tag,
                }
            }
            Ok(Event::Text(e)) => {
                let txt = e.unescape().unwrap_or_default().to_string();
                match (section, current_tag.as_str()) {
                    ("Contents", "Key") => key = txt,
                    ("Contents", "Size") => size = txt.parse().unwrap_or(0),
                    ("Contents", "LastModified") => last_modified = parse_s3_timestamp(&txt),
                    ("Contents", "ETag") => etag = Some(txt.trim_matches('"').to_string()),
                    ("Contents", "StorageClass") => storage_class = Some(txt),
                    ("CommonPrefixes", "Prefix") => prefix_val = txt,
                    ("", "IsTruncated") => page.is_truncated = txt == "true",
                    ("", "NextContinuationToken") | ("", "NextMarker") if !txt.is_empty() => {
                        page.next_continuation_token = Some(txt);
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "Contents" => {
                        section = "";
                        last_key = key.clone();
                        page.objects.push(ObjectInfo {
                            key: key.clone(),
                            size,
                            last_modified,
                            etag: etag.take(),
                            storage_class: storage_class.take(),
                            version_id: None,
                            is_latest: None,
                            is_delete_marker: false,
                        });
                    }
                    "CommonPrefixes" => {
                        section = "";
                        if !prefix_val.is_empty() {
                            page.common_prefixes.push(prefix_val.clone());
                        }
                    }
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(S3ClientError::Xml(e.to_string())),
            _ => {}
        }
        buf.clear();
    }

    if page.is_truncated && page.next_continuation_token.is_none() && !last_key.is_empty() {
        page.next_continuation_token = Some(last_key);
    }

    Ok(page)
}

pub fn parse_list_object_versions(xml: &[u8]) -> Result<ListObjectsPage, S3ClientError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut page = ListObjectsPage::default();

    let mut section = "";
    let mut current_tag = String::new();
    let mut key = String::new();
    let mut size = 0u64;
    let mut last_modified = None;
    let mut etag = None;
    let mut storage_class = None;
    let mut version_id = None;
    let mut is_latest = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "Version" | "DeleteMarker" => {
                        section = if tag == "Version" {
                            "Version"
                        } else {
                            "DeleteMarker"
                        };
                        key.clear();
                        size = 0;
                        last_modified = None;
                        etag = None;
                        storage_class = None;
                        version_id = None;
                        is_latest = None;
                    }
                    _ => current_tag = tag,
                }
            }
            Ok(Event::Text(e)) => {
                let txt = e.unescape().unwrap_or_default().to_string();
                match (section, current_tag.as_str()) {
                    ("Version" | "DeleteMarker", "Key") => key = txt,
                    ("Version", "Size") => size = txt.parse().unwrap_or(0),
                    ("Version" | "DeleteMarker", "LastModified") => {
                        last_modified = parse_s3_timestamp(&txt)
                    }
                    ("Version", "ETag") => etag = Some(txt.trim_matches('"').to_string()),
                    ("Version", "StorageClass") => storage_class = Some(txt),
                    ("Version" | "DeleteMarker", "VersionId") => version_id = Some(txt),
                    ("Version" | "DeleteMarker", "IsLatest") => is_latest = Some(txt == "true"),
                    ("", "IsTruncated") => page.is_truncated = txt == "true",
                    ("", "NextKeyMarker") if !txt.is_empty() => {
                        page.next_continuation_token = Some(txt);
                    }
                    ("", "NextVersionIdMarker") if !txt.is_empty() => {
                        page.next_version_id_marker = Some(txt);
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if tag == "Version" || tag == "DeleteMarker" {
                    let is_dm = tag == "DeleteMarker";
                    section = "";
                    page.objects.push(ObjectInfo {
                        key: key.clone(),
                        size,
                        last_modified,
                        etag: etag.take(),
                        storage_class: storage_class.take(),
                        version_id: version_id.take(),
                        is_latest,
                        is_delete_marker: is_dm,
                    });
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(S3ClientError::Xml(e.to_string())),
            _ => {}
        }
        buf.clear();
    }

    Ok(page)
}

pub fn parse_initiate_multipart_upload(xml: &[u8]) -> Result<String, S3ClientError> {
    extract_xml_tag(xml, "UploadId")
        .ok_or_else(|| S3ClientError::Xml("Missing UploadId in response".to_string()))
}

pub fn parse_etag_from_xml(xml: &[u8]) -> Result<String, S3ClientError> {
    extract_xml_tag(xml, "ETag")
        .map(|s| s.trim_matches('"').to_string())
        .ok_or_else(|| S3ClientError::Xml("Missing ETag in response".to_string()))
}

pub fn parse_bucket_versioning(xml: &[u8]) -> Option<String> {
    extract_xml_tag(xml, "Status")
}

pub fn serialize_complete_multipart_upload(parts: &[CompletedPartInfo]) -> String {
    let mut xml = String::with_capacity(128 + parts.len() * 96);
    xml.push_str("<CompleteMultipartUpload>");
    for p in parts {
        let etag = if p.etag.starts_with('"') {
            p.etag.clone()
        } else {
            format!("\"{}\"", p.etag)
        };
        xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            p.part_number, etag
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

pub fn serialize_delete_objects(keys: &[(String, Option<String>)], quiet: bool) -> String {
    let mut xml = String::with_capacity(128 + keys.len() * 64);
    xml.push_str("<Delete>");
    if quiet {
        xml.push_str("<Quiet>true</Quiet>");
    }
    for (k, vid) in keys {
        xml.push_str("<Object><Key>");
        xml.push_str(&quick_xml::escape::escape(k));
        xml.push_str("</Key>");
        if let Some(v) = vid {
            xml.push_str("<VersionId>");
            xml.push_str(&quick_xml::escape::escape(v));
            xml.push_str("</VersionId>");
        }
        xml.push_str("</Object>");
    }
    xml.push_str("</Delete>");
    xml
}

pub fn parse_delete_objects_result(xml: &[u8]) -> Result<DeleteObjectsOutput, S3ClientError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut out = DeleteObjectsOutput {
        deleted: Vec::new(),
        errors: Vec::new(),
    };
    let mut section = "";
    let mut current_tag = String::new();
    let mut key = String::new();
    let mut msg = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "Deleted" => {
                        section = "Deleted";
                        key.clear();
                    }
                    "Error" => {
                        section = "Error";
                        key.clear();
                        msg.clear();
                    }
                    _ => current_tag = tag,
                }
            }
            Ok(Event::Text(e)) => {
                let txt = e.unescape().unwrap_or_default().to_string();
                match (section, current_tag.as_str()) {
                    ("Deleted" | "Error", "Key") => key = txt,
                    ("Error", "Message" | "Code") => {
                        if msg.is_empty() {
                            msg = txt;
                        } else {
                            msg = format!("{msg}: {txt}");
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "Deleted" => {
                        section = "";
                        if !key.is_empty() {
                            out.deleted.push(key.clone());
                        }
                    }
                    "Error" => {
                        section = "";
                        out.errors.push((key.clone(), msg.clone()));
                    }
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(S3ClientError::Xml(e.to_string())),
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

pub fn parse_s3_error(status: u16, xml: &[u8]) -> S3ClientError {
    let code = extract_xml_tag(xml, "Code").unwrap_or_else(|| format!("HTTP_{status}"));
    let message = extract_xml_tag(xml, "Message")
        .unwrap_or_else(|| String::from_utf8_lossy(xml).trim().to_string());
    S3ClientError::Service {
        status,
        code,
        message,
    }
}

fn extract_xml_tag(xml: &[u8], target: &str) -> Option<String> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut in_target = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                if e.local_name().as_ref() == target.as_bytes() {
                    in_target = true;
                }
            }
            Ok(Event::Text(e)) if in_target => {
                return Some(e.unescape().unwrap_or_default().to_string());
            }
            Ok(Event::End(e)) => {
                if e.local_name().as_ref() == target.as_bytes() {
                    in_target = false;
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    None
}

pub fn parse_s3_timestamp(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc2822(s)
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        })
}
