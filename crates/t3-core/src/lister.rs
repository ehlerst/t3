use crate::filter::GlobFilter;
use crate::uri::split_wildcard;
use std::sync::Arc;
use t3_s3_client::{ObjectInfo, S3Client, S3ClientError};
use t3_telemetry::TelemetryStats;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct MatchedS3Object {
    pub info: ObjectInfo,
    pub rel_key: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn stream_matching_s3_objects(
    client: S3Client,
    bucket: String,
    key_pattern: String,
    raw_mode: bool,
    all_versions: bool,
    includes: Vec<String>,
    excludes: Vec<String>,
    stats: Option<Arc<TelemetryStats>>,
    tx: mpsc::Sender<Result<MatchedS3Object, S3ClientError>>,
) {
    let sp = split_wildcard(&key_pattern, raw_mode);
    let filter = match GlobFilter::compile(sp.glob_pattern.as_deref(), &includes, &excludes) {
        Ok(f) => Arc::new(f),
        Err(e) => {
            let _ = tx
                .send(Err(S3ClientError::InvalidConfig(e.to_string())))
                .await;
            return;
        }
    };

    let exact_non_glob =
        !sp.has_glob && !sp.root_prefix.is_empty() && !sp.root_prefix.ends_with('/');

    if all_versions {
        let mut key_marker = None;
        let mut vid_marker = None;
        loop {
            let prefix_opt = if sp.root_prefix.is_empty() {
                None
            } else {
                Some(sp.root_prefix.as_str())
            };
            match client
                .list_object_versions_page(
                    &bucket,
                    prefix_opt,
                    key_marker.as_deref(),
                    vid_marker.as_deref(),
                )
                .await
            {
                Ok(page) => {
                    if let Some(ref st) = stats {
                        st.record_listed(page.objects.len() as u64);
                    }
                    for obj in page.objects {
                        if exact_non_glob
                            && obj.key != sp.root_prefix
                            && !obj.key.starts_with(&format!("{}/", sp.root_prefix))
                        {
                            continue;
                        }
                        let rel_key = compute_rel_key(&obj.key, &sp.root_prefix);
                        if filter.matches(&obj.key, &rel_key)
                            && tx
                                .send(Ok(MatchedS3Object { info: obj, rel_key }))
                                .await
                                .is_err()
                        {
                            return;
                        }
                    }
                    if !page.is_truncated {
                        break;
                    }
                    key_marker = page.next_continuation_token;
                    vid_marker = page.next_version_id_marker;
                    if key_marker.is_none() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
        return;
    }

    // Speculative Prefix Fan-Out for deep wildcard hierarchies (e.g. `logs/2026/*/*.parquet`)
    if sp.can_fanout_delimiter {
        if let Ok(first_page) = client
            .list_objects_page(&bucket, Some(&sp.root_prefix), Some("/"), None, 1000)
            .await
        {
            if !first_page.common_prefixes.is_empty() && !first_page.is_truncated {
                if let Some(ref st) = stats {
                    st.record_listed(first_page.objects.len() as u64);
                }
                for obj in first_page.objects {
                    let rel_key = compute_rel_key(&obj.key, &sp.root_prefix);
                    if filter.matches(&obj.key, &rel_key) {
                        let _ = tx.send(Ok(MatchedS3Object { info: obj, rel_key })).await;
                    }
                }

                let sem = Arc::new(Semaphore::new(32));
                let mut set = JoinSet::new();
                for sub_prefix in first_page.common_prefixes {
                    let c = client.clone();
                    let b = bucket.clone();
                    let root_p = sp.root_prefix.clone();
                    let flt = filter.clone();
                    let st = stats.clone();
                    let sender = tx.clone();
                    let s = sem.clone();
                    set.spawn(async move {
                        let _permit = s.acquire().await;
                        paginate_prefix(c, b, sub_prefix, root_p, false, flt, st, sender).await;
                    });
                }
                while set.join_next().await.is_some() {}
                return;
            }
        }
    }

    paginate_prefix(
        client,
        bucket,
        sp.root_prefix.clone(),
        sp.root_prefix,
        exact_non_glob,
        filter,
        stats,
        tx,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn paginate_prefix(
    client: S3Client,
    bucket: String,
    list_prefix: String,
    root_prefix: String,
    exact_non_glob: bool,
    filter: Arc<GlobFilter>,
    stats: Option<Arc<TelemetryStats>>,
    tx: mpsc::Sender<Result<MatchedS3Object, S3ClientError>>,
) {
    let mut token: Option<String> = None;
    loop {
        let prefix_opt = if list_prefix.is_empty() {
            None
        } else {
            Some(list_prefix.as_str())
        };
        match client
            .list_objects_page(&bucket, prefix_opt, None, token.as_deref(), 1000)
            .await
        {
            Ok(page) => {
                if let Some(ref st) = stats {
                    st.record_listed(page.objects.len() as u64);
                }
                for obj in page.objects {
                    if exact_non_glob
                        && obj.key != root_prefix
                        && !obj.key.starts_with(&format!("{}/", root_prefix))
                    {
                        continue;
                    }
                    let rel_key = compute_rel_key(&obj.key, &root_prefix);
                    if filter.matches(&obj.key, &rel_key)
                        && tx
                            .send(Ok(MatchedS3Object { info: obj, rel_key }))
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
                if !page.is_truncated {
                    break;
                }
                token = page.next_continuation_token;
                if token.is_none() {
                    break;
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }
}

pub fn compute_rel_key(full_key: &str, root_prefix: &str) -> String {
    if root_prefix.is_empty() {
        return full_key.to_string();
    }
    if full_key == root_prefix {
        return full_key.rsplit('/').next().unwrap_or(full_key).to_string();
    }
    let stripped = full_key
        .strip_prefix(root_prefix)
        .unwrap_or(full_key)
        .trim_start_matches('/');
    if stripped.is_empty() {
        full_key.rsplit('/').next().unwrap_or(full_key).to_string()
    } else {
        stripped.to_string()
    }
}
