pub mod engine;
pub mod filter;
pub mod lister;
pub mod run_parser;
pub mod select;
pub mod uri;

pub use engine::{
    parse_duration_secs, CopyOptions, DuOptions, LsOptions, RmOptions, SyncOptions, T3Engine,
};
pub use filter::{FilterError, GlobFilter};
pub use lister::{compute_rel_key, stream_matching_s3_objects, MatchedS3Object};
pub use run_parser::parse_run_line;
pub use select::SelectQuery;
pub use uri::{compute_destination_key, contains_glob_meta, split_wildcard, StorageLocation};

pub fn sanitize_url_for_bench(s: &str) -> bool {
    StorageLocation::parse(s).is_ok()
}
