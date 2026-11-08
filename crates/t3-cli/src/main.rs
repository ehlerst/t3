use anyhow::{bail, Result};
use clap::{Args, Parser, Subcommand};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::process::ExitCode;
use t3_core::{
    parse_duration_secs, parse_run_line, CopyOptions, DuOptions, LsOptions, RmOptions, SyncOptions,
    T3Engine,
};
use t3_s3_client::{ConfigOverrides, PutObjectOptions, ResolvedConfig, S3Client};
use t3_telemetry::{LogLevel, OutputContext};
use tokio::task::JoinSet;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "t3",
    version,
    about = "t3 (Turbo 3): Blazing-fast parallel S3 client and drop-in s5cmd replacement in Rust"
)]
pub struct Cli {
    /// Size of global object worker pool (negative values = N * num_cpus)
    #[arg(
        long = "numworkers",
        default_value_t = 256,
        global = true,
        allow_hyphen_values = true
    )]
    pub numworkers: isize,

    /// Max retries per request with exponential backoff & jitter
    #[arg(short = 'r', long = "retry-count", default_value_t = 10, global = true)]
    pub retry_count: u32,

    /// Custom S3 endpoint URL (supports comma-separated URLs for round-robin load balancing)
    #[arg(long = "endpoint-url", env = "S3_ENDPOINT_URL", global = true)]
    pub endpoint_url: Option<String>,

    /// AWS credentials/config profile
    #[arg(long = "profile", global = true)]
    pub profile: Option<String>,

    /// Custom credentials file path
    #[arg(long = "credentials-file", global = true)]
    pub credentials_file: Option<PathBuf>,

    /// Do not sign requests (anonymous access for public buckets)
    #[arg(long = "no-sign-request", default_value_t = false, global = true)]
    pub no_sign_request: bool,

    /// Skip TLS certificate verification
    #[arg(long = "no-verify-ssl", default_value_t = false, global = true)]
    pub no_verify_ssl: bool,

    /// Fallback to ListObjects v1 for legacy S3 / GCS compatibility
    #[arg(long = "use-list-objects-v1", default_value_t = false, global = true)]
    pub use_list_objects_v1: bool,

    /// Set x-amz-request-payer header (e.g. requester)
    #[arg(long = "request-payer", global = true)]
    pub request_payer: Option<String>,

    /// Simulate operations without mutating remote or local storage
    #[arg(long = "dry-run", default_value_t = false, global = true)]
    pub dry_run: bool,

    /// Emit structured newline-delimited JSON (NDJSON) output
    #[arg(long = "json", default_value_t = false, global = true)]
    pub json: bool,

    /// Collect and print execution summary statistics
    #[arg(long = "stat", default_value_t = false, global = true)]
    pub stat: bool,

    /// Log level (trace, debug, info, error)
    #[arg(long = "log", default_value = "info", global = true)]
    pub log: String,

    /// Generate shell completion script (bash, zsh, fish)
    #[arg(long = "install-completion", global = true)]
    pub install_completion: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Copy objects between S3 and local filesystem (or S3 to S3)
    Cp(TransferArgs),

    /// Move objects between S3 and local filesystem (or S3 to S3)
    Mv(TransferArgs),

    /// Synchronize a directory tree or S3 prefix to another
    Sync(SyncArgs),

    /// List buckets or objects
    Ls(LsArgs),

    /// Remove S3 objects (batches up to 1,000 keys per request)
    Rm(RmArgs),

    /// Execute batch commands concurrently from a file or stdin ("Beast Mode")
    Run(RunArgs),

    /// Stream one or more S3 objects to stdout
    Cat(CatArgs),

    /// Stream stdin directly to an S3 object via bounded multipart upload
    Pipe(PipeArgs),

    /// Print bucket or object metadata (HeadBucket / HeadObject)
    Head(HeadArgs),

    /// Show object disk usage and count summary
    Du(DuArgs),

    /// Create an S3 bucket
    Mb {
        /// Bucket URI (s3://bucket)
        target: String,
    },

    /// Remove an S3 bucket
    Rb {
        /// Force delete all objects in the bucket before removing it
        #[arg(long = "force", short = 'f')]
        force: bool,
        /// Bucket URI (s3://bucket)
        target: String,
    },

    /// Generate a presigned GET URL for an S3 object
    Presign {
        /// Expiration duration (e.g. 3600, 1h, 30m)
        #[arg(short = 'e', long = "expires", default_value = "1h")]
        expires: String,
        /// Target S3 object URI (s3://bucket/key)
        target: String,
    },

    /// Execute SQL Select queries on S3 objects (JSON / CSV)
    Select(SelectArgs),

    /// Get or set S3 bucket versioning status
    #[command(name = "bucket-version")]
    BucketVersion {
        /// Set versioning status (Enabled or Suspended)
        #[arg(long = "set")]
        set: Option<String>,
        /// Target S3 bucket URI (s3://bucket)
        target: String,
    },

    /// Print version and build metadata
    Version,
}

#[derive(Args, Debug, Clone)]
pub struct SharedTransferFlags {
    /// Concurrent parts per large file transfer
    #[arg(short = 'c', long = "concurrency", default_value_t = 5)]
    pub concurrency: usize,

    /// Multipart chunk size in MiB (minimum 5 MiB)
    #[arg(short = 'p', long = "part-size", default_value_t = 50)]
    pub part_size: u64,

    /// Flatten source directory hierarchy
    #[arg(short = 'f', long = "flatten", default_value_t = false)]
    pub flatten: bool,

    /// Do not overwrite existing destination object/file
    #[arg(short = 'n', long = "no-clobber", default_value_t = false)]
    pub no_clobber: bool,

    /// Only overwrite if source and destination sizes differ
    #[arg(short = 's', long = "if-size-differ", default_value_t = false)]
    pub if_size_differ: bool,

    /// Only overwrite if source modification time is newer than destination
    #[arg(short = 'u', long = "if-source-newer", default_value_t = false)]
    pub if_source_newer: bool,

    /// Show real-time progress bar
    #[arg(long = "show-progress", visible_alias = "sp", default_value_t = false)]
    pub show_progress: bool,

    /// Exclude objects matching glob pattern (repeatable, takes precedence over --include)
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Include objects matching glob pattern (repeatable)
    #[arg(long = "include")]
    pub include: Vec<String>,

    /// Disable wildcard expansion (treat *, ?, [ literally)
    #[arg(long = "raw", default_value_t = false)]
    pub raw: bool,

    /// Do not follow symlinks when walking local filesystem
    #[arg(long = "no-follow-symlinks", default_value_t = false)]
    pub no_follow_symlinks: bool,

    /// S3 storage class (STANDARD, STANDARD_IA, INTELLIGENT_TIERING, EXPRESS_ONEZONE, etc.)
    #[arg(long = "storage-class")]
    pub storage_class: Option<String>,

    /// Server-side encryption mode (AES256, aws:kms, aws:kms:dsse)
    #[arg(long = "sse")]
    pub sse: Option<String>,

    /// KMS key ID for server-side encryption
    #[arg(long = "sse-kms-key-id")]
    pub sse_kms_key_id: Option<String>,

    /// Canned ACL (private, public-read, bucket-owner-full-control, etc.)
    #[arg(long = "acl")]
    pub acl: Option<String>,

    /// Cache-Control header
    #[arg(long = "cache-control")]
    pub cache_control: Option<String>,

    /// Expires header
    #[arg(long = "expires")]
    pub expires: Option<String>,

    /// Content-Type header
    #[arg(long = "content-type")]
    pub content_type: Option<String>,

    /// Content-Encoding header
    #[arg(long = "content-encoding")]
    pub content_encoding: Option<String>,

    /// Content-Disposition header
    #[arg(long = "content-disposition")]
    pub content_disposition: Option<String>,

    /// User metadata Key=Value (repeatable)
    #[arg(long = "metadata")]
    pub metadata: Vec<String>,

    /// Metadata directive for S3 copy (COPY or REPLACE)
    #[arg(long = "metadata-directive")]
    pub metadata_directive: Option<String>,

    /// Source region override
    #[arg(long = "source-region")]
    pub source_region: Option<String>,

    /// Destination region override
    #[arg(long = "destination-region")]
    pub destination_region: Option<String>,

    /// Specific object version ID
    #[arg(long = "version-id")]
    pub version_id: Option<String>,
}

impl SharedTransferFlags {
    pub fn to_copy_options(&self, delete_source: bool) -> CopyOptions {
        let mut meta_map = HashMap::new();
        for kv in &self.metadata {
            if let Some((k, v)) = kv.split_once('=') {
                meta_map.insert(k.trim().to_string(), v.trim().to_string());
            }
        }

        CopyOptions {
            concurrency: self.concurrency.max(1),
            part_size_bytes: self.part_size.max(5) * 1024 * 1024,
            flatten: self.flatten,
            no_clobber: self.no_clobber,
            if_size_differ: self.if_size_differ,
            if_source_newer: self.if_source_newer,
            raw: self.raw,
            no_follow_symlinks: self.no_follow_symlinks,
            includes: self.include.clone(),
            excludes: self.exclude.clone(),
            version_id: self.version_id.clone(),
            source_region: self.source_region.clone(),
            destination_region: self.destination_region.clone(),
            put_opts: PutObjectOptions {
                storage_class: self.storage_class.clone(),
                acl: self.acl.clone(),
                sse: self.sse.clone(),
                sse_kms_key_id: self.sse_kms_key_id.clone(),
                cache_control: self.cache_control.clone(),
                expires: self.expires.clone(),
                content_type: self.content_type.clone(),
                content_encoding: self.content_encoding.clone(),
                content_disposition: self.content_disposition.clone(),
                metadata: meta_map,
                metadata_directive: self.metadata_directive.clone(),
                checksum_crc32c: false,
            },
            delete_source,
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct TransferArgs {
    #[command(flatten)]
    pub flags: SharedTransferFlags,
    /// Source path or S3 URI
    pub src: String,
    /// Destination path or S3 URI
    pub dst: String,
}

#[derive(Args, Debug, Clone)]
pub struct SyncArgs {
    /// Delete destination objects that do not exist in source (respects --include/--exclude)
    #[arg(long = "delete", default_value_t = false)]
    pub delete: bool,

    /// Compare object size only, ignoring modification timestamps
    #[arg(long = "size-only", default_value_t = false)]
    pub size_only: bool,

    /// Require exact timestamp match instead of source-newer
    #[arg(long = "exact-timestamps", default_value_t = false)]
    pub exact_timestamps: bool,

    #[command(flatten)]
    pub flags: SharedTransferFlags,

    /// Source path or S3 URI
    pub src: String,
    /// Destination path or S3 URI
    pub dst: String,
}

#[derive(Args, Debug, Clone)]
pub struct LsArgs {
    /// Display object ETag column
    #[arg(short = 'e', long = "etag", default_value_t = false)]
    pub etag: bool,

    /// Human-readable object sizes
    #[arg(short = 'H', long = "humanize", default_value_t = false)]
    pub humanize: bool,

    /// Show full s3://bucket/key path for objects
    #[arg(short = 's', long = "show-fullpath", default_value_t = false)]
    pub show_fullpath: bool,

    /// List all object versions
    #[arg(long = "all-versions", default_value_t = false)]
    pub all_versions: bool,

    /// Optional S3 URI or local path
    pub target: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct RmArgs {
    /// Disable wildcard expansion
    #[arg(long = "raw", default_value_t = false)]
    pub raw: bool,

    /// Exclude objects matching glob pattern
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Include objects matching glob pattern
    #[arg(long = "include")]
    pub include: Vec<String>,

    /// Delete a specific object version
    #[arg(long = "version-id")]
    pub version_id: Option<String>,

    /// Delete all versions of matched objects
    #[arg(long = "all-versions", default_value_t = false)]
    pub all_versions: bool,

    /// Target S3 URIs (one or more)
    #[arg(required = true)]
    pub targets: Vec<String>,
}

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    /// Optional batch file path (reads from stdin if omitted or '-')
    pub file: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct CatArgs {
    /// Specific object version ID
    #[arg(long = "version-id")]
    pub version_id: Option<String>,

    /// Target S3 object URIs
    #[arg(required = true)]
    pub targets: Vec<String>,
}

#[derive(Args, Debug, Clone)]
pub struct PipeArgs {
    #[command(flatten)]
    pub flags: SharedTransferFlags,

    /// Destination S3 URI (s3://bucket/key)
    pub target: String,
}

#[derive(Args, Debug, Clone)]
pub struct HeadArgs {
    /// Specific object version ID
    #[arg(long = "version-id")]
    pub version_id: Option<String>,

    /// Target S3 URI (s3://bucket or s3://bucket/key)
    pub target: String,
}

#[derive(Args, Debug, Clone)]
pub struct DuArgs {
    /// Group disk usage by S3 storage class
    #[arg(short = 'g', long = "group", default_value_t = false)]
    pub group: bool,

    /// Human-readable byte counts
    #[arg(short = 'H', long = "humanize", default_value_t = false)]
    pub humanize: bool,

    /// Exclude objects matching glob pattern
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Include objects matching glob pattern
    #[arg(long = "include")]
    pub include: Vec<String>,

    /// Include all object versions
    #[arg(long = "all-versions", default_value_t = false)]
    pub all_versions: bool,

    /// Target S3 URI
    pub target: String,
}

#[derive(Args, Debug, Clone)]
pub struct SelectArgs {
    /// SQL query expression (e.g. "SELECT * FROM S3Object s")
    #[arg(short = 'q', long = "query")]
    pub query: String,

    /// Input format (json or csv)
    #[arg(long = "input-format", default_value = "json")]
    pub input_format: String,

    /// Output format (json or csv)
    #[arg(long = "output-format", default_value = "json")]
    pub output_format: String,

    /// Exclude objects matching glob pattern
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Include objects matching glob pattern
    #[arg(long = "include")]
    pub include: Vec<String>,

    /// Target S3 URI (supports wildcards)
    pub target: String,
}

#[derive(Parser, Debug)]
struct BatchSubcommandParser {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Some(ref shell) = cli.install_completion {
        print_completion_script(shell);
        return ExitCode::SUCCESS;
    }

    let Some(ref cmd) = cli.command else {
        eprintln!("Error: subcommand required (try `t3 --help`)");
        return ExitCode::FAILURE;
    };

    let show_progress = match cmd {
        Commands::Cp(a) | Commands::Mv(a) => a.flags.show_progress,
        Commands::Sync(a) => a.flags.show_progress,
        Commands::Pipe(a) => a.flags.show_progress,
        _ => false,
    };

    let output = OutputContext::new(
        cli.json,
        cli.stat,
        cli.dry_run,
        show_progress,
        LogLevel::parse(&cli.log),
    );

    let overrides = ConfigOverrides {
        endpoint_url: cli.endpoint_url.clone(),
        profile: cli.profile.clone(),
        credentials_file: cli.credentials_file.clone(),
        region: None,
        no_sign_request: cli.no_sign_request,
        no_verify_ssl: cli.no_verify_ssl,
        use_list_objects_v1: cli.use_list_objects_v1,
        request_payer: cli.request_payer.clone(),
        retry_count: cli.retry_count,
        unsigned_payload: true,
    };

    let resolved = ResolvedConfig::resolve(&overrides);
    let s3 = match S3Client::new(resolved, Some(output.stats.clone())) {
        Ok(c) => c,
        Err(e) => {
            output.emit_error("init", "s3-client", &e.to_string());
            return ExitCode::FAILURE;
        }
    };

    let engine = T3Engine::new(s3, output.clone(), cli.numworkers);
    let res = execute_command(&engine, cmd).await;

    output.print_stats();

    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            output.emit_error("t3", "", &e.to_string());
            ExitCode::FAILURE
        }
    }
}

async fn execute_command(engine: &T3Engine, cmd: &Commands) -> Result<()> {
    match cmd {
        Commands::Run(args) => execute_run_batch(engine, args.file.as_deref()).await,
        other => execute_single_command(engine, other).await,
    }
}

async fn execute_single_command(engine: &T3Engine, cmd: &Commands) -> Result<()> {
    match cmd {
        Commands::Cp(args) => {
            let opts = args.flags.to_copy_options(false);
            engine.cp(&args.src, &args.dst, &opts).await
        }
        Commands::Mv(args) => {
            let opts = args.flags.to_copy_options(true);
            engine.cp(&args.src, &args.dst, &opts).await
        }
        Commands::Sync(args) => {
            let opts = SyncOptions {
                copy_opts: args.flags.to_copy_options(false),
                delete: args.delete,
                size_only: args.size_only,
                exact_timestamps: args.exact_timestamps,
            };
            engine.sync(&args.src, &args.dst, &opts).await
        }
        Commands::Ls(args) => {
            let opts = LsOptions {
                etag: args.etag,
                humanize: args.humanize,
                show_fullpath: args.show_fullpath,
                all_versions: args.all_versions,
            };
            engine.ls(args.target.as_deref(), &opts).await
        }
        Commands::Rm(args) => {
            let opts = RmOptions {
                raw: args.raw,
                includes: args.include.clone(),
                excludes: args.exclude.clone(),
                version_id: args.version_id.clone(),
                all_versions: args.all_versions,
            };
            engine.rm(&args.targets, &opts).await
        }
        Commands::Run(_) => bail!("Nested `run` command inside `run` batch is not allowed"),
        Commands::Cat(args) => engine.cat(&args.targets, args.version_id.as_deref()).await,
        Commands::Pipe(args) => {
            let opts = args.flags.to_copy_options(false);
            engine.pipe(&args.target, &opts).await
        }
        Commands::Head(args) => engine.head(&args.target, args.version_id.as_deref()).await,
        Commands::Du(args) => {
            let opts = DuOptions {
                group_by_class: args.group,
                humanize: args.humanize,
                includes: args.include.clone(),
                excludes: args.exclude.clone(),
                all_versions: args.all_versions,
            };
            engine.du(&args.target, &opts).await
        }
        Commands::Mb { target } => engine.mb(target).await,
        Commands::Rb { force, target } => engine.rb(target, *force).await,
        Commands::Presign { expires, target } => {
            let secs = parse_duration_secs(expires)?;
            engine.presign(target, secs)?;
            Ok(())
        }
        Commands::Select(args) => {
            engine
                .select(
                    &args.target,
                    &args.query,
                    &args.input_format,
                    &args.output_format,
                    &args.include,
                    &args.exclude,
                )
                .await
        }
        Commands::BucketVersion { set, target } => {
            engine.bucket_version(target, set.as_deref()).await
        }
        Commands::Version => {
            if engine.output.json_mode {
                println!(
                    "{}",
                    serde_json::json!({
                        "name": "t3",
                        "version": env!("CARGO_PKG_VERSION"),
                        "compatibility": "s5cmd v2.3.0+",
                        "engine": "ruststack-t3",
                    })
                );
            } else {
                println!(
                    "t3 v{} (Turbo 3 — High-Performance Rust S3 Client, s5cmd drop-in compatible)",
                    env!("CARGO_PKG_VERSION")
                );
            }
            Ok(())
        }
    }
}

async fn execute_run_batch(engine: &T3Engine, file: Option<&str>) -> Result<()> {
    let content = match file {
        Some(path) if path != "-" => fs::read_to_string(path)?,
        _ => {
            let stdin = io::stdin();
            let mut buf = String::new();
            for line in stdin.lock().lines() {
                buf.push_str(&line?);
                buf.push('\n');
            }
            buf
        }
    };

    let mut set = JoinSet::new();
    for raw_line in content.lines() {
        let Some(tokens) = parse_run_line(raw_line) else {
            continue;
        };
        let mut argv = Vec::with_capacity(tokens.len() + 1);
        argv.push("t3".to_string());
        argv.extend(tokens);

        let parsed = BatchSubcommandParser::try_parse_from(&argv)?;
        let eng = engine.clone();
        set.spawn(async move { execute_single_command(&eng, &parsed.command).await });
    }

    let mut errors = 0usize;
    let mut first_err = None;
    while let Some(res) = set.join_next().await {
        match res? {
            Ok(()) => {}
            Err(e) => {
                errors += 1;
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }

    if let Some(e) = first_err {
        bail!("run batch finished with {errors} error(s): {e}");
    }
    Ok(())
}

fn print_completion_script(shell: &str) {
    println!(
        "# Shell completion for t3 ({shell})\ncomplete -W \"cp mv sync ls rm run cat pipe head du mb rb presign select bucket-version version\" t3 s5cmd"
    );
}
