use criterion::{black_box, criterion_group, criterion_main, Criterion};
use t3_core::{split_wildcard, GlobFilter, SelectQuery};
use t3_storage::sanitize_relative_path;

fn bench_t3_core(c: &mut Criterion) {
    let filter = GlobFilter::compile(
        Some("logs/2026/*/*.parquet"),
        &["*.parquet".to_string()],
        &["*secret*".to_string()],
    )
    .unwrap();

    c.bench_function("glob_filter_dfa", |b| {
        b.iter(|| {
            black_box(filter.matches(
                black_box("logs/2026/11/part-0001.parquet"),
                black_box("11/part-0001.parquet"),
            ))
        })
    });

    c.bench_function("wildcard_split_and_path_guard", |b| {
        b.iter(|| {
            let _ = black_box(split_wildcard(black_box("logs/2026/*/*.parquet"), false));
            let _ = black_box(sanitize_relative_path(black_box(
                "logs/2026/11/part-0001.parquet",
            )));
        })
    });

    let q = SelectQuery::parse("SELECT s.name, s.val FROM S3Object s WHERE s.val > 10");
    let data = br#"{"name":"a","val":20}
{"name":"b","val":5}"#;
    c.bench_function("select_query_ndjson", |b| {
        b.iter(|| black_box(q.execute_on_bytes(black_box(data), "json", "json")))
    });
}

criterion_group!(benches, bench_t3_core);
criterion_main!(benches);
