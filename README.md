# CSV Pipeline

CSV processing library inspired by [csvsc](https://crates.io/crates/csvsc)

[![Crates.io](https://img.shields.io/crates/v/csv-pipeline.svg)](https://crates.io/crates/csv-pipeline)
[![Documentation](https://docs.rs/csv-pipeline/badge.svg)](https://docs.rs/csv-pipeline)

## Example

```rs
use csv_pipeline::{Pipeline, Transformer};

let source = "\
  Person,Score\n\
  A,1\n\
  A,8\n\
  B,3\n\
  B,4\n";
let reader = csv::Reader::from_reader(source.as_bytes());
let csv = Pipeline::from_reader(reader)
  .unwrap()
  .map(|_headers, row| Ok(row))
  // Transform into a new csv
  .transform_into(|| {
    vec![
      // Keep every Person
      Transformer::new("Person").keep_unique(),
      // Sum the scores into a "Total score" column
      Transformer::new("Total score").from_col("Score").sum(0),
    ]
  })
  .collect_into_string()
  .unwrap();

assert_eq!(
  csv,
  "Person,Total score\n\
    A,9\n\
    B,7\n"
);
```

## Dev Instructions

### Get started

Install [Rust](https://www.rust-lang.org).

Run tests:
```
cargo test
```

### Releasing a new version

1. Update `CHANGELOG.md`
2. Bump the version number in `Cargo.toml`
3. Run `cargo test`
4. Run `cargo publish`
5. Create a git tag in format `v#.#.#`
6. Create GitHub release with release notes
