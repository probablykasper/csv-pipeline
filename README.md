# CSV Pipeline

CSV processing library inspired by [csvsc](https://crates.io/crates/csvsc)

[![Crates.io](https://img.shields.io/crates/v/csv-pipeline.svg)](https://crates.io/crates/csv-pipeline)
[![Documentation](https://docs.rs/csv-pipeline/badge.svg)](https://docs.rs/csv-pipeline)

## Basic Example
```rs
use csv_pipeline::{Pipeline, Transformer};

// First create a pipeline from a CSV file path
let csv = Pipeline::from_path("test/Countries.csv")
  .unwrap()
  // Add a column with values computed from a closure
  .add_col("Language", |headers, row| {
    match headers.get_field(row, "Country") {
      Some("Norway") => Ok("Norwegian".into()),
      _ => Ok("Unknown".into()),
    }
  })
  // Make the "Country" column uppercase
  .rename_col("Country", "COUNTRY")
  .map_col("COUNTRY", |id_str| Ok(id_str.to_uppercase()))
  // Collect the csv into a string
  .collect_into_string()
  .unwrap();

assert_eq!(
  csv,
  "ID,COUNTRY,Language\n\
    1,NORWAY,Norwegian\n\
    2,TUVALU,Unknown\n"
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
