# CSV Pipeline

CSV processing library inspired by [csvsc](https://crates.io/crates/csvsc)

[![Crates.io](https://img.shields.io/crates/v/csv-pipeline.svg)](https://crates.io/crates/csv-pipeline)
[![Documentation](https://docs.rs/csv-pipeline/badge.svg)](https://docs.rs/csv-pipeline)

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
