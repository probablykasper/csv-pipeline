//! CSV processing library inspired by [csvsc](https://crates.io/crates/csvsc)
//!
//! ## Get started
//!
//! The first thing you need is to create a [`Pipeline`]. This can be done by calling [`Pipeline::from_reader`] with a [`csv::Reader`], or [`Pipeline::from_path`] with a path.
//!
//! Once you have a pipeline, there are various methods available which let you add your desired processing steps. Check the [`Pipeline`] for more details and examples.
//!
//! In the end, you may want to write the result somewhere. To do that, you can [flush](Pipeline::flush) into a [`Target`].
//!
//! Finally, you probably want to run the pipeline. There are a few options:
//! - [`Pipeline::build`] gives you a [`PipelineIter`] which you can iterate through
//! - [`Pipeline::run`] runs through the pipeline until it finds an error, or the end
//! - [`Pipeline::collect_into_string`] runs the pipeline and returns the csv as a `Result<String, Error>`. Can be a convenient alternative to flushing to a [`StringTarget`](target::StringTarget).
//!
//! ## Basic Example
//!
//! ```
//! use csv_pipeline::{Pipeline, Transformer};
//!
//! // First create a pipeline from a CSV file path
//! let csv = Pipeline::from_path("test/Countries.csv")
//!   .unwrap()
//!   // Add a column with values computed from a closure
//!   .add_col("Language", |headers, row| {
//!     match headers.get_field(row, "Country") {
//!       Some("Norway") => Ok("Norwegian".into()),
//!       _ => Ok("Unknown".into()),
//!     }
//!   })
//!   // Make the "Country" column uppercase
//!   .rename_col("Country", "COUNTRY")
//!   .map_col("COUNTRY", |id_str| Ok(id_str.to_uppercase()))
//!   // Collect the csv into a string
//!   .collect_into_string()
//!   .unwrap();
//!
//! assert_eq!(
//!   csv,
//!   "ID,COUNTRY,Language\n\
//!     1,NORWAY,Norwegian\n\
//!     2,TUVALU,Unknown\n"
//! );
//! ```
//!
//! ## Transform Example
//! ```
//! use csv_pipeline::{Pipeline, Transformer};
//!
//! let source = "\
//!   Person,Score\n\
//!   A,1\n\
//!   A,8\n\
//!   B,3\n\
//!   B,4\n";
//! let reader = csv::Reader::from_reader(source.as_bytes());
//! let csv = Pipeline::from_reader(reader)
//!   .unwrap()
//!   .map(|_headers, row| Ok(row))
//!   // Transform into a new csv
//!   .transform_into(|| {
//!     vec![
//!       // Keep every Person
//!       Transformer::new("Person").keep_unique(),
//!       // Sum the scores into a "Total score" column
//!       Transformer::new("Total score").from_col("Score").sum(0),
//!     ]
//!   })
//!   .collect_into_string()
//!   .unwrap();
//!
//! assert_eq!(
//!   csv,
//!   "Person,Total score\n\
//!     A,9\n\
//!     B,7\n"
//! );
//! ```
//!

use std::path::PathBuf;

mod headers;
mod pipeline;
mod pipeline_iterators;
mod transform;

pub use headers::Headers;
pub use pipeline::{Pipeline, PipelineIter};
pub use transform::{Transform, Transformer};

pub mod target;
/// Helper for building a target to flush data into
pub struct Target {}
impl Target {
	pub fn path<P: Into<PathBuf>>(path: P) -> target::PathTarget {
		target::PathTarget::new(path)
	}
	pub fn stdout() -> target::StdoutTarget {
		target::StdoutTarget::new()
	}
	pub fn stderr() -> target::StderrTarget {
		target::StderrTarget::new()
	}
	pub fn string<'a>(s: &'a mut String) -> target::StringTarget {
		target::StringTarget::new(s)
	}
}

/// Alias of [`csv::StringRecord`]
pub type Row = csv::StringRecord;
/// Alias of `Result<Row, Error>`
pub type RowResult = Result<Row, PlError>;

/// Error originating from the specified pipeline source index
#[derive(Debug)]
pub struct PlError {
	pub error: Error,
	pub source: usize,
}

#[derive(Debug)]
pub enum Error {
	/// CSV and IO errors are in here.
	Csv(csv::Error),
	/// The column of this name is missing.
	MissingColumn(String),
	/// This column name appears twice.
	DuplicateColumn(String),
	/// This field has an invalid format.
	InvalidField(String),
	/// Two pipeline sources don't have the same headers.
	MismatchedHeaders(Row, Row),
}
impl Error {
	pub fn at_source(self, source: usize) -> PlError {
		PlError {
			error: self,
			source,
		}
	}
}
