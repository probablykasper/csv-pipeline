mod chain;
mod headers;
mod pipeline;

pub use headers::Headers;
pub use pipeline::{Pipeline, PipelineBuilder};

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	DuplicatedColumn(String),
	Csv,
}

pub type Row = csv::StringRecord;
pub type RowResult = Result<Row, Error>;
