pub mod headers;
pub mod pipe;
pub mod pipeline;

pub use headers::Headers;
pub use pipe::Pipe;
pub use pipeline::Pipeline;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	DuplicatedColumn(String),
	Csv,
}

pub type Row = csv::StringRecord;
pub type RowResult = Result<Row, Error>;
