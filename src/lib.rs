mod chain;
mod headers;
mod pipeline;

use std::io;

use csv::StringRecordsIntoIter;
pub use headers::Headers;
pub use pipeline::{Pipeline, PipelineBuilder};

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	DuplicatedColumn(String),
	Csv,
}

pub type Row = csv::StringRecord;
pub type RowResult = Result<Row, Error>;

pub struct RowIter<R: io::Read> {
	inner: StringRecordsIntoIter<R>,
}
impl<R: io::Read> RowIter<R> {
	pub fn from_records(records: StringRecordsIntoIter<R>) -> Self {
		RowIter { inner: records }
	}
}
impl<R: io::Read> Iterator for RowIter<R> {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|result| {
			result.map_err(|err| {
				return Error::Csv;
			})
		})
	}
}
