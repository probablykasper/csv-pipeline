use super::headers::Headers;
use crate::pipeline_iterators::{AddCol, Flush, MapCol, MapRow};
use crate::target::Target;
use crate::{Error, Row, RowResult};
use csv::{Reader, ReaderBuilder, StringRecordsIntoIter};
use std::fs::File;
use std::io;
use std::path::Path;

pub struct Pipeline<'a> {
	pub headers: Headers,
	iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}

impl<'a> Pipeline<'a> {
	pub fn from_reader(mut reader: Reader<File>) -> Self {
		let headers_row = reader.headers().unwrap().clone();
		let row_iterator = RowIter::from_records(reader.into_records());
		Pipeline {
			headers: Headers::from(headers_row),
			iterator: Box::new(row_iterator),
		}
	}

	/// Create a pipeline from a CSV or TSV file.
	pub fn from_path<P: AsRef<Path>>(file_path: P) -> Self {
		let ext = file_path.as_ref().extension().unwrap_or_default();
		let delimiter = match ext.to_string_lossy().as_ref() {
			"tsv" => b'\t',
			"csv" => b',',
			_ => panic!("Unsupported file {}", file_path.as_ref().display()),
		};
		let reader = ReaderBuilder::new()
			.delimiter(delimiter)
			.from_path(file_path)
			.unwrap();
		Self::from_reader(reader)
	}

	/// Adds a column with values computed from the closure for each row.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// Pipeline::from_path("test/Countries.csv")
	///   .add_col("Language", |headers, row| {
	///     Ok("".to_string())
	///   });
	/// ```
	pub fn add_col<F>(mut self, name: &str, get_value: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<String, Error> + 'a,
	{
		self.headers.push_field(name);
		self.iterator = Box::new(AddCol {
			iterator: self.iterator,
			f: get_value,
			headers: self.headers.clone(),
		});
		self
	}

	/// Maps each row.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// Pipeline::from_path("test/Countries.csv")
	///   .map(|headers, row| {
	///     Ok(row.into_iter().map(|field| field.to_uppercase()).collect())
	///   });
	/// ```
	pub fn map<F>(mut self, get_row: F) -> Self
	where
		F: FnMut(&Headers, Row) -> Result<Row, Error> + 'a,
	{
		self.iterator = Box::new(MapRow {
			iterator: self.iterator,
			f: get_row,
			headers: self.headers.clone(),
		});
		self
	}

	/// Maps each field of a column.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// Pipeline::from_path("test/Countries.csv")
	///   .map_col("Country", |field| {
	/// 	  Ok(field.to_uppercase())
	///   });
	/// ```
	pub fn map_col<F>(mut self, col: &str, get_value: F) -> Self
	where
		F: FnMut(&str) -> Result<String, Error> + 'a,
	{
		self.iterator = Box::new(MapCol {
			iterator: self.iterator,
			f: get_value,
			name: col.to_string(),
			index: self.headers.get_index(col),
		});
		self
	}

	pub fn flush(mut self, target: impl Target + 'a) -> Self {
		let flush = Flush::new(self.iterator, target, self.headers.clone());
		self.iterator = Box::new(flush);
		self
	}

	/// Turn the pipeline into an iterator.
	/// You can also do this using `pipeline.into_iter()`.
	pub fn build(self) -> PipelineIter<'a> {
		PipelineIter {
			headers: self.headers,
			iterator: Box::new(self.iterator),
		}
	}
}
impl<'a> IntoIterator for Pipeline<'a> {
	type Item = RowResult;
	type IntoIter = PipelineIter<'a>;

	fn into_iter(self) -> Self::IntoIter {
		self.build()
	}
}

pub struct PipelineIter<'a> {
	pub headers: Headers,
	pub iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}

impl<'a> PipelineIter<'a> {
	/// Advances the iterator until an error is found.
	///
	/// Returns `None` when the iterator is finished.
	pub fn next_error(&mut self) -> Option<Error> {
		while let Some(item) = self.next() {
			if let Err(err) = item {
				return Some(err);
			}
		}
		None
	}
}
impl<'a> Iterator for PipelineIter<'a> {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.iterator.next()
	}
}

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
				return Error::from(err);
			})
		})
	}
}
