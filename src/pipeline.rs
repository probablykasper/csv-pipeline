use super::headers::Headers;
use super::pipe::{Pipe, PipeIterator};
use crate::{Error, Row, RowResult};
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::Path;

pub struct Pipeline {
	pub headers: Headers,
	pipe: PipeIterator,
}

impl Pipeline {
	pub fn from_reader(mut reader: Reader<File>) -> Self {
		let headers_row = reader.headers().unwrap().clone();
		let records = reader.into_records().map(|r| {
			let row_result: RowResult = match r {
				Ok(row) => Ok(row),
				Err(err) => Err(Error::Csv),
			};
			row_result
		});
		Self {
			headers: Headers::from(headers_row),
			pipe: Box::new(records),
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
		F: FnMut(&Headers, &Row) -> Result<String, Error>,
	{
		self.headers.add(name);

		struct State<F> {
			get_value: F,
			headers: Headers,
		}
		let pipe = Pipe::new(self.pipe).with_state(State {
			get_value,
			headers: self.headers.clone(),
		});
		let newpipe = pipe.map(|row_result, state| {
			let mut row = row_result?;
			let value = (state.get_value)(&state.headers, &row)?;
			row.push_field(&value);
			Ok(row)
		});

		self.pipe = Box::new(newpipe.iterator);

		self
	}
}

#[cfg(test)]
mod tests {
	use crate::Pipeline;

	#[test]
	fn add_col() {
		let mut pipeline = Pipeline::from_path("test/Countries.csv")
			.add_col("Language", |_headers, _row| Ok("".to_string()));
	}
}
