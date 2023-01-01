use super::chain::{BoxedIterator, Chain};
use super::headers::Headers;
use crate::{Error, Row, RowResult};
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::Path;

pub struct PipelineBuilder {
	pub headers: Headers,
	chain: Chain,
}

impl PipelineBuilder {
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
			chain: Chain::new(Box::new(records)),
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
	/// use csv_pipeline::PipelineBuilder;
	///
	/// PipelineBuilder::from_path("test/Countries.csv")
	///   .add_col("Language", |headers, row| {
	///     Ok("".to_string())
	///   });
	/// ```
	pub fn add_col<F>(mut self, name: &str, get_value: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<String, Error>,
	{
		self.headers.push_field(name);

		struct State<F> {
			get_value: F,
			headers: Headers,
		}
		let stateful_chain = self.chain.with_state(State {
			get_value,
			headers: self.headers.clone(),
		});
		let new_chain = stateful_chain.map(|row_result, state| {
			println!("ADDCOL-map");
			let mut row = row_result?;
			let value = (state.get_value)(&state.headers, &row)?;
			row.push_field(&value);
			Ok(row)
		});

		self.chain = new_chain;

		self
	}

	pub fn build(self) -> Pipeline {
		Pipeline {
			headers: self.headers,
			iterator: Box::new(self.chain),
		}
	}
}

pub struct Pipeline {
	pub headers: Headers,
	pub iterator: BoxedIterator,
}
impl Iterator for Pipeline {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.iterator.next()
	}
}

#[cfg(test)]
mod tests {
	use crate::PipelineBuilder;

	#[test]
	fn add_col() {
		let mut pipeline = PipelineBuilder::from_path("test/Countries.csv")
			.add_col("Language", |_headers, _row| Ok("".to_string()))
			.build();

		let mut writer = csv::Writer::from_writer(vec![]);
		writer.write_record(&pipeline.headers).unwrap();
		println!("{:?}", pipeline.headers.get_row());
		while let Some(item) = pipeline.next() {
			println!("{:?}", item.clone().unwrap());
			writer.write_record(&item.unwrap()).unwrap();
		}
		let s = String::from_utf8(writer.into_inner().unwrap()).unwrap();
		print!("{}", s);
	}
}
