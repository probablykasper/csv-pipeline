use super::headers::Headers;
use crate::{Error, Row, RowIter, RowResult};
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::Path;

pub struct PipelineBuilder<'a> {
	pub headers: Headers,
	iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}

impl<'a> PipelineBuilder<'a> {
	pub fn from_reader(mut reader: Reader<File>) -> Self {
		let headers_row = reader.headers().unwrap().clone();
		let row_iterator = RowIter::from_records(reader.into_records());
		PipelineBuilder {
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
	/// use csv_pipeline::PipelineBuilder;
	///
	/// PipelineBuilder::from_path("test/Countries.csv")
	///   .add_col("Language", |headers, row| {
	///     Ok("".to_string())
	///   });
	/// ```
	pub fn add_col<F>(mut self, name: &str, get_value: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<String, Error> + 'a,
	{
		self.headers.push_field(name);

		let add_col = AddCol {
			iterator: self.iterator,
			f: get_value,
			headers: self.headers.clone(),
		};

		self.iterator = Box::new(add_col);

		self
	}

	pub fn build(self) -> Pipeline<'a> {
		Pipeline {
			headers: self.headers,
			iterator: Box::new(self.iterator),
		}
	}
}

struct AddCol<I, F: FnMut(&Headers, &Row) -> Result<String, Error>> {
	iterator: I,
	f: F,
	headers: Headers,
}
impl<I, F> Iterator for AddCol<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&Headers, &Row) -> Result<String, Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let row = match self.iterator.next() {
			Some(Ok(row)) => row,
			Some(Err(e)) => return Some(Err(e)),
			None => return None,
		};
		match (self.f)(&self.headers, &row) {
			Ok(value) => {
				let mut row = row;
				row.push_field(&value);
				Some(Ok(row))
			}
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct Pipeline<'a> {
	pub headers: Headers,
	pub iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}
impl<'a> Iterator for Pipeline<'a> {
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
			.add_col("Language", |_headers, row| match row.get(1) {
				Some("Norway") => Ok("Norwegian".to_string()),
				_ => Ok("Unknown".to_string()),
			})
			.build();

		let mut writer = csv::Writer::from_writer(vec![]);
		writer.write_record(&pipeline.headers).unwrap();
		while let Some(item) = pipeline.next() {
			writer.write_record(&item.unwrap()).unwrap();
		}
		let csv_str = String::from_utf8(writer.into_inner().unwrap()).unwrap();

		assert_eq!(
			csv_str,
			"ID,Country,Language\n\
			1,Norway,Norwegian\n\
			2,Tuvalu,Unknown\n"
		);
	}
}
