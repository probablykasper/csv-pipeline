use super::headers::Headers;
use crate::pipeline_iterators::{AddCol, Flush, MapCol, MapRow, TransformInto};
use crate::target::Target;
use crate::transform::Transform;
use crate::{Error, Row, RowResult, StringTarget};
use csv::{Reader, ReaderBuilder, StringRecordsIntoIter};
use std::borrow::BorrowMut;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub struct Pipeline<'a> {
	pub headers: Headers,
	iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}

impl<'a> Pipeline<'a> {
	pub fn from_reader<R: io::Read + 'a>(mut reader: Reader<R>) -> Result<Self, Error> {
		let headers_row = reader.headers().unwrap().clone();
		let row_iterator = RowIter::from_records(reader.into_records());
		Ok(Pipeline {
			headers: Headers::from_row(headers_row)?,
			iterator: Box::new(row_iterator),
		})
	}

	/// Create a pipeline from a CSV or TSV file.
	pub fn from_path<P: AsRef<Path>>(file_path: P) -> Result<Self, Error> {
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
	/// Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .add_col("C", |headers, row| {
	///     Ok("1")
	///   });
	/// ```
	pub fn add_col<F>(mut self, name: &str, get_value: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<&'a str, Error> + 'a,
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
	/// let csv = Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .map(|headers, row| {
	///     Ok(row.into_iter().map(|field| field.to_string() + "0").collect())
	///   })
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "A,B\n10,20\n"
	/// );
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
	/// let csv = Pipeline::from_path("test/Countries.csv")
	///   .unwrap()
	///   .map_col("Country", |field| Ok(field.to_uppercase()))
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(
	///   csv,
	///   "ID,Country\n\
	///     1,NORWAY\n\
	///     2,TUVALU\n"
	/// );
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

	/// Panics if a new name already exists
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::{Pipeline, StringTarget};
	///
	/// let csv = Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .rename_col("A", "X")
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "X,B\n1,2\n");
	/// ```
	pub fn rename_col(mut self, from: &str, to: &str) -> Self {
		match self.headers.rename(from, to) {
			Ok(()) => (),
			Err(e) => panic!("{:?}", e),
		};
		self
	}

	/// Panics if a new name already exists
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::{Pipeline, StringTarget};
	///
	/// let csv = Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .rename_cols(|i, name| {
	///     match name {
	///       "A" => "X",
	///       name => name,
	///     }
	///   })
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "X,B\n1,2\n");
	/// ```
	pub fn rename_cols<R>(mut self, mut get_name: R) -> Self
	where
		R: FnMut(usize, &str) -> &str,
	{
		let mut new_headers = Headers::new();
		for (i, name) in self.headers.into_iter().enumerate().borrow_mut() {
			let new_name = get_name(i, name);
			match new_headers.push_field(new_name) {
				true => (),
				false => panic!("New column name already exists"),
			}
		}
		self.headers = new_headers;
		self
	}

	/// Group and reduce rows
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::{Pipeline, StringTarget};
	///
	/// let csv = Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .rename_cols(|i, name| {
	///     match name {
	///       "A" => "X",
	///       name => name,
	///     }
	///   })
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "X,B\n1,2\n");
	/// ```
	pub fn transform_into<T>(mut self, mut get_transformers: T) -> Self
	where
		T: FnMut() -> Vec<Box<dyn Transform>> + 'a,
	{
		self.iterator = Box::new(TransformInto {
			iterator: self.iterator,
			groups: BTreeMap::new(),
			hashers: get_transformers(),
			get_transformers,
			headers: self.headers.clone(),
		});
		self
	}

	/// Write to the specified [`Target`].
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::{Pipeline, StringTarget};
	///
	/// let mut csv = String::new();
	/// Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .flush(StringTarget::new(&mut csv))
	///   .run()
	///   .unwrap();
	///
	/// assert_eq!(csv, "A,B\n1,2\n");
	/// ```
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

	/// Shorthand for `.build().run()`.
	pub fn run(self) -> Result<(), Error> {
		self.build().run()
	}

	pub fn collect_into_string(self) -> Result<String, Error> {
		let mut csv = String::new();
		self.flush(StringTarget::new(&mut csv)).run()?;
		Ok(csv)
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

	/// Run through the whole iterator. Returns the first error found, if any
	pub fn run(&mut self) -> Result<(), Error> {
		while let Some(item) = self.next() {
			item?;
		}
		Ok(())
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
