use super::headers::Headers;
use crate::pipeline_iterators::{
	AddCol, Filter, FilterCol, Flush, MapCol, MapRow, PipelinesChain, Select, TransformInto,
	Validate, ValidateCol,
};
use crate::target::{StringTarget, Target};
use crate::transform::Transform;
use crate::{Error, PlError, Row, RowResult};
use csv::{Reader, ReaderBuilder, StringRecordsIntoIter};
use linked_hash_map::LinkedHashMap;
use std::borrow::BorrowMut;
use std::io;
use std::path::Path;

/// The main thing
pub struct Pipeline<'a> {
	pub headers: Headers,
	pub(crate) source: usize,
	iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}

impl<'a> Pipeline<'a> {
	pub fn from_reader<R: io::Read + 'a>(mut reader: Reader<R>) -> Result<Self, PlError> {
		let headers_row = reader.headers().unwrap().clone();
		let row_iterator = RowIter::from_records(0, reader.into_records());
		Ok(Pipeline {
			headers: match Headers::from_row(headers_row) {
				Ok(headers) => headers,
				Err(duplicated_col) => {
					return Err(Error::DuplicateColumn(duplicated_col).at_source(0))
				}
			},
			source: 0,
			iterator: Box::new(row_iterator),
		})
	}

	/// Create a pipeline from a CSV or TSV file.
	pub fn from_path<P: AsRef<Path>>(file_path: P) -> Result<Self, PlError> {
		let ext = file_path.as_ref().extension().unwrap_or_default();
		let delimiter = match ext.to_string_lossy().as_ref() {
			"tsv" => b'\t',
			"csv" => b',',
			_ => panic!("Unsupported file {}", file_path.as_ref().display()),
		};
		let reader_result = ReaderBuilder::new()
			.delimiter(delimiter)
			.from_path(file_path);
		match reader_result {
			Ok(reader) => Self::from_reader(reader),
			Err(e) => Err(Error::Csv(e).at_source(0)),
		}
	}

	pub fn from_rows<I: IntoIterator<Item = Row>>(records: I) -> Result<Self, PlError>
	where
		<I as IntoIterator>::IntoIter: 'a,
	{
		let mut records = records.into_iter();
		let headers_row = records.next().unwrap();
		let row_iterator = records.map(|row| -> RowResult {
			return Ok(row);
		});
		Ok(Pipeline {
			headers: match Headers::from_row(headers_row) {
				Ok(headers) => headers,
				Err(duplicated_col) => {
					return Err(Error::DuplicateColumn(duplicated_col).at_source(0))
				}
			},
			source: 0,
			iterator: Box::new(row_iterator),
		})
	}

	/// Merge multiple source pipelines into one. The source pipelines must have identical headers, otherwise the pipelie will return a [`MismatchedHeaders`](Error::MismatchedHeaders) error  returned.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// let csv = Pipeline::from_pipelines(vec![
	///   Pipeline::from_path("test/AB.csv").unwrap(),
	///   Pipeline::from_path("test/AB.csv").unwrap(),
	/// ])
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "A,B\n1,2\n1,2\n");
	/// ```
	pub fn from_pipelines<I>(pipelines: I) -> Self
	where
		I: IntoIterator<Item = Pipeline<'a>>,
		<I as IntoIterator>::IntoIter: 'a,
	{
		let mut pipelines = pipelines.into_iter();
		let current = pipelines.next();
		let headers = match current {
			Some(ref pipeline) => pipeline.headers.clone(),
			None => Headers::new(),
		};
		Pipeline {
			headers: headers.clone(),
			source: 0,
			iterator: Box::new(PipelinesChain {
				pipelines,
				current: current.map(|p| p.build()),
				index: 0,
				headers,
			}),
		}
	}

	/// Adds a column with values computed from the closure for each row.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// let csv = Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .add_col("C", |headers, row| {
	///     Ok("3".to_string())
	///   })
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "A,B,C\n1,2,3\n");
	/// ```
	pub fn add_col<F>(mut self, name: &str, get_value: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<String, Error> + 'a,
	{
		self.headers.push_field(name);
		self.iterator = Box::new(AddCol {
			iterator: self.iterator,
			f: get_value,
			source: self.source,
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
			source: self.source,
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
			source: self.source,
			index: self.headers.get_index(col),
		});
		self
	}

	/// Filter rows using the provided closure.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// let csv = Pipeline::from_path("test/Countries.csv")
	///   .unwrap()
	///   .filter(|headers, row| {
	///     let country = headers.get_field(&row, "Country").unwrap();
	///     country == "Tuvalu"
	///   })
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(
	///   csv,
	///   "ID,Country\n\
	///     2,Tuvalu\n"
	/// );
	/// ```
	pub fn filter<F>(mut self, get_row: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> bool + 'a,
	{
		self.iterator = Box::new(Filter {
			iterator: self.iterator,
			f: get_row,
			source: self.source,
			headers: self.headers.clone(),
		});
		self
	}

	/// Filter rows based on the field of the specified column, using the provided closure.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// let csv = Pipeline::from_path("test/Countries.csv")
	///   .unwrap()
	///   .filter_col("Country", |country| country == "Tuvalu")
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(
	///   csv,
	///   "ID,Country\n\
	///     2,Tuvalu\n"
	/// );
	/// ```
	pub fn filter_col<F>(mut self, name: &str, get_row: F) -> Self
	where
		F: FnMut(&str) -> bool + 'a,
	{
		self.iterator = Box::new(FilterCol {
			name: name.to_string(),
			iterator: self.iterator,
			f: get_row,
			source: self.source,
			headers: self.headers.clone(),
		});
		self
	}

	/// Pick which columns to output, in the specified order. Panics if duplicate colums are specified.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
	///
	/// let csv = Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .select(vec!["B"])
	///   .collect_into_string()
	///   .unwrap();
	///
	/// assert_eq!(csv, "B\n2\n");
	/// ```
	pub fn select(mut self, columns: Vec<&str>) -> Self {
		let new_header_row = Row::from(columns.clone());
		self.iterator = Box::new(Select {
			iterator: self.iterator,
			columns: columns.into_iter().map(String::from).collect(),
			source: self.source,
			headers: self.headers.clone(),
		});
		self.headers = Headers::from_row(new_header_row).unwrap();
		self
	}

	/// Panics if a new name already exists
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
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
			Err(e) => panic!("Error renaming column in source {}: {}", self.source, e),
		};
		self
	}

	/// Panics if a new name already exists
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
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

	/// Group and reduce rows into the provided format. Panics if the transform results in duplicate column names.
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::Pipeline;
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
	pub fn transform_into<T>(self, mut get_transformers: T) -> Self
	where
		T: FnMut() -> Vec<Box<dyn Transform>> + 'a,
	{
		let hashers = get_transformers();
		let names: Vec<_> = hashers.iter().map(|hasher| hasher.name()).collect();
		Pipeline {
			headers: Headers::from_row(Row::from(names)).unwrap(),
			source: self.source,
			iterator: Box::new(TransformInto {
				iterator: self.iterator,
				groups: LinkedHashMap::new(),
				hashers: get_transformers(),
				get_transformers,
				source: self.source,
				headers: self.headers.clone(),
			}),
		}
	}

	/// Do your own validation on each row.
	pub fn validate<F>(mut self, f: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<(), Error> + 'a,
	{
		self.iterator = Box::new(Validate {
			iterator: self.iterator,
			f,
			source: self.source,
			headers: self.headers.clone(),
		});
		self
	}

	/// Do your own validation on the fields in a column.
	pub fn validate_col<F>(mut self, name: &str, f: F) -> Self
	where
		F: FnMut(&str) -> Result<(), Error> + 'a,
	{
		self.iterator = Box::new(ValidateCol {
			name: name.to_string(),
			iterator: self.iterator,
			f,
			source: self.source,
			headers: self.headers.clone(),
		});
		self
	}

	/// Write to the specified [`Target`].
	///
	/// ## Example
	///
	/// ```
	/// use csv_pipeline::{Pipeline, Target};
	///
	/// let mut csv = String::new();
	/// Pipeline::from_path("test/AB.csv")
	///   .unwrap()
	///   .flush(Target::string(&mut csv))
	///   .run()
	///   .unwrap();
	///
	/// assert_eq!(csv, "A,B\n1,2\n");
	/// ```
	pub fn flush(mut self, target: impl Target + 'a) -> Self {
		let flush = Flush::new(self.iterator, target, self.source, self.headers.clone());
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
	pub fn run(self) -> Result<(), PlError> {
		self.build().run()
	}

	pub fn collect_into_string(self) -> Result<String, PlError> {
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

/// A pipeline you can iterate through. You can get one using [`Pipeline::build`].
pub struct PipelineIter<'a> {
	pub headers: Headers,
	pub iterator: Box<dyn Iterator<Item = RowResult> + 'a>,
}

impl<'a> PipelineIter<'a> {
	/// Advances the iterator until an error is found.
	///
	/// Returns `None` when the iterator is finished.
	pub fn next_error(&mut self) -> Option<PlError> {
		while let Some(item) = self.next() {
			if let Err(err) = item {
				return Some(err);
			}
		}
		None
	}

	/// Run through the whole iterator. Returns the first error found, if any
	pub fn run(&mut self) -> Result<(), PlError> {
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
	source: usize,
}
impl<R: io::Read> RowIter<R> {
	pub fn from_records(source: usize, records: StringRecordsIntoIter<R>) -> Self {
		RowIter {
			source,
			inner: records,
		}
	}
}
impl<R: io::Read> Iterator for RowIter<R> {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|result| {
			result.map_err(|err| {
				return Error::Csv(err).at_source(self.source);
			})
		})
	}
}

#[test]
fn from_pipelines_mismatch() {
	let err = Pipeline::from_pipelines(vec![
		Pipeline::from_path("test/AB.csv").unwrap(),
		Pipeline::from_path("test/AB.csv").unwrap(),
		Pipeline::from_path("test/Countries.csv").unwrap(),
	])
	.collect_into_string()
	.unwrap_err();

	assert_eq!(err.source, 2);
	match err.error {
		Error::MismatchedHeaders(h1, h2) => {
			assert_eq!(h1, Row::from(vec!["A", "B"]));
			assert_eq!(h2, Row::from(vec!["ID", "Country"]));
		}
		_ => panic!("Expected MismatchedHeaders"),
	}
}
