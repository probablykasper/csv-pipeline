use super::headers::Headers;
use crate::pipeline_iterators::{
	AddCol, Flush, MapCol, MapRow, PipelinesChain, Select, TransformInto, Validate, ValidateCol,
};
use crate::target::{StringTarget, Target};
use crate::transform::Transform;
use crate::{Error, Row, RowResult};
use csv::{Reader, ReaderBuilder, StringRecordsIntoIter};
use linked_hash_map::LinkedHashMap;
use std::borrow::BorrowMut;
use std::io;
use std::path::Path;

/// The main thing
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
	pub fn from_pipelines(pipelines: Vec<Pipeline<'a>>) -> Self {
		let mut pipelines = pipelines.into_iter();
		let current = pipelines.next();
		let headers = match current {
			Some(ref pipeline) => pipeline.headers.clone(),
			None => Headers::new(),
		};
		Pipeline {
			headers: headers.clone(),
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
			Err(e) => panic!("{:?}", e),
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

	/// Group and reduce rows into. Panics if the transform results in duplicate column names.
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
			iterator: Box::new(TransformInto {
				iterator: self.iterator,
				groups: LinkedHashMap::new(),
				hashers: get_transformers(),
				get_transformers,
				headers: self.headers.clone(),
			}),
		}
	}

	pub fn validate<F>(mut self, f: F) -> Self
	where
		F: FnMut(&Headers, &Row) -> Result<(), Error> + 'a,
	{
		self.iterator = Box::new(Validate {
			iterator: self.iterator,
			f,
			headers: self.headers.clone(),
		});
		self
	}

	pub fn validate_col<F>(mut self, name: &str, f: F) -> Self
	where
		F: FnMut(&str) -> Result<(), Error> + 'a,
	{
		self.iterator = Box::new(ValidateCol {
			name: name.to_string(),
			iterator: self.iterator,
			f,
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

/// A pipeline you can iterate through. You can get one using [`Pipeline::build`].
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

#[test]
fn from_pipelines_mismatch() {
	let err = Pipeline::from_pipelines(vec![
		Pipeline::from_path("test/AB.csv").unwrap(),
		Pipeline::from_path("test/AB.csv").unwrap(),
		Pipeline::from_path("test/Countries.csv").unwrap(),
	])
	.collect_into_string()
	.unwrap_err();

	match err {
		Error::MismatchedHeaders(h1, h2) => {
			assert_eq!(h1, Row::from(vec!["A", "B"]));
			assert_eq!(h2, Row::from(vec!["ID", "Country"]));
		}
		_ => panic!("Expected MismatchedHeaders"),
	}
}
