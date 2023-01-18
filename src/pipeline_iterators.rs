use super::headers::Headers;
use crate::target::Target;
use crate::transform::{compute_hash, Transform};
use crate::{Error, Pipeline, PipelineIter, Row, RowResult};
use linked_hash_map::{Entry, LinkedHashMap};

pub struct PipelinesChain<'a, P> {
	pub pipelines: P,
	pub current: Option<PipelineIter<'a>>,
	pub index: usize,
	pub headers: Headers,
}
impl<'a, P> Iterator for PipelinesChain<'a, P>
where
	P: Iterator<Item = Pipeline<'a>>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		// If current is None, iteration is done
		match self.current.as_mut()?.next() {
			Some(mut row) => {
				if let Err(e) = row.as_mut() {
					e.source = self.index;
				}
				return Some(row);
			}
			None => {}
		};
		// If current was done, go to the next pipeline
		match self.pipelines.next() {
			Some(pipeline) => {
				self.index += 1;
				self.current = Some(pipeline.build());
				let current = self.current.as_mut().unwrap();
				if current.headers.get_row() != self.headers.get_row() {
					return Some(Err(Error::MismatchedHeaders(
						self.headers.get_row().to_owned(),
						current.headers.get_row().to_owned(),
					)
					.at_source(self.index)));
				}
			}
			None => {
				self.current = None;
				return None;
			}
		}
		self.next()
	}
}

pub struct AddCol<I, F: FnMut(&Headers, &Row) -> Result<String, Error>> {
	pub iterator: I,
	pub f: F,
	pub source: usize,
	pub headers: Headers,
}
impl<I, F> Iterator for AddCol<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&Headers, &Row) -> Result<String, Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let mut row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		match (self.f)(&self.headers, &row) {
			Ok(value) => {
				row.push_field(&value);
				Some(Ok(row))
			}
			Err(e) => Some(Err(e.at_source(self.source))),
		}
	}
}

pub struct MapRow<I, F: FnMut(&Headers, Row) -> Result<Row, Error>> {
	pub iterator: I,
	pub f: F,
	pub source: usize,
	pub headers: Headers,
}
impl<I, F> Iterator for MapRow<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&Headers, Row) -> Result<Row, Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		match (self.f)(&self.headers, row) {
			Ok(value) => Some(Ok(value)),
			Err(e) => Some(Err(e.at_source(self.source))),
		}
	}
}

pub struct MapCol<I, F: FnMut(&str) -> Result<String, Error>> {
	pub iterator: I,
	pub f: F,
	pub name: String,
	pub source: usize,
	pub index: Option<usize>,
}
impl<I, F> Iterator for MapCol<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&str) -> Result<String, Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		let mut row_vec: Vec<_> = row.into_iter().collect();
		let index = match self.index {
			Some(index) => index,
			None => {
				return Some(Err(
					Error::MissingColumn(self.name.clone()).at_source(self.source)
				))
			}
		};
		let field = match row_vec.get_mut(index) {
			Some(field) => field,
			None => {
				return Some(Err(
					Error::MissingColumn(self.name.clone()).at_source(self.source)
				))
			}
		};
		let new_value = match (self.f)(field) {
			Ok(value) => value,
			Err(e) => return Some(Err(e.at_source(self.source))),
		};
		*field = &new_value;
		Some(Ok(row_vec.into()))
	}
}

pub struct Filter<I, F: FnMut(&Headers, &Row) -> Result<bool, Error>> {
	pub iterator: I,
	pub f: F,
	pub source: usize,
	pub headers: Headers,
}
impl<I, F> Iterator for Filter<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&Headers, &Row) -> Result<bool, Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let row = match self.iterator.next()? {
				Ok(row) => row,
				Err(e) => return Some(Err(e)),
			};
			let filter = (self.f)(&self.headers, &row);
			match filter {
				Ok(true) => return Some(Ok(row)),
				Ok(false) => continue,
				Err(e) => return Some(Err(e.at_source(self.source))),
			}
		}
	}
}

pub struct Select<I> {
	pub iterator: I,
	pub columns: Vec<String>,
	pub source: usize,
	pub headers: Headers,
}
impl<I> Iterator for Select<I>
where
	I: Iterator<Item = RowResult>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		let mut selection = Vec::with_capacity(self.columns.len());
		for col in &self.columns {
			let field = match self.headers.get_field(&row, col) {
				Some(field) => field,
				None => return Some(Err(Error::MissingColumn(col.clone()).at_source(self.source))),
			};
			selection.push(field);
		}
		Some(Ok(selection.into()))
	}
}

pub struct TransformInto<I, F>
where
	F: FnMut() -> Vec<Box<dyn Transform>>,
{
	pub iterator: I,
	pub groups: LinkedHashMap<u64, Vec<Box<dyn Transform>>>,
	pub hashers: Vec<Box<dyn Transform>>,
	pub get_transformers: F,
	pub source: usize,
	pub headers: Headers,
}
impl<I, F> Iterator for TransformInto<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut() -> Vec<Box<dyn Transform>>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		// If any error rows are found, they are returned first
		while let Some(row_result) = self.iterator.next() {
			// First run iterator into LinkedHashMap
			let row = match row_result {
				Ok(row) => row,
				Err(e) => return Some(Err(e)),
			};
			let hash = match compute_hash(&self.hashers, &self.headers, &row) {
				Ok(hash) => hash,
				Err(e) => return Some(Err(e.at_source(self.source))),
			};

			match self.groups.entry(hash) {
				Entry::Occupied(_) => {}
				Entry::Vacant(entry) => {
					let transformers = (self.get_transformers)();
					entry.insert(transformers);
				}
			}

			let group_row = self.groups.get_mut(&hash).unwrap();
			for reducer in group_row {
				let result = reducer.add_row(&self.headers, &row);
				if let Err(e) = result {
					return Some(Err(e.at_source(self.source)));
				}
			}
		}
		// Finally, return rows from the LinkedHashMap
		if let Some(key) = self.groups.keys().next().copied() {
			let reducers = self.groups.remove(&key).unwrap();
			let fields: Vec<_> = reducers.iter().map(|reducer| reducer.value()).collect();
			let row = Row::from(fields);
			Some(Ok(row))
		} else {
			None
		}
	}
}

pub struct Validate<I, F> {
	pub iterator: I,
	pub f: F,
	pub source: usize,
	pub headers: Headers,
}
impl<I, F> Iterator for Validate<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&Headers, &Row) -> Result<(), Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		match (self.f)(&self.headers, &row) {
			Ok(()) => Some(Ok(row)),
			Err(e) => Some(Err(e.at_source(self.source))),
		}
	}
}

pub struct ValidateCol<I, F> {
	pub name: String,
	pub iterator: I,
	pub f: F,
	pub source: usize,
	pub headers: Headers,
}
impl<I, F> Iterator for ValidateCol<I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&str) -> Result<(), Error>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		let field = match self.headers.get_field(&row, &self.name) {
			Some(field) => field,
			None => {
				return Some(Err(
					Error::MissingColumn(self.name.clone()).at_source(self.source)
				))
			}
		};
		match (self.f)(&field) {
			Ok(()) => Some(Ok(row)),
			Err(e) => Some(Err(e.at_source(self.source))),
		}
	}
}

pub struct Flush<I, T> {
	pub iterator: I,
	pub target: T,
	pub source: usize,
	/// `None` if headers have been written, `Some` otherwise
	headers: Option<Headers>,
}
impl<I, T> Flush<I, T> {
	pub fn new(iterator: I, target: T, source: usize, headers: Headers) -> Self {
		Self {
			iterator,
			target,
			source,
			headers: Some(headers),
		}
	}
}
impl<I, T> Iterator for Flush<I, T>
where
	I: Iterator<Item = RowResult>,
	T: Target,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(headers) = &self.headers {
			match self.target.write_headers(headers) {
				Ok(()) => self.headers = None,
				Err(e) => return Some(Err(Error::Csv(e).at_source(self.source))),
			}
		}

		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		let r = match self.target.write_row(&row) {
			Ok(()) => Some(Ok(row)),
			Err(e) => return Some(Err(Error::Csv(e).at_source(self.source))),
		};
		r
	}
}
