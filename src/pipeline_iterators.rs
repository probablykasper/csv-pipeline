use super::headers::Headers;
use crate::target::Target;
use crate::transform::{compute_hash, Transform};
use crate::{Error, Pipeline, PipelineIter, Row, RowResult};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

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
		match &mut self.current {
			Some(current) => match current.next() {
				Some(row) => return Some(row),
				None => {}
			},
			None => {}
		};
		match self.pipelines.next() {
			Some(pipeline) => {
				if pipeline.headers.get_row() != self.headers.get_row() {
					return Some(Err(Error::MismatchedHeaders(
						self.headers.get_row().to_owned(),
						pipeline.headers.get_row().to_owned(),
					)));
				}
				self.current = Some(pipeline.build());
				self.index += 1;
			}
			None => {
				self.current = None;
			}
		}
		match self.current {
			Some(ref mut current) => current.next(),
			None => None,
		}
	}
}

pub struct AddCol<I, F: FnMut(&Headers, &Row) -> Result<String, Error>> {
	pub iterator: I,
	pub f: F,
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
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct MapRow<I, F: FnMut(&Headers, Row) -> Result<Row, Error>> {
	pub iterator: I,
	pub f: F,
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
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct MapCol<I, F: FnMut(&str) -> Result<String, Error>> {
	pub iterator: I,
	pub f: F,
	pub name: String,
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
			None => return Some(Err(Error::MissingColumn(self.name.clone()))),
		};
		let field = match row_vec.get_mut(index) {
			Some(field) => field,
			None => return Some(Err(Error::MissingColumn(self.name.clone()))),
		};
		let new_value = match (self.f)(field) {
			Ok(value) => value,
			Err(e) => return Some(Err(e)),
		};
		*field = &new_value;
		Some(Ok(row_vec.into()))
	}
}

pub struct Select<I> {
	pub iterator: I,
	pub columns: Vec<String>,
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
				None => return Some(Err(Error::MissingColumn(col.clone()))),
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
	pub groups: BTreeMap<u64, Vec<Box<dyn Transform>>>,
	pub hashers: Vec<Box<dyn Transform>>,
	pub get_transformers: F,
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
			// First run iterator into BTreeMap
			let row = match row_result {
				Ok(row) => row,
				Err(e) => return Some(Err(e)),
			};
			let hash = match compute_hash(&self.hashers, &self.headers, &row) {
				Ok(hash) => hash,
				Err(e) => return Some(Err(e)),
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
					return Some(Err(e));
				}
			}
		}
		// Finally, return rows from the BTreeMap
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
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct ValidateCol<I, F> {
	pub name: String,
	pub iterator: I,
	pub f: F,
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
			None => return Some(Err(Error::MissingColumn(self.name.clone()))),
		};
		match (self.f)(&field) {
			Ok(()) => Some(Ok(row)),
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct Flush<I, T> {
	pub iterator: I,
	pub target: T,
	/// `None` if headers have been written, `Some` otherwise
	headers: Option<Headers>,
}
impl<I, T> Flush<I, T> {
	pub fn new(iterator: I, target: T, headers: Headers) -> Self {
		Self {
			iterator,
			target,
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
				Err(e) => return Some(Err(e)),
			}
		}

		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
		};
		let r = match self.target.write_row(&row) {
			Ok(()) => Some(Ok(row)),
			Err(e) => Some(Err(e)),
		};
		r
	}
}
