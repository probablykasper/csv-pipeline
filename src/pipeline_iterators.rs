use super::headers::Headers;
use crate::target::Target;
use crate::transform::{compute_hash, Reduce, Transform};
use crate::{Error, Row, RowResult};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub struct AddCol<'a, I, F: FnMut(&Headers, &Row) -> Result<&'a str, Error>> {
	pub iterator: I,
	pub f: F,
	pub headers: Headers,
}
impl<'a, I, F> Iterator for AddCol<'a, I, F>
where
	I: Iterator<Item = RowResult>,
	F: FnMut(&Headers, &Row) -> Result<&'a str, Error>,
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

pub struct TransformInto<'a, I> {
	pub iterator: I,
	pub groups: HashMap<u64, Vec<Box<dyn Reduce + 'a>>>,
	pub transformers: Vec<Box<dyn Transform + 'a>>,
	pub headers: Headers,
}
impl<'a, I> TransformInto<'a, I> {
	pub fn ensure_group(&'a mut self, hash: u64) {
		if let Entry::Vacant(entry) = self.groups.entry(hash) {
			let reducers = self
				.transformers
				.iter_mut()
				.map(|transformer| transformer.new_reducer())
				.collect();
			entry.insert(reducers);
		};
	}
}
impl<'a, I> Iterator for TransformInto<'a, I>
where
	I: Iterator<Item = RowResult>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		// If any error rows are found, they are returned first
		if let Some(row_result) = self.iterator.next() {
			// First run iterator into hashmap
			let row = match row_result {
				Ok(row) => row,
				Err(e) => return Some(Err(e)),
			};
			let hash = match compute_hash(&self.transformers, &self.headers, &row) {
				Ok(hash) => hash,
				Err(e) => return Some(Err(e)),
			};
			self.ensure_group(hash);
			let group_row = self.groups.get_mut(&hash).unwrap();
			for reducer in group_row {
				let result = reducer.add_row(&self.headers, &row);
				if let Err(e) = result {
					return Some(Err(e));
				}
			}
			return self.next();
		}
		// Finally, return rows from the hashmap
		if let Some(key) = self.groups.keys().next().copied() {
			let reducers = self.groups.remove(&key).unwrap();
			let fields: Vec<_> = reducers.iter().map(|reducer| reducer.value()).collect();
			let row = Row::from(fields);
			println!("x {row:?}");
			println!("- {}", reducers.len());
			Some(Ok(row))
		} else {
			None
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
