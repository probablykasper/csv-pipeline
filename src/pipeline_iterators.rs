use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::HashMap;
use std::hash::Hasher;

use super::headers::Headers;
use crate::target::Target;
use crate::transform::compute_hash;
use crate::{Error, Row, RowResult, Transform};

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

pub struct TransformInto<I> {
	pub iterator: I,
	pub groups: HashMap<u64, Vec<Box<dyn Transform>>>,
	pub transformers: Vec<Box<dyn Transform>>,
	pub headers: Headers,
}
impl<I> Iterator for TransformInto<I>
where
	I: Iterator<Item = RowResult>,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		// First run iterator into hashmap, then return rows from the hashmap
		// If any error rows are found, they are returned first
		if let Some(row_result) = self.iterator.next() {
			let row = match row_result {
				Ok(row) => row,
				Err(e) => return Some(Err(e)),
			};
			let hash = match compute_hash(&self.transformers, &self.headers, &row) {
				Ok(hash) => hash,
				Err(e) => return Some(Err(e)),
			};
			let group_row = self.groups.entry(hash).or_default();
			for transformer in self.transformers {
				let result = transformer.add_row(&self.headers, &row);
				if let Err(e) = result {
					return Some(Err(e));
				}
			}
			group_row.push_field(row);
		} else {
		}

		x
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
