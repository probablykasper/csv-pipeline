use super::headers::Headers;
use crate::target::Target;
use crate::{Error, Row, RowResult};

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
		let row = match self.iterator.next()? {
			Ok(row) => row,
			Err(e) => return Some(Err(e)),
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
