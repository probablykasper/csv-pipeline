use crate::{Error, Row};
use csv::StringRecordIter;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Headers {
	indexes: HashMap<String, usize>,
	row: Row,
}
impl Headers {
	pub fn new() -> Self {
		Headers {
			indexes: HashMap::new(),
			row: Row::new(),
		}
	}

	/// Returns `false` if `from` is non-existant or if the new name already exists
	pub fn rename(&mut self, from: &str, to: &str) -> Result<(), Error> {
		if self.contains(to) {
			return Err(Error::DuplicateColumn(to.to_string()));
		}
		let index = match self.indexes.remove(from) {
			Some(index) => index,
			None => return Err(Error::MissingColumn(from.to_string())),
		};
		self.indexes.insert(to.to_string(), index);
		let mut row_vec: Vec<_> = self.row.into_iter().collect();
		row_vec[index] = to;
		self.row = row_vec.into_iter().collect();
		Ok(())
	}

	/// Returns false if the field already exists
	pub fn push_field(&mut self, name: &str) -> bool {
		if self.indexes.contains_key(name) {
			return false;
		}

		self.row.push_field(name);
		self.indexes.insert(name.to_string(), self.row.len() - 1);

		true
	}

	pub fn contains(&self, name: &str) -> bool {
		self.indexes.contains_key(name)
	}

	pub fn get_field<'a>(&self, row: &'a Row, name: &str) -> Option<&'a str> {
		self.indexes.get(name).and_then(|index| row.get(*index))
	}

	pub fn get_index(&self, name: &str) -> Option<usize> {
		self.indexes.get(name).copied()
	}

	pub fn get_row(&self) -> &Row {
		&self.row
	}

	pub fn from_row(row: Row) -> Result<Self, Error> {
		let mut header = Headers::new();
		for field in &row {
			let added = header.push_field(field);
			if !added {
				return Err(Error::DuplicateColumn(field.to_string()));
			}
		}
		Ok(header)
	}
}

impl<'a> IntoIterator for &'a Headers {
	type Item = &'a str;
	type IntoIter = StringRecordIter<'a>;

	fn into_iter(self) -> StringRecordIter<'a> {
		self.row.into_iter()
	}
}
impl From<Headers> for Row {
	fn from(headers: Headers) -> Row {
		headers.row
	}
}
