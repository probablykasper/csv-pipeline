use crate::Row;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Headers {
	indexes: HashMap<String, usize>,
	names: Row,
}
impl Headers {
	pub fn add(&mut self, name: &str) -> bool {
		if self.indexes.contains_key(name) {
			return false;
		}

		self.names.push_field(name);
		self.indexes.insert(name.to_string(), self.names.len() - 1);

		true
	}

	pub fn contains(&self, name: &str) -> bool {
		self.indexes.contains_key(name)
	}
}
impl From<Row> for Headers {
	fn from(row: Row) -> Headers {
		Headers {
			indexes: row
				.iter()
				.enumerate()
				.map(|(index, entry)| (entry.to_string(), index))
				.collect(),
			names: row,
		}
	}
}
