use crate::{Error, Headers, Row};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::AddAssign;
use std::str::FromStr;

pub trait Transform {
	/// Add the row to the hasher to group this row separately from others
	fn hash(&self, hasher: &mut DefaultHasher, headers: &Headers, row: &Row) -> Result<(), Error> {
		Ok(())
	}

	/// Combine the row with the value
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error>;

	/// Turn the current value to a string
	fn value(&self) -> String;

	/// Get the resulting column name
	fn name(&self) -> String;
}

pub struct Transformer {
	col_name: String,
	from_col: String,
}
impl Transformer {
	pub fn new(col_name: &str) -> Self {
		Self {
			col_name: col_name.to_string(),
			from_col: col_name.to_string(),
		}
	}
	pub fn keep_unique(self) -> Box<dyn Transform> {
		Box::new(KeepUnique {
			col_name: self.col_name,
			from_col: self.from_col,
			value: "".to_string(),
		})
	}
	pub fn sum<'a, N: AddAssign + FromStr + ToString + 'a>(
		self,
		init: N,
	) -> Box<dyn Transform + 'a> {
		Box::new(Sum {
			col_name: self.col_name,
			from_col: self.from_col,
			value: init,
		})
	}
}

pub fn compute_hash(
	transformers: &[Box<dyn Transform>],
	headers: &Headers,
	row: &Row,
) -> Result<u64, Error> {
	let mut hasher = DefaultHasher::new();
	for transformer in transformers {
		let result = transformer.hash(&mut hasher, &headers, &row);
		if let Err(e) = result {
			return Err(e);
		}
	}
	Ok(hasher.finish())
}

pub struct KeepUnique {
	col_name: String,
	from_col: String,
	value: String,
}
impl Transform for KeepUnique {
	fn hash(&self, hasher: &mut DefaultHasher, headers: &Headers, row: &Row) -> Result<(), Error> {
		let field = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.value.clone()))?;
		field.hash(hasher);
		Ok(())
	}
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error> {
		self.value = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.value.clone()))?
			.to_string();
		Ok(())
	}
	fn value(&self) -> String {
		self.value.clone()
	}
	fn name(&self) -> String {
		self.col_name.clone()
	}
}

pub struct Sum<N> {
	col_name: String,
	from_col: String,
	value: N,
}
impl<N: AddAssign + FromStr + ToString> Transform for Sum<N> {
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error> {
		let field = headers.get_field(row, &self.col_name).unwrap();
		let number = match N::from_str(field) {
			Ok(number) => number,
			Err(e) => return Err(Error::InvalidField(field.to_string())),
		};
		self.value += number;
		Ok(())
	}
	fn value(&self) -> String {
		self.value.to_string()
	}
	fn name(&self) -> String {
		self.col_name.clone()
	}
}
