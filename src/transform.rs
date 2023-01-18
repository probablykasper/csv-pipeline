use crate::{Error, Headers, Row};
use core::fmt::Display;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::AddAssign;
use std::str::FromStr;

/// For grouping and reducing rows.
pub trait Transform {
	/// Add the row to the hasher to group this row separately from others
	fn hash(
		&self,
		_hasher: &mut DefaultHasher,
		_headers: &Headers,
		_row: &Row,
	) -> Result<(), Error> {
		Ok(())
	}

	/// Get the resulting column name
	fn name(&self) -> String;

	/// Combine the row with the value
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error>;

	/// Turn the current value to a string
	fn value(&self) -> String;
}

/// A struct for building a [`Transform`], which you can use with [`Pipeline::transform_into`](crate::Pipeline::transform_into).
pub struct Transformer {
	pub name: String,
	pub from_col: String,
}
impl Transformer {
	pub fn new(col_name: &str) -> Self {
		Self {
			name: col_name.to_string(),
			from_col: col_name.to_string(),
		}
	}
	/// Specify which column the transform should be based on
	pub fn from_col(mut self, col_name: &str) -> Self {
		self.from_col = col_name.to_string();
		self
	}
	/// Keep the unique values from this column
	pub fn keep_unique(self) -> Box<dyn Transform> {
		Box::new(KeepUnique {
			name: self.name,
			from_col: self.from_col,
			value: "".to_string(),
		})
	}
	/// Sum the values in this column
	pub fn sum<'a, N>(self, init: N) -> Box<dyn Transform + 'a>
	where
		N: Display + AddAssign + FromStr + Clone + 'a,
	{
		Box::new(Sum {
			name: self.name,
			from_col: self.from_col,
			value: init,
		})
	}
	/// Reduce the values from this column into a single value using a closure
	pub fn reduce<'a, R, V>(self, reduce: R, init: V) -> Box<dyn Transform + 'a>
	where
		R: FnMut(V, &str) -> Result<V, Error> + 'a,
		V: Display + Clone + 'a,
	{
		Box::new(Reduce {
			name: self.name,
			from_col: self.from_col,
			reduce,
			value: init,
		})
	}
}

struct KeepUnique {
	name: String,
	from_col: String,
	value: String,
}
impl Transform for KeepUnique {
	fn hash(&self, hasher: &mut DefaultHasher, headers: &Headers, row: &Row) -> Result<(), Error> {
		let field = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.from_col.clone()))?;
		field.hash(hasher);
		Ok(())
	}

	fn name(&self) -> String {
		self.name.clone()
	}

	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error> {
		self.value = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.from_col.clone()))?
			.to_string();
		Ok(())
	}

	fn value(&self) -> String {
		self.value.clone()
	}
}

pub(crate) fn compute_hash<'a>(
	transformers: &Vec<Box<dyn Transform + 'a>>,
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

struct Reduce<F, V> {
	name: String,
	from_col: String,
	reduce: F,
	value: V,
}
impl<F, V> Transform for Reduce<F, V>
where
	F: FnMut(V, &str) -> Result<V, Error>,
	V: Display + Clone,
{
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error> {
		let field = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.from_col.clone()))?
			.to_string();
		self.value = (self.reduce)(self.value.clone(), &field)?;
		Ok(())
	}

	fn value(&self) -> String {
		self.value.to_string()
	}

	fn name(&self) -> String {
		self.name.clone()
	}
}

struct Sum<N> {
	name: String,
	from_col: String,
	value: N,
}
impl<V> Transform for Sum<V>
where
	V: Display + AddAssign + FromStr + Clone,
{
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error> {
		let field = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.from_col.clone()))?
			.to_string();
		let new: V = match field.parse() {
			Ok(v) => v,
			Err(_) => return Err(Error::InvalidField(field)),
		};
		self.value += new;
		Ok(())
	}

	fn value(&self) -> String {
		self.value.to_string()
	}
	fn name(&self) -> String {
		self.name.clone()
	}
}
