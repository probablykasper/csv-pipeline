use crate::{Error, Headers, Row};
use core::fmt::Display;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

	fn new_reducer<'a>(&'a mut self) -> Box<dyn Reduce + 'a>;

	/// Get the resulting column name
	fn name(&self) -> String;
}

pub trait Reduce {
	/// Combine the row with the value
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error>;

	/// Turn the current value to a string
	fn value(&self) -> String;
}

pub struct Transformer {
	name: String,
	from_col: String,
}
impl<'a> Transformer {
	pub fn new(col_name: &str) -> Self {
		Self {
			name: col_name.to_string(),
			from_col: col_name.to_string(),
		}
	}
	pub fn keep_unique(self) -> Box<dyn Transform> {
		Box::new(KeepUnique {
			name: self.name,
			from_col: self.from_col,
			value: "".to_string(),
		})
	}
}
impl Transformer {
	pub fn reduce<'a, R, V>(self, reduce: R, init: V) -> Box<dyn Transform + 'a>
	where
		R: FnMut(V, &str) -> Result<V, Error> + 'a,
		V: Display + Clone + 'a,
	{
		Box::new(ClosureTransformer {
			name: self.name,
			from_col: self.from_col,
			reduce,
			value: init,
		})
	}
}

struct ClosureTransformer<F, V> {
	name: String,
	from_col: String,
	reduce: F,
	value: V,
}
impl<F, V> Transform for ClosureTransformer<F, V>
where
	F: FnMut(V, &str) -> Result<V, Error>,
	V: Display + Clone,
{
	fn new_reducer<'a>(&'a mut self) -> Box<dyn Reduce + 'a> {
		Box::new(Closure {
			from_col: self.from_col.clone(),
			reduce: &mut self.reduce,
			value: self.value.clone(),
		})
	}

	fn name(&self) -> String {
		self.name.clone()
	}
}

struct Closure<'a, F, V> {
	from_col: String,
	reduce: &'a mut F,
	value: V,
}
impl<F, V> Reduce for Closure<'_, F, V>
where
	F: FnMut(V, &str) -> Result<V, Error>,
	V: Display + Clone,
{
	fn add_row(&mut self, headers: &Headers, row: &Row) -> Result<(), Error> {
		let field = headers
			.get_field(row, &self.from_col)
			.ok_or(Error::MissingColumn(self.from_col.clone()))?
			.to_string();
		(self.reduce)(self.value.clone(), &field)?;
		todo!()
	}

	fn value(&self) -> String {
		self.value.to_string()
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
	fn new_reducer<'a>(&'a mut self) -> Box<dyn Reduce + 'a> {
		Box::new(Self {
			name: self.name.clone(),
			from_col: self.name.clone(),
			value: "".to_string(),
		})
	}

	fn name(&self) -> String {
		self.name.clone()
	}
}
impl Reduce for KeepUnique {
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

pub fn compute_hash<'a>(
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
