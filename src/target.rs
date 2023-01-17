use crate::{Headers, Row};
use csv::WriterBuilder;
use std::fs::{self, File};
use std::io;
use std::path::PathBuf;

pub trait Target {
	/// Useful for initializations
	fn write_headers(&mut self, headers: &Headers) -> Result<(), csv::Error>;
	fn write_row(&mut self, row: &Row) -> Result<(), csv::Error>;
}

pub struct PathTarget {
	path: PathBuf,
	writer: Option<csv::Writer<File>>,
}
impl PathTarget {
	pub fn new<P: Into<PathBuf>>(path: P) -> Self {
		Self {
			path: path.into(),
			writer: None,
		}
	}
}
impl Target for PathTarget {
	fn write_headers(&mut self, headers: &Headers) -> Result<(), csv::Error> {
		if let Some(parent) = self.path.parent() {
			fs::create_dir_all(parent)?;
		}

		self.writer = Some(csv::Writer::from_path(&self.path)?);
		self.write_row(headers.get_row())
	}
	fn write_row(&mut self, row: &Row) -> Result<(), csv::Error> {
		self.writer.as_mut().unwrap().write_record(row)?;
		Ok(())
	}
}

pub struct StdoutTarget {
	writer: Option<csv::Writer<io::Stdout>>,
}
impl StdoutTarget {
	pub fn new() -> Self {
		Self { writer: None }
	}
}
impl Target for StdoutTarget {
	fn write_headers(&mut self, headers: &Headers) -> Result<(), csv::Error> {
		let writer = WriterBuilder::new().from_writer(io::stdout());
		self.writer = Some(writer);
		self.write_row(headers.get_row())?;
		Ok(())
	}
	fn write_row(&mut self, row: &Row) -> Result<(), csv::Error> {
		self.writer.as_mut().unwrap().write_record(row)?;
		Ok(())
	}
}

pub struct StderrTarget {
	writer: Option<csv::Writer<io::Stderr>>,
}
impl StderrTarget {
	pub fn new() -> Self {
		Self { writer: None }
	}
}
impl Target for StderrTarget {
	fn write_headers(&mut self, headers: &Headers) -> Result<(), csv::Error> {
		let writer = WriterBuilder::new().from_writer(io::stderr());
		self.writer = Some(writer);
		self.write_row(headers.get_row())
	}
	fn write_row(&mut self, row: &Row) -> Result<(), csv::Error> {
		self.writer.as_mut().unwrap().write_record(row)?;
		Ok(())
	}
}

pub struct StringWriter<'a> {
	s: &'a mut String,
}
impl<'a> io::Write for StringWriter<'a> {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		match std::str::from_utf8(buf) {
			Ok(s) => {
				self.s.push_str(s);
				Ok(buf.len())
			}
			Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
		}
	}
	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

pub struct StringTarget<'a> {
	writer: csv::Writer<StringWriter<'a>>,
}
impl<'a> StringTarget<'a> {
	pub fn new(s: &'a mut String) -> Self {
		let writer = WriterBuilder::new().from_writer(StringWriter { s });
		Self { writer }
	}
}
impl<'a> Target for StringTarget<'a> {
	fn write_headers(&mut self, headers: &Headers) -> Result<(), csv::Error> {
		self.write_row(headers.get_row())
	}
	fn write_row(&mut self, row: &Row) -> Result<(), csv::Error> {
		self.writer.write_record(row)?;
		Ok(())
	}
}
