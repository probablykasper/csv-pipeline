mod headers;
mod pipeline;
mod pipeline_iterators;
mod target;
mod transform;

pub use headers::Headers;
pub use pipeline::{Pipeline, PipelineIter};
pub use target::{PathTarget, StderrTarget, StdoutTarget, StringTarget, Target};
pub use transform::{Transform, Transformer};

pub type Row = csv::StringRecord;
pub type RowResult = Result<Row, Error>;

#[derive(Debug)]
pub enum Error {
	Csv(csv::Error),
	Io(std::io::Error),
	MissingColumn(String),
	DuplicateColumn(String),
	InvalidField(String),
}
impl From<csv::Error> for Error {
	fn from(error: csv::Error) -> Error {
		Error::Csv(error)
	}
}

impl From<std::io::Error> for Error {
	fn from(error: std::io::Error) -> Error {
		Error::Io(error)
	}
}

#[test]
fn test_pipeline() {
	let csv_str = Pipeline::from_path("test/Countries.csv")
		.unwrap()
		.add_col("Language", |headers, row| {
			match headers.get_field(row, "Country") {
				Some("Norway") => Ok("Norwegian"),
				_ => Ok("Unknown"),
			}
		})
		.transform_into([
			Transformer::new("ID").keep_unique(),
			Transformer::new("Country").sum(0),
		])
		.rename_col("Country", "COUNTRY")
		.map_col("COUNTRY", |id_str| Ok(id_str.to_uppercase()))
		.collect_into_string()
		.unwrap();

	assert_eq!(
		csv_str,
		"ID,COUNTRY,Language\n\
			1,NORWAY,Norwegian\n\
			2,TUVALU,Unknown\n"
	);
}
