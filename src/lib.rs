mod headers;
mod pipeline;
mod pipeline_iterators;
pub mod target;

pub use headers::Headers;
pub use pipeline::{Pipeline, PipelineBuilder};
pub type Row = csv::StringRecord;
pub type RowResult = Result<Row, Error>;

#[derive(Debug)]
pub enum Error {
	Example,
	Csv(csv::Error),
	Io(std::io::Error),
	MissingColumn(String),
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
	let mut csv_str = String::new();
	let mut pipeline = PipelineBuilder::from_path("test/Countries.csv")
		.add_col("Language", |headers, row| {
			match headers.get_field(row, "Country") {
				Some("Norway") => Ok("Norwegian".to_string()),
				_ => Ok("Unknown".to_string()),
			}
		})
		.map_col("Country", |id_str| Ok(id_str.to_uppercase()))
		.flush(target::StringTarget::new(&mut csv_str))
		.build();

	while let Some(err) = pipeline.next_error() {
		eprintln!("Error: {err:?}");
	}
	drop(pipeline);

	assert_eq!(
		csv_str,
		"ID,Country,Language\n\
			1,NORWAY,Norwegian\n\
			2,TUVALU,Unknown\n"
	);
}
