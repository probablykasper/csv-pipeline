mod headers;
mod pipeline;
mod pipeline_iterators;
mod target;
mod transform;

pub use headers::Headers;
pub use pipeline::{Pipeline, PipelineIter};
pub use target::{PathTarget, StderrTarget, StdoutTarget, StringTarget, Target};
pub use transform::Transformer;

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
fn transform() {
	let source = "\
		Person,Score\n\
		A,1\n\
		A,8\n\
		B,3\n\
		B,4\n";
	let reader = csv::ReaderBuilder::new()
		.flexible(true)
		.from_reader(source.as_bytes());
	let csv_str = Pipeline::from_reader(reader)
		.unwrap()
		.map(|_headers, row| Ok(row))
		.transform_into(|| {
			vec![
				Transformer::new("Person").keep_unique(),
				Transformer::new("Total score").from_col("Score").reduce(
					|accumulator, current| {
						let score: u64 = current.parse().unwrap();
						Ok(accumulator + score)
					},
					0,
				),
			]
		})
		.collect_into_string()
		.unwrap();

	assert_eq!(
		csv_str,
		"Person,Score\n\
			A,9\n\
			B,7\n"
	);
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
