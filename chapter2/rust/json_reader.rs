use std::{
    fs::File,
    io::BufReader,
    sync::Arc
};
use arrow::json::{
    RawReaderBuilder as ReaderBuilder,
    reader::infer_json_schema
};

fn main() {
    let file = File::open("sample.json").unwrap();
    let mut buffer = BufReader::new(file);
    let schema = Arc::new(infer_json_schema(&mut buffer, None).unwrap());
    let mut reader = ReaderBuilder::new(schema).build(buffer).unwrap();
    let batch = reader.next().unwrap().unwrap();
    println!("{:?}", batch);
}
