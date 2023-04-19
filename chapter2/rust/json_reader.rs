use std::fs::File;
use arrow::json::RawReaderBuilder;

fn main() {
    let file = File::open("sample.json").unwrap();
    let reader = RawReaderBuild::new(file).unwrap();
    let batch = reader.read().unwrap();
    println!("{}", batch);
}
