use std::fs::File;
use arrow::json::RawReader;

fn main() {
    let file = File::open("sample.json").unwrap();
    let reader = RawReader::new(file).unwrap();
    let table = reader.read().unwrap();
    println!("{}", table);
}
