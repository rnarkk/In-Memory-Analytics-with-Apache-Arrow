use std::fs::File;
use arrow::json::RawReader;

fn main() {
    let filename = "sample.json";  // could be any json file
    let file = File::open(filename).unwrap();

    let reader = RawReader::new(file).unwrap();
    let table = reader.read().unwrap();
    println!("{}", table);
}
