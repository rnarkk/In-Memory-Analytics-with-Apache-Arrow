use std::fs::File;
use arrow::csv::ReaderBuilder;

fn main() {
    let file = File::open("../../sample_data/train.csv").unwrap();
    let reader = ReaderBuilder::new().build(file).unwrap();
    let batch = reader.read().unwrap();
    println!("{}", batch);
}
