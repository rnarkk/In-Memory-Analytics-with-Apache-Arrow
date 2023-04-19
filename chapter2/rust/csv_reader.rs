use std::fs::File;
use arrow::{
    csv::ReaderBuilder,
    table
};

fn main() {
    let file = File::open("../../sample_data/train.csv").unwrap();
    let reader = ReaderBuilder::new(file).unwrap();
    let table = reader.read().unwrap();
    println!("{}", table);
}
