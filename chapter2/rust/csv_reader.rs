use std::fs::File;
use arrow::{
    csv::ReaderBuilder,  // the csv functions and objects
    table   // to read the data into a table
};

fn main() {
    let file = File::open("../../sample_data/train.csv").unwrap();
    let read_options = arrow::csv::ReadOptions::Defaults();
    let parse_options = arrow::csv::ParseOptions::Defaults();
    let convert_options = arrow::csv::ConvertOptions::Defaults();

    let reader = Reader::new(file, read_options, parse_options, convert_options).unwrap();

    // finally read the data from the file
    let table = reader.read().unwrap();
    println!("{}", table);
}
