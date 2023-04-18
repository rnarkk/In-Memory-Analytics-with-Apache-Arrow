use std::fs::File;
use arrow;
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    writer::SerializedFileWriter
};

fn main() {
    let file = File::open("../../sample_data/train.parquet").unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    let table = std::shared_ptr<arrow::Table>;
    reader.ReadTable(&table).unwrap();
    println!("{}", table);

    let outfile = File::create("train.parquet").unwrap();
    let writer = SerializedFileWriter::new(outfile).unwrap();
    let chunk_size = 1024i64;
    writer.write(*table, outfile, chunk_size).unwrap();
    writer.close().unwrap();
}
