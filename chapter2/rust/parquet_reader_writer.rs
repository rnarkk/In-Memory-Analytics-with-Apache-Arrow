use std::fs::File;
use arrow;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    writer::SerializedFileWriter
};

fn main() {
    let file = File::open("../../sample_data/train.parquet").unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let batch = reader.next().unwrap();
    println!("{}", batch);

    let outfile = File::create("train.parquet").unwrap();
    let writer = SerializedFileWriter::new(outfile).unwrap();
    let chunk_size = 1024i64;
    writer.write(*table, outfile, chunk_size).unwrap();
    writer.close().unwrap();
}
