use core::fs::File;
use arrow;
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    writer::SerializedFileWriter
};

fn main() {
    let file = File::open("../../sample_data/train.parquet").unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader.ReadTable(&table));
    println!("{}", table.ToString());

    let outfile = File::create("train.parquet").unwrap();
    let writer = SerializedFileWriter::new(outfile).unwrap();
    let i64 chunk_size = 1024;
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
          *table, outfile, chunk_size));
    writer.close().unwrap();
}
