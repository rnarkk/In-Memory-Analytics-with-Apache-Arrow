#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

use core::fs::File;
use arrow;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() {
    let input = File::open("../../sample_data/train.parquet").unwrap();
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    let status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(),
                                         &arrow_reader);

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(arrow_reader.ReadTable(&table));

    println!("{}", table.ToString());

    let outfile = arrow::io::FileOutputStream::Open("train.parquet").unwrap();
    let i64 chunk_size = 1024;
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, chunk_size));
  PARQUET_THROW_NOT_OK(outfile.Close());
}
