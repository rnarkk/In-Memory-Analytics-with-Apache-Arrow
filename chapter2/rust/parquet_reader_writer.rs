#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <iostream>

use core::fs::File;
use arrow;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn main(int argc, char** argv) {
    let input = File::open("../../sample_data/train.parquet").unwrap();

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    let status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(),
                                         &arrow_reader);
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(arrow_reader->ReadTable(&table));

    std::cout << table->ToString() << std::endl;

    PARQUET_ASSIGN_OR_THROW(auto outfile,
                          arrow::io::FileOutputStream::Open("train.parquet"));
    let i64 chunk_size = 1024;
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, chunk_size));
  PARQUET_THROW_NOT_OK(outfile->Close());
}
