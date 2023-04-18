use std::fs::File;
use arrow::{
    csv::Reader,
    ipc
};

fn read_csv(filename: &str) -> arrow::Result<std::shared_ptr<arrow::Table>> {
    let file = File::open(filename).unwrap();
    let read_options = arrow::csv::ReadOptions::Defaults();
    let parse_options = arrow::csv::ParseOptions::Defaults();
    let convert_options = arrow::csv::ConvertOptions::Defaults();

    let reader = Reader::new(file, read_options, parse_options, convert_options).unwrap();
    reader.read()
}

fn write_table(table: std::shared_ptr<arrow::Table>,
               output_filename: &str) -> arrow::Status {
    let append = false;  // set to true to append to an existing file
    let output = File::open(output_filename, append).unwrap();
    let write_options = arrow::csv::WriteOptions::Defaults();
    arrow::csv::WriteCSV(*table, write_options, output.get())
}

fn incremental_write(table: std::shared_ptr<arrow::Table>,
                     output_filename: &str) -> arrow::Status {
    let append = false;  // set to true to append to an existing file
    let output = File::open(output_filename, append).unwrap();
    let table_reader = arrow::TableBatchReader(*table);

    let maybe_writer = arrow::csv::MakeCSVWriter(
        output, table_reader.schema(), arrow::csv::WriteOptions::Defaults());
    if (!maybe_writer.ok()) {
        return maybe_writer.status();
    }

    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = *maybe_writer;
    std::shared_ptr<arrow::RecordBatch> batch;
    while let Some(todo) = table_reader.next(&batch)  {
        if !batch {
            return;
        }
        status = writer.WriteRecordBatch(*batch);
        if !status.ok() {
            return status;
        }
    }

    RETURN_NOT_OK(writer.close());
    RETURN_NOT_OK(output.close());

    Ok(())
}

fn main() {
    let table = read_csv("../../sample_data/train.csv").unwrap();
    write_table(table, "train.csv").unwrap();
    incremental_write(table, "train.csv").unwrap();
}
