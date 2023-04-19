use std::fs::{File, OpenOptions};
use arrow::{
    csv::{ReaderBuilder, WriterBuilder},
    ipc,
    record_batch::RecordBatch
};

fn read_csv(filename: &str) -> arrow::Result<std::shared_ptr<arrow::Table>> {
    let file = File::open(filename).unwrap();
    let reader = Reader::new(file).unwrap();
    reader.read()
}

fn write_table(batch: &RecordBatch, filename: &str) -> Result<()> {
    let append = false;  // set to true to append to an existing file
    let file = OpenOptions::new().append(append).open(filename).unwrap();
    let writer = WriterBuilder::new().build(file);
    writer.write(batch)
}

fn incremental_write(table: std::shared_ptr<arrow::Table>,
                     output_filename: &str) -> arrow::Status {
    let append = false;  // set to true to append to an existing file
    let output = OpenOptions::new().append(append).open(output_filename, append).unwrap();
    let table_reader = arrow::TableBatchReader(*table);

    let maybe_writer = arrow::csv::MakeCSVWriter(
        output, table_reader.schema(), arrow::csv::WriteOptions::Defaults());
    if !maybe_writer.ok() {
        return maybe_writer.status();
    }

    let writer: std::shared_ptr<arrow::ipc::RecordBatchWriter> = *maybe_writer;
    let batch = std::shared_ptr<arrow::RecordBatch>;
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
