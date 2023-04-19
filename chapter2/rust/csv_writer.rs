use std::fs::{File, OpenOptions};
use arrow::{
    csv::{ReaderBuilder, Writer, WriterBuilder},
    error::Result,
    record_batch::RecordBatch
};

fn read_csv(path: &str) -> Result<RecordBatch> {
    let file = File::open(path).unwrap();
    let mut reader = ReaderBuilder::new().build(file).unwrap();
    reader.next().unwrap()
}

fn write_table(batch: &RecordBatch, path: &str) -> Result<()> {
    let append = false;  // set to true to append to an existing file
    let file = OpenOptions::new().append(append).open(path).unwrap();
    let mut writer = WriterBuilder::new().build(file);
    writer.write(batch)
}

fn incremental_write(batch: &RecordBatch, path: &str) -> Result<()> {
    let append = false;  // set to true to append to an existing file
    let output = OpenOptions::new().append(append).open(path).unwrap();
    let reader = StreamReader::try_new(batch, None).unwrap();
    let writer = Writer::new(output);

    while let Some(batch) = reader.next() {
        if !batch {
            return;
        }
        status = writer.write(&batch);
        if !status.ok() {
            return status;
        }
    }

    Ok(())
}

fn main() {
    let batch = read_csv("../../sample_data/train.csv").unwrap();
    write_table(&batch, "train.csv").unwrap();
    incremental_write(&batch, "train.csv").unwrap();
}
