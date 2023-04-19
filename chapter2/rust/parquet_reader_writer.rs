use std::{
    fs::File,
    sync::Arc
};
use parquet::{
    errors::Result,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::SerializedFileWriter
    }
};

fn main() -> Result<()> {
    let input = File::open("../../sample_data/train.parquet").unwrap();
    let reader = SerializedFileReader::new(input)?;
    let batch = reader.into_iter().next().unwrap();
    println!("{:?}", batch);

    let output = File::create("train.parquet").unwrap();
    let schema = Arc::new(reader.metadata().file_metadata().schema().clone());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(output, schema, props)?;
    writer.next_row_group()?.next_column()?.unwrap().typed()
        .write_batch(&batch, None, None)?;
    writer.close()?;
    Ok(())
}
