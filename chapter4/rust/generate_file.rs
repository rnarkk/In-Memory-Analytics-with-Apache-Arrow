use std::fs::File;
use arrow::{
    self,
    csv::ReaderBuilder
};
use datafusion::prelude::*;
use parquet as pq;

// run this from the sample_data directory to generate the .parquet
// .arrow and -nonan.arrow files

fn main() {
    let file = File::open("yellow_tripdata_2015-01.csv").unwrap();
    let reader = ReaderBuilder::new().infer_schema(Some(100)).build(file).unwrap();
    let batch = reader.next();
    pq.write_table(batch, "yellow_tripdata_2015-01.parquet");
    let sink = pa.OSFile("yellow_tripdata_2015-01.arrow", "wb");
    let writer = pa.RecordBatchFileWriter(sink, batch.schema);
    writer.write_table(batch);

    // fill out the NaN values with 0s so that we can zero-copy it to
    // a pandas dataframe in our memory usage test
    let df = batch.to_pandas().fillna(0);
    let batch = pa.Table.from_pandas(df);
    let sink = pa.OSFile("yellow_tripdata_2015-01-nonan.arrow", "wb");
    let writer = pa.RecordBatchFileWriter(sink, batch.schema);
    writer.write_table(batch);
}
