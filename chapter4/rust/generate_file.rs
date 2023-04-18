use std::fs::File;
use arrow::{
    self,
    csv::Reader
};
use parquet as pq;
// import pandas as pd

// run this from the sample_data directory to generate the .parquet
// .arrow and -nonan.arrow files

fn main() {
    let tbl = pa.csv.read_csv("yellow_tripdata_2015-01.csv");
    pq.write_table(tbl, "yellow_tripdata_2015-01.parquet");
    let sink = pa.OSFile("yellow_tripdata_2015-01.arrow", "wb");
    let writer = pa.RecordBatchFileWriter(sink, tbl.schema);
    writer.write_table(tbl);

    // fill out the NaN values with 0s so that we can zero-copy it to
    // a pandas dataframe in our memory usage test
    let df = tbl.to_pandas().fillna(0);
    let tbl = pa.Table.from_pandas(df);
    let sink = pa.OSFile("yellow_tripdata_2015-01-nonan.arrow", "wb");
    let writer = pa.RecordBatchFileWriter(sink, tbl.schema);
    writer.write_table(tbl);
}
