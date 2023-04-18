import pyarrow as pa
use core::fs::File;
use arrow::{
    self,
    csv::Reader
};
use parquet as pq;
import pandas as pd

// run this from the sample_data directory to generate the .parquet
// .arrow and -nonan.arrow files

fn main () {
tbl = pa.csv.read_csv('yellow_tripdata_2015-01.csv')
pq.write_table(tbl, 'yellow_tripdata_2015-01.parquet')
with pa.OSFile('yellow_tripdata_2015-01.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, tbl.schema) as writer:
        writer.write_table(tbl)

# fill out the NaN values with 0s so that we can zero-copy it to
# a pandas dataframe in our memory usage test
df = tbl.to_pandas().fillna(0)
tbl = pa.Table.from_pandas(df)
with pa.OSFile('yellow_tripdata_2015-01-nonan.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, tbl.schema) as writer:
        writer.write_table(tbl)
}
