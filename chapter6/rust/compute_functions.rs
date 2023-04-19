use std::fs::File;
use arrow::{
    compute::*,
    error::Result
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn compute_parquet() -> Result<()> {
    let path = "../../sample_data/yellow_tripdata_2015-01.parquet";
    let input = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)?.build()?;
    let batch = reader.next().unwrap().unwrap();
    let column = batch.column_by_name("total_amount").unwrap();
    println!("{:?}", column);
    let added = add_scalar(column, 5.5)?;
    println!("{}", added);
    Ok(())
}

fn find_minmax() -> Result<()> {
    let path = "../../sample_data/yellow_tripdata_2015-01.parquet";
    let input = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)?.build()?;
    let batch = reader.next().unwrap().unwrap();
    let column = batch.column_by_name("total_amount").unwrap();
    println!("{:?}", column);

    let scalar_agg_opts = arrow::compute::ScalarAggregateOptions;
    scalar_agg_opts.skip_nulls = false;
    let minmax = arrow::compute::CallFunction("min_max", {column}, &scalar_agg_opts).unwrap();
    println!("{}", minmax.scalar_as::<arrow::StructScalar>());
    Ok(())
}

fn sort_table() -> Result<()> {
    let filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
    let input = File::open(filepath).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)?.build()?;
    let batch = reader.next().unwrap().unwrap();
    let column = batch.column_by_name("total_amount").unwrap();
    let sort_opts = SortOptions { descending: true, nulls_first: true };
    let indices = sort_to_indices(column, &sort_opts, None)?;
    let output = take(batch, &indices, None)?;
    println!("{}", output);
    Ok(())
}

fn main() {
    compute_parquet().unwrap();
    find_minmax().unwrap();
    sort_table().unwrap();
}
