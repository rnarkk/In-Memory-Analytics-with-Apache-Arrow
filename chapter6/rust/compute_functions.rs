use std::fs::File;
use arrow::{
    compute::{self as cp, SortOptions},
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

    let incremented = cp::add_scalar(column, 5.5)?;
    println!("{}", incremented);
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

    let sort_opts = SortOptions { descending: true, nulls_first: };
    sort_opts.sort_keys = {arrow::compute::SortKey{
        "total_amount", arrow::compute::SortOrder::Descending}};
    let indices = arrow::compute::CallFunction("sort_indices", {batch}, &sort_opts).unwrap();

    let sorted = cp::take(batch, indices).unwrap();
    let output = std::move(sorted).table();
    println!("{}", output);

    Ok(())
}

fn main() {
    compute_parquet().unwrap();
    find_minmax().unwrap();
    sort_table().unwrap();
}
