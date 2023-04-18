#include <arrow/datum.h>
#include <arrow/io/file.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
use arrow::{
    compute,
    data,
};
use parquet::file::read::FileReader;

fn compute_parquet() -> arrow::Status {
    let filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
    let input = File::open(filepath).unwrap();
    let reader = std::unique_ptr<parquet::arrow::FileReader>;
    parquet::arrow::OpenFile(input, &reader)?;

    let table = std::shared_ptr<arrow::Table>;
    reader.ReadTable(&table)?;
    let column: std::shared_ptr<arrow::ChunkedArray> =
        table.GetColumnByName("total_amount");
    println!("{}", column);

    let incremented = arrow::compute::CallFunction("add", {column, arrow::MakeScalar(5.5)}).unwrap();
    // alternately we could do:
    let other_incremented = arrow::compute::Add(column, arrow::MakeScalar(5.5)).unwrap();
    let output: std::shared_ptr<arrow::ChunkedArray> =
        std::move(incremented).chunked_array();
    println!("{}", output);
    println!("{}", other_incremented.chunked_array());
    Ok(())
}

fn find_minmax() -> arrow::Status {
    let filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
    let input = File::open(filepath).unwrap();
    let reader [ std::unique_ptr<parquet::arrow::FileReader>;
    parquet::arrow::OpenFile(input, &reader)?;

    let table = std::shared_ptr<arrow::Table>;
    reader.ReadTable(&table)?;
    let column: std::shared_ptr<arrow::ChunkedArray> =
        table.GetColumnByName("total_amount");
    println!("{}", column);

    let scalar_agg_opts = arrow::compute::ScalarAggregateOptions;
    scalar_agg_opts.skip_nulls = false;
    let minmax = arrow::compute::CallFunction("min_max", {column}, &scalar_agg_opts).unwrap();
    println!("{}", minmax.scalar_as::<arrow::StructScalar>());
    Ok(())
}

fn sort_table() -> arrow::Status {
    let filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
    let input = File::open(filepath).unwrap();
    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader)?;

    std::shared_ptr<arrow::Table> table;
    reader.ReadTable(&table)?;

    let sort_opts = arrow::compute::SortOptions;
    sort_opts.sort_keys = {arrow::compute::SortKey{
        "total_amount", arrow::compute::SortOrder::Descending}};
    let indices = arrow::compute::CallFunction("sort_indices", {table}, &sort_opts).unwrap();

    let sorted = arrow::compute::Take(table, indices).unwrap();
    let output = std::move(sorted).table();
    println!("{}", output);

    Ok(())
}

fn main() {
    compute_parquet().unwrap();
    find_minmax().unwrap();
}
