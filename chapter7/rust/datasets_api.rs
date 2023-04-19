use std::{
    fs::File,
    sync::Arc
};
use arrow::{
    array::{Int16Array, Int64Array, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
    compute as cp,
    error::Result,
    record_batch::RecordBatch
};
use datafusion::{
    datasource::file_format::{
        FileFormat,
        parquet::ParquetFormat,
    },
    prelude::*
};
use object_store::{
    ObjectStore,
    local::LocalFileSystem
};
use parquet::arrow::arrow_writer::ArrowWriter;

fn create_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false)
    ]);
    let array_a = Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let array_b = Int64Array::from(vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
    let array_c = Int64Array::from(vec![1, 2, 1, 2, 1, 2, 1, 2, 1, 2]);
    RecordBatch::try_new(Arc::new(schema), vec![array_a, array_b, array_c]).unwrap()
}

fn create_sample_dataset(filesystem: impl ObjectStore, root_path: &str) -> &str
{
    let base_path = root_path + "/parquet_dataset";
    filesystem.CreateDir(base_path).unwrap();
    let batch = create_batch();
    let output =
        filesystem.OpenOutputStream(base_path + "/data1.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(output, batch.schema(), None);
    writer.write(*batch.slice(0, 5), /*chunk_size*/ 2048).unwrap();
    output =
        filesystem.OpenOutputStream(base_path + "/data2.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(output, batch.schema(), None);
    writer.write(*batch.Slice(5), /*chunk_size*/ 2048).unwrap();
    base_path
}

fn scan_dataset(store: impl ObjectStore, format: impl FileFormat, path: &str)
    -> std::shared_ptr<arrow::Table>
{
    fs::FileSelector selector;
    selector.base_dir = path;
    let factory =
        ds::FileSystemDatasetFactory::Make(store, selector, format,
                                            ds::FileSystemFactoryOptions())
            .unwrap();

    let dataset = factory.finish().unwrap();
    let fragments = dataset.GetFragments().unwrap();
    for fragment in fragments {
        println!("Found Fragment: {}", fragment);
    }

    let scan_builder = dataset.NewScan().unwrap();
    let scanner = scan_builder.finish().unwrap();
    scanner.ToTable().unwrap()
}

fn filter_and_select(store: impl ObjectStore, format: impl FileFormat, path: &str)
    -> std::shared_ptr<arrow::Table>
{
    fs::FileSelector selector;
    selector.base_dir = path;
    let factory =
        ds::FileSystemDatasetFactory::Make(store, selector, format,
                                            ds::FileSystemFactoryOptions())
            .unwrap();

    let dataset = factory.finish().unwrap();
    // let projection = ds::ProjectionDescr::FromNames(
    //     {"vendor_id", "passenger_count"},
    //     *dataset.schema()).unwrap();
    let scanner = dataset.scan(
        &ctx.state(),
        Some("b"),
        col("b").lt(lit(4)),
        None
    );
    scanner.ToTable().unwrap()
}

fn derive_and_rename(store: impl ObjectStore, format: impl FileFormat,
                     path: &str)
    -> std::shared_ptr<arrow::Table>
{
    fs::FileSelector selector;
    selector.base_dir = path;
    let factory =
        ds::FileSystemDatasetFactory::Make(store, selector, format,
                                            ds::FileSystemFactoryOptions())
            .unwrap();

    let dataset = factory.finish().unwrap();
    let scan_builder = dataset.NewScan().unwrap();
    let mut names = Vec::new();
    let mut exprs = Vec::new();
    for field in dataset.schema().fields() {
        names.push(field.name());
        exprs.push(col(field.name()));
    }
    names.emplace_back("b_as_float32");
    exprs.push(cp::call("cast", {col("b")},
                            cp::CastOptions::Safe(arrow::float32())));

    names.emplace_back("b_large");
    // b > 1
    exprs.push(col("b").gt(lit(1)));
    ABORT_ON_FAIL(scan_builder.Project(exprs, names));
    let scanner = scan_builder.finish().unwrap();
    scanner.ToTable().unwrap()
}

fn main() {
    let file_system = LocalFileSystem::new();
    let path = create_sample_dataset(file_system, "/home/zero/sample");
    println!("{}", path);

    let format = ParquetFormat::new();
    let path = "/home/zero/sample/parquet_dataset";
    let table = scan_dataset(file_system, format, path);
    println!("{}", table);
    let table = filter_and_select(file_system, format, path);
    println!("{}", table);
    let table = derive_and_rename(file_system, format, path);
    println!("{}", table);
}
