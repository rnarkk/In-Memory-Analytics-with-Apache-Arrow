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
    datasource::file_format::FileFormat,
    prelude::*
};
use parquet;

fn create_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false)
    ]);
    let array_a = Int64Array::from([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let array_b = Int64Array::from([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
    let array_c = Int64Array::from([1, 2, 1, 2, 1, 2, 1, 2, 1, 2]);
    RecordBatch::try_new(Arc::new(schema), vec![array_a, array_b, array_c]).unwrap()
}

fn create_sample_dataset(
        filesystem: &const std::shared_ptr<fs::FileSystem>,
        root_path: &str) -> std::string {
    let base_path = root_path + "/parquet_dataset";
    ABORT_ON_FAIL(filesystem.CreateDir(base_path));
    let batch = create_batch();
    let output =
        filesystem.OpenOutputStream(base_path + "/data1.parquet").unwrap();
    ABORT_ON_FAIL(parquet::arrow::WriteTable(*batch.Slice(0, 5),
                                            arrow::default_memory_pool(), output,
                                            /*chunk_size*/ 2048));
    output =
        filesystem.OpenOutputStream(base_path + "/data2.parquet").unwrap();
    ABORT_ON_FAIL(parquet::arrow::WriteTable(*batch.Slice(5),
                                            arrow::default_memory_pool(), output,
                                            /*chunk_size*/ 2048));
    base_path
}

fn scan_dataset(
    filesystem: const std::shared_ptr<fs::FileSystem>&,
    format: impl FileFormat,
    base_dir: &str) -> std::shared_ptr<arrow::Table> {
    fs::FileSelector selector;
    selector.base_dir = base_dir;
    let factory =
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                            ds::FileSystemFactoryOptions())
            .unwrap();

    let dataset = factory.finish().unwrap();
    let fragments = dataset.GetFragments().unwrap();
    for fragment in fragments {
        println!("Found Fragment: {}", fragment);
    }

    let scan_builder = dataset.NewScan().unwrap();
    let scanner = scan_builder.finish().unwrap();
    return scanner.ToTable().unwrap();
}

fn filter_and_select(
    filesystem: const std::shared_ptr<fs::FileSystem>&,
    format: const std::shared_ptr<ds::FileFormat>&,
    base_dir: &str) -> std::shared_ptr<arrow::Table> {
    fs::FileSelector selector;
    selector.base_dir = base_dir;
    let factory =
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                            ds::FileSystemFactoryOptions())
            .unwrap();

    let dataset = factory.finish().unwrap();
    let scan_builder = dataset.NewScan().unwrap();
    ABORT_ON_FAIL(scan_builder.Project({"b"}));
    ABORT_ON_FAIL(
        scan_builder.Filter(cp::less(cp::field_ref("b"), cp::literal(4))));
    let scanner = scan_builder.finish().unwrap();
    return scanner.ToTable().unwrap();
}

fn derive_and_rename(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format,
    base_dir: &str) -> std::shared_ptr<arrow::Table> {
    fs::FileSelector selector;
    selector.base_dir = base_dir;
    let factory =
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                            ds::FileSystemFactoryOptions())
            .unwrap();

    let dataset = factory.finish().unwrap();
    let scan_builder = dataset.NewScan().unwrap();
    let mut names = Vec::new();
    let mut exprs = Vec::new();
    for  field in dataset.schema().fields() {
        names.push(field.name());
        exprs.push(cp::field_ref(field.name()));
    }
    names.emplace_back("b_as_float32");
    exprs.push(cp::call("cast", {cp::field_ref("b")},
                            cp::CastOptions::Safe(arrow::float32())));

    names.emplace_back("b_large");
    // b > 1
    exprs.push(cp::greater(cp::field_ref("b"), cp::literal(1)));
    ABORT_ON_FAIL(scan_builder.Project(exprs, names));
    let scanner = scan_builder.finish().unwrap();
    return scanner.ToTable().unwrap();
}

fn main() {
    std::shared_ptr<fs::FileSystem> filesystem =
        std::make_shared<fs::LocalFileSystem>();
    let path = create_sample_dataset(filesystem, "/home/zero/sample");
    println!("{}", path);

    std::shared_ptr<ds::FileFormat> format =
        std::make_shared<ds::ParquetFileFormat>();

    let table =
        scan_dataset(filesystem, format, "/home/zero/sample/parquet_dataset");
    println!("{}", table);
    table = filter_and_select(filesystem, format,
                              "/home/zero/sample/parquet_dataset");
    println!("{}", table);
    table = derive_and_rename(filesystem, format,
                              "/home/zero/sample/parquet_dataset");
    println!("{}", table);
}
