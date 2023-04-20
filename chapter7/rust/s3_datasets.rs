use std::{
    fs::File,
    sync::{
        Arc,
        atomic::AtomicI64
    }
};
use arrow::{
    array::{Int16Array, Int64Array, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
    compute::*,
    error::Result,
    record_batch::RecordBatch
};
use datafusion::{
    datasource::{
        datasource::{TableProvider, TableProviderFactory},
        file_format::{
            FileFormat,
            parquet::ParquetFormat,
        }
    },
    prelude::*
};
use parquet;
use object_store::{
    ObjectStore,
    aws::AmazonS3Builder
};

fn timing_test() {
    let s3 = AmazonS3Builder::new().region("us-east-2");
    let format = ParquetFormat::new();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    let factory: TableProviderFactory;
    let dataset: TableProvider;
    {
        let t = timer;
        factory = ds::FileSystemDatasetFactory::Make(
            s3, selector, format, ds::FileSystemFactoryOptions())
                        .unwrap();
        dataset = factory.Finish().unwrap();
    }

    let scanner = dataset.scan().unwrap();
    {
        let t = timer;
        println!("{}", scanner.CountRows().unwrap());
    }
}

fn compute_mean() {
    let s3 = AmazonS3Builder::new().region("us-east-2");
    let format = ParquetFormat::new();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    let factory: TableProviderFactory;
    let dataset: TableProvider;

    {
        let t = timer;
        let scan_builder = dataset.scan().unwrap();
        scan_builder.BatchSize(1 << 28);  // default is 1 << 20
        scan_builder.UseThreads(true);
        scan_builder.Project({"passenger_count"});
        let scanner = scan_builder.Finish().unwrap();
        let passengers = AtomicI64::new(0);
        let count = AtomicI64::new(0);
        scanner.Scan(|batch: ds::TaggedRecordBatch| -> Result<()> {
            let result = sum(batch.record_batch.column_by_name("passenger_count")).unwrap();
            passengers += result.scalar_as<arrow::Int64Scalar>().value;
            count += batch.record_batch.num_rows();
            Ok(())
        }).unwrap();
        let mean = double(passengers.load()) / double(count.load());
        println!("{}", mean);
    }
}

fn scan_fragments() {
    let s3 = AmazonS3Builder::new().region("us-east-2");
    let format = ParquetFormat::new();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    ds::FileSystemFactoryOptions options;
    options.partitioning = ds::DirectoryPartitioning::MakeFactory({"year", "month"});

    let factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options).unwrap();
    let dataset = factory.Finish().unwrap();
    let fragments = dataset.GetFragments().unwrap();

    for fragment in fragments {
        println!("Found Fragment: {}", *fragment);
        println!("Partition Expression: {}", fragment.partition_expression());
    }

    println!("{}", dataset.schema());
}

fn main() {
    fs::InitializeS3(fs::S3GlobalOptions{});
    timing_test();
    compute_mean();
    scan_fragments();
}
