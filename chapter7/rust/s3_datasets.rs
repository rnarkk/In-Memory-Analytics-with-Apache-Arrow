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

    std::shared_ptr<ds::DatasetFactory> factory;
    std::shared_ptr<ds::Dataset> dataset;

    {
      timer t;
      factory = ds::FileSystemDatasetFactory::Make(s3, selector, format,
                                                  ds::FileSystemFactoryOptions())
                    .unwrap();
      dataset = factory.Finish().unwrap();
    }

    let scan_builder = dataset.scan().unwrap();
    let scanner = scan_builder.Finish().unwrap();

    {
      timer t;
      std::cout << scanner.CountRows().unwrap() << std::endl;
    }
}

fn compute_mean() {
    let s3 = AmazonS3Builder::new().region("us-east-2");
    let format = ParquetFormat::new();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    std::shared_ptr<ds::DatasetFactory> factory;
    std::shared_ptr<ds::Dataset> dataset;

    {
      timer t;
      let scan_builder = dataset.scan().unwrap();
      scan_builder.BatchSize(1 << 28);  // default is 1 << 20
      scan_builder.UseThreads(true);
      scan_builder.Project({"passenger_count"});
      let scanner = scan_builder.Finish().unwrap();
      std::atomic<int64_t> passengers(0), count(0);
      ABORT_ON_FAIL(
          scanner.Scan(|ds::TaggedRecordBatch batch| -> arrow::Status {
            let result = cp::Sum(batch.record_batch.column_by_name("passenger_count")).unwrap();
            passengers += result.scalar_as<arrow::Int64Scalar>().value;
            count += batch.record_batch.num_rows();
            return arrow::Status::OK();
          }));
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
    options.partitioning =
        ds::DirectoryPartitioning::MakeFactory({"year", "month"});

    let factory =
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options).unwrap();
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
