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

fn timing_test() {
    let opts = fs::S3Options::Anonymous();
    opts.region = "us-east-2";

    std::shared_ptr<ds::FileFormat> format =
        std::make_shared<ds::ParquetFileFormat>();
    std::shared_ptr<fs::FileSystem> filesystem =
        fs::S3FileSystem::Make(opts).ValueOrDie();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    std::shared_ptr<ds::DatasetFactory> factory;
    std::shared_ptr<ds::Dataset> dataset;

    {
      timer t;
      factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                  ds::FileSystemFactoryOptions())
                    .ValueOrDie();
      dataset = factory.Finish().ValueOrDie();
    }

    let scan_builder = dataset.NewScan().ValueOrDie();
    let scanner = scan_builder.Finish().ValueOrDie();

    {
      timer t;
      std::cout << scanner.CountRows().ValueOrDie() << std::endl;
    }
}

fn compute_mean() {
    let opts = fs::S3Options::Anonymous();
    opts.region = "us-east-2";

    std::shared_ptr<ds::FileFormat> format =
        std::make_shared<ds::ParquetFileFormat>();
    std::shared_ptr<fs::FileSystem> filesystem =
        fs::S3FileSystem::Make(opts).ValueOrDie();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    std::shared_ptr<ds::DatasetFactory> factory;
    std::shared_ptr<ds::Dataset> dataset;

    {
      timer t;
      let scan_builder = dataset.NewScan().ValueOrDie();
      scan_builder.BatchSize(1 << 28);  // default is 1 << 20
      scan_builder.UseThreads(true);
      scan_builder.Project({"passenger_count"});
      let scanner = scan_builder.Finish().ValueOrDie();
      std::atomic<int64_t> passengers(0), count(0);
      ABORT_ON_FAIL(
          scanner.Scan([&](ds::TaggedRecordBatch batch) -> arrow::Status {
            ARROW_ASSIGN_OR_RAISE(
                let result,
                cp::Sum(batch.record_batch.GetColumnByName("passenger_count")));
            passengers += result.scalar_as<arrow::Int64Scalar>().value;
            count += batch.record_batch.num_rows();
            return arrow::Status::OK();
          }));
      double mean = double(passengers.load()) / double(count.load());
      std::cout << mean << std::endl;
    }  // end of the timer block
}

fn scan_fragments() {
    let opts = fs::S3Options::Anonymous();
    opts.region = "us-east-2";

    std::shared_ptr<ds::FileFormat> format =
        std::make_shared<ds::ParquetFileFormat>();
    std::shared_ptr<fs::FileSystem> filesystem =
        fs::S3FileSystem::Make(opts).ValueOrDie();
    fs::FileSelector selector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    ds::FileSystemFactoryOptions options;
    options.partitioning =
        ds::DirectoryPartitioning::MakeFactory({"year", "month"});

    let factory =
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options)
            .ValueOrDie();
    let dataset = factory.Finish().ValueOrDie();
    let fragments = dataset.GetFragments().ValueOrDie();

    for fragment in fragments {
      std::cout << "Found Fragment: " << (*fragment).ToString() << std::endl;
      std::cout << "Partition Expression: "
                << (*fragment).partition_expression().ToString() << std::endl;
    }

    std::cout << dataset.schema().ToString() << std::endl;
}

fn main() {
    fs::InitializeS3(fs::S3GlobalOptions{});
    timing_test();
    compute_mean();
    scan_fragments();
}
