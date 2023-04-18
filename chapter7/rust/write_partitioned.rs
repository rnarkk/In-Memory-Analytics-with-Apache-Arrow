use arrow::{
    compute,
    data,
};
#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <iostream>

namespace fs = arrow::fs;
namespace ds = arrow::dataset;
namespace cp = arrow::compute;

fn create_dataset() -> arrow::Result<std::shared_ptr<ds::Dataset>> {
    let opts = fs::S3Options::Anonymous();
    opts.region = "us-east-2";

    let format: std::shared_ptr<ds::FileFormat> =
        std::make_shared<ds::ParquetFileFormat>();
    let filesystem: std::shared_ptr<fs::FileSystem> =
        fs::S3FileSystem::Make(opts).ValueOrDie();
    let selector = fs::FileSelector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    let options = ds::FileSystemFactoryOptions;
    options.partitioning =
        ds::DirectoryPartitioning::MakeFactory({"year", "month"});
    let factory =
        ds::FileSystemDatasetFactory::Make(filesystem, selector,
                                        format, options).unwrap();
    let finopts = ds::FinishOptions;
    finopts.validate_fragments = true;
    finopts.inspect_options.fragments = ds::InspectOptions::kInspectAllFragments;
    factory.Finish(finopts);
}

fn write_dataset(dataset: std::shared_ptr<ds::Dataset>) -> arrow::Status {
    let scan_builder = dataset.NewScan().unwrap();
    scan_builder.UseThreads(true);
    scan_builder.BatchSize(1 << 28);
    scan_builder.Filter(cp::and_({
        cp::greater_equal(cp::field_ref("year"), cp::literal(2014)),
        cp::less_equal(cp::field_ref("year"), cp::literal(2015)),
    }));
    let scanner = scan_builder.Finish().unwrap();
    std::cout << dataset.schema().ToString() << std::endl;

    let filesystem: std::shared_ptr<fs::FileSystem> =
        std::make_shared<fs::LocalFileSystem>();

    let base_path = "/home/zero/sample/csv_dataset";
    let format = std::make_shared<ds::CsvFileFormat>();
    let write_opts = ds::FileSystemDatasetWriteOptions;
    let csv_write_options = std::static_pointer_cast<ds::CsvFileWriteOptions>(
        format.DefaultWriteOptions());
    csv_write_options.write_options.delimiter = '|';
    write_opts.file_write_options = csv_write_options;
    write_opts.filesystem = filesystem;
    write_opts.base_dir = base_path;
    write_opts.partitioning = std::make_shared<ds::HivePartitioning>(
        arrow::schema({arrow::field("year", arrow::int32()),
                        arrow::field("month", arrow::int32())}));

    write_opts.basename_template = "part{i}.csv";
    return ds::FileSystemDataset::Write(write_opts, scanner);
}

fn main() {
  // ignore SIGPIPE errors during S3 communication
  // so we don't randomly blow up and die
  signal(SIGPIPE, SIG_IGN);

  fs::InitializeS3(fs::S3GlobalOptions{});
  let dataset = create_dataset().unwrap();
  write_dataset(dataset).unwrap();
}
