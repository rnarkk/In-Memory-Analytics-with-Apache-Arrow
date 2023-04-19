use std::fs::File;
use arrow::{
    datatypes::{DataType, Field, Schema},
    error::Result
};
use datafusion::{
    datasource::{
        datasource::TableProvider,
        file_format::{
            csv::CsvFormat,
            parquet::ParquetFormat
        }
    },
    prelude::*
};
use aws_config::SdkConfig;
use object_store::{
    aws::AmazonS3Builder,
    local::LocalFileSystem
};

fn create_dataset() -> Result<impl TableProvider> {
    let s3 = AmazonS3Builder::new().with_region("us-east-2").build();
    let format = ParquetFormat::new();
    let selector = fs::FileSelector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    let options = ds::FileSystemFactoryOptions;
    options.partitioning =
        ds::DirectoryPartitioning::MakeFactory({"year", "month"});
    let factory =
        ds::FileSystemDatasetFactory::Make(s3, selector,
                                        format, options).unwrap();
    let finopts = ds::FinishOptions;
    finopts.validate_fragments = true;
    finopts.inspect_options.fragments = ds::InspectOptions::kInspectAllFragments;
    factory.finish(finopts);
}

fn write_dataset(dataset: impl TableProvider) -> Result<()> {
    let ctx = SessionContext::new();
    let scanner = dataset.scan(
        &ctx.state(),
        None,
        and(col("year").gt_eq(lit(2014)),
            col("year").lt_eq(lit(2015))),
        Some(1 << 28)
    );
    // scan_builder.UseThreads(true);
    println!("{}", dataset.schema());

    let filesystem = LocalFileSystem::new();

    let base_path = "/home/zero/sample/csv_dataset";
    let format = CsvFormat::default().with_delimiter('|');
    let write_opts = ds::FileSystemDatasetWriteOptions;
    let csv_write_options = std::static_pointer_cast<ds::CsvFileWriteOptions>(
        format.DefaultWriteOptions());
    write_opts.file_write_options = csv_write_options;
    write_opts.filesystem = filesystem;
    write_opts.base_dir = base_path;
    write_opts.partitioning = std::make_shared<ds::HivePartitioning>(
        Schema::new(vec![
            Field::new("year", DataType::Int32(), false),
            Field::new("month", DataType::Int32(), false)
        ]));

    write_opts.basename_template = "part{i}.csv";
    ds::FileSystemDataset::Write(write_opts, scanner)
}

fn main() {
    // ignore SIGPIPE errors during S3 communication
    // so we don't randomly blow up and die
    signal(SIGPIPE, SIG_IGN);

    fs::InitializeS3(fs::S3GlobalOptions{});
    let dataset = create_dataset().unwrap();
    write_dataset(dataset).unwrap();
}
