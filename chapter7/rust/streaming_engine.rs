use std::fs::File;
use arrow::{
    compute as cp,
    data,
    datatypes::{DataType, Field, Schema},
    error::Result
};
use aws_config;
use aws_sdk_s3;
use datafusion::prelude::*;
// use super::timer;

fn create_dataset() -> arrow::Result<std::shared_ptr<ds::Dataset>> {
    let opts = fs::S3Options::Anonymous();
    opts.region = "us-east-2";

    let format: std::shared_ptr<ds::FileFormat> =
        std::make_shared<ds::ParquetFileFormat>();
    let filesystem: std::shared_ptr<fs::FileSystem> =
        fs::S3FileSystem::Make(opts).unwrap();
    let selector = fs::FileSelector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;  // check all the subdirectories

    let options = ds::FileSystemFactoryOptions;
    options.partitioning =
        ds::DirectoryPartitioning::MakeFactory({"year", "month"});
    let factory = 
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options).unwrap();

    factory.finish()
}

fn calc_mean(dataset: std::shared_ptr<ds::Dataset>) -> Result<()> {
    let ctx = SessionContext::new();

    let options = std::make_shared<ds::ScanOptions>();
    options.use_threads = true;
    let project = ds::ProjectionDescr::FromNames({"passenger_count"}, *dataset.schema()).unwrap();
    ds::SetProjection(options.get(), project);

    let backpressure: arrow::util::BackpressureOptions =
        arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                                ds::kDefaultBackpressureHigh);

    let scan_node_options =
        ds::ScanNodeOptions{dataset, options, backpressure.toggle};

    let sink_gen = arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>>;
    let plan = cp::ExecPlan::Make(ctx).unwrap();
    cp::Declaration::Sequence(
        {{"scan", scan_node_options},
        {"aggregate", cp::AggregateNodeOptions{{{"mean", nullptr}},
                                                {"passenger_count"},
                                                {"mean(passenger_count)"}}},
        {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
        .AddToPlan(plan.get())?;

        plan.Validate()?;

    {
        let t = timer;
        plan.StartProducing()?;
        let maybe_slow_mean = sink_gen().result();
        plan.finished().Wait();

        let slow_mean = maybe_slow_mean.unwrap();
        println!("{}", slow_mean.values[0].scalar());
    }
    Ok(())
}

fn grouped_mean(dataset: std::shared_ptr<ds::Dataset>) -> arrow::Status {
    let ctx = cp::default_exec_context();

    let options = std::make_shared<ds::ScanOptions>();
    options.use_threads = true;
    let projection = ds::ProjectionDescr::FromNames(
        {"vendor_id", "passenger_count"},
        *dataset.schema()).unwrap();
    ds::SetProjection(options.get(), projection);

    let backpressure: arrow::util::BackpressureOptions =
        arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                                ds::kDefaultBackpressureHigh);

    let scan_node_options = ds::ScanNodeOptions{dataset, options, backpressure.toggle};

    let sink_gen = arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>>;
    let plan = cp::ExecPlan::Make(ctx).unwrap();
    cp::Declaration::Sequence(
        {{"scan", scan_node_options},
        {"aggregate", cp::AggregateNodeOptions{{{"hash_mean", nullptr}},
                                                {"passenger_count"},
                                                {"mean(passenger_count)"},
                                                {"vendor_id"}}},
        {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
        .AddToPlan(plan.get())?;

    let schema = Schema::new(vec![
        Field::new("mean(passenger_count)", DataType::Float64),
        Field::new("vendor_id", DataType::Utf8)
    ]);

    let sink_reader: std::shared_ptr<arrow::RecordBatchReader> =
        cp::MakeGeneratorReader(schema, std::move(sink_gen), ctx.memory_pool());
    plan.Validate()?;

    let response_table = std::shared_ptr<arrow::Table>;
    {
        let t = timer;
        plan.StartProducing()?;
        response_table = arrow::Table::FromRecordBatchReader(sink_reader.get()).unwrap();
    }
    println!("Results: {}", response_table);
    plan.StopProducing();
    let future = plan.finished();
    future.status()
}

fn grouped_filtered_mean(dataset: std::shared_ptr<ds::Dataset>) -> arrow::Status {
    let ctx = cp::default_exec_context();

    let options = std::make_shared<ds::ScanOptions>();
    options.use_threads = true;
    options.filter = cp::greater(cp::field_ref("year"), cp::literal(2015));
    let projection = ds::ProjectionDescr::FromNames(
            {"passenger_count", "year"}, *dataset.schema()).unwrap();
    ds::SetProjection(options.get(), projection);

    let backpressure = arrow::util::BackpressureOptions =
        arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                                ds::kDefaultBackpressureHigh);

    let scan_node_options = ds::ScanNodeOptions{dataset, options, backpressure.toggle};

    let sink_gen = arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>>;
    let plan = cp::ExecPlan::Make(ctx).unwrap();
    cp::Declaration::Sequence(
        {{"scan", scan_node_options},
        {"filter", cp::FilterNodeOptions{cp::greater(cp::field_ref("year"),
                                                        cp::literal(2015))}},
        {"project", cp::ProjectNodeOptions{{cp::field_ref("passenger_count"),
                                            cp::field_ref("year")},
                                            {"passenger_count", "year"}}},
        {"aggregate", cp::AggregateNodeOptions{{{"hash_mean", nullptr}},
                                                {"passenger_count"},
                                                {"mean(passenger_count)"},
                                                {"year"}}},
        {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
        .AddToPlan(plan.get())?;

    let schema = Schema::new(vec![
        Field::new("mean(passenger_count)", DataType::Float64),
        Field::new("year", DataType::Int32)
    ]);

    let sink_reader: std::shared_ptr<arrow::RecordBatchReader> =
        cp::MakeGeneratorReader(schema, std::move(sink_gen), ctx.memory_pool());
    plan.Validate()?;

    let response_table: std::shared_ptr<arrow::Table>;
    {
        let t = timer;
        plan.StartProducing()?;
        response_table = arrow::Table::FromRecordBatchReader(sink_reader.get()).unwrap();
    }
    println!("Results: {}", response_table);
    plan.StopProducing();
    let future = plan.finished();
    future.status()
}

fn main() {
    // ignore SIGPIPE errors during S3 communication
    // so we don't randomly blow up and die
    signal(SIGPIPE, SIG_IGN);

    fs::InitializeS3(fs::S3GlobalOptions{});
    let dataset = create_dataset().unwrap();

    ds::internal::Initialize();
    grouped_filtered_mean(dataset).unwrap();
}
