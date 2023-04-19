use std::fs::File;
use arrow::{
    compute as cp,
    datatypes::{DataType, Field, Schema},
    error::Result
};
use aws_config;
use aws_sdk_s3;
use datafusion::{
    datasource::{
        datasource::TableProvider,
        file_format::parquet::ParquetFormat
    },
    physical_plan::{
        ExecutionPlan,
        file_format::FileMeta,
    },
    prelude::*
};
use object_store::{
    ObjectMeta,
    aws::AmazonS3Builder
};
// use super::timer;

fn create_dataset() -> Result<impl TableProvider> {
    let ctx = SessionContext::new();
    let s3 = AmazonS3Builder::new().with_region("us-east-2").build();
    let format = ParquetFormat::new();
    // check all the subdirectories
    let selector = fs::FileSelector;
    selector.base_dir = "ursa-labs-taxi-data";
    selector.recursive = true;

    let options = ds::FileSystemFactoryOptions;
    options.partitioning =
        ds::DirectoryPartitioning::MakeFactory(vec!["year", "month"]);
    let factory = 
        ds::FileSystemDatasetFactory::Make(s3, selector, format, options).unwrap();

    factory.finish()
}

fn calc_mean(dataset: impl TableProvider) -> Result<()> {
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

fn grouped_mean(dataset: impl TableProvider) -> arrow::Status {
    let ctx = SessionContext::new();
    // options.use_threads = true;
    let projection = ds::ProjectionDescr::FromNames(
        {"vendor_id", "passenger_count"},
        *dataset.schema()).unwrap();
    let scanner = dataset.scan(
        &ctx.state(),
        Some(projection),
        todo!(),
        None
    );

    let options = std::make_shared<ds::ScanOptions>();
    
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
        Field::new("mean(passenger_count)", DataType::Float64, false),
        Field::new("vendor_id", DataType::Utf8, false)
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

fn grouped_filtered_mean(dataset: impl TableProvider) -> arrow::Status {
    let ctx = SessionContext::new();
    // use_threads = true;
    let projection = ds::ProjectionDescr::FromNames(
        {"passenger_count", "year"}, *dataset.schema()).unwrap()
    let scanner = dataset.scan(
        &ctx.state(),
        Some(projection),
        col("year").gt(lit(2015)),
        None
    );

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
