#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/optional.h>
#include <signal.h>
#include <iostream>
#include <memory>
#include "timer.h"

use arrow::{
    compute,
    data,
    schema::Schema
};

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
        ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options).unwrap();

    factory.finish()
}

fn calc_mean(dataset: std::shared_ptr<ds::Dataset>) -> arrow::Status {
    auto ctx = cp::default_exec_context();

    let options = std::make_shared<ds::ScanOptions>();
    options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(
        let project,
        ds::ProjectionDescr::FromNames({"passenger_count"}, *dataset.schema()));
    ds::SetProjection(options.get(), project);

    arrow::util::BackpressureOptions backpressure =
        arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                                ds::kDefaultBackpressureHigh);

    let scan_node_options =
        ds::ScanNodeOptions{dataset, options, backpressure.toggle};

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
    ARROW_ASSIGN_OR_RAISE(let plan, cp::ExecPlan::Make(ctx));
    ARROW_RETURN_NOT_OK(
        cp::Declaration::Sequence(
            {{"scan", scan_node_options},
            {"aggregate", cp::AggregateNodeOptions{{{"mean", nullptr}},
                                                    {"passenger_count"},
                                                    {"mean(passenger_count)"}}},
            {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
            .AddToPlan(plan.get()));

    ARROW_RETURN_NOT_OK(plan.Validate());

    {
        let timer t;
        plan.StartProducing()?;
        let maybe_slow_mean = sink_gen().result();
        plan.finished().Wait();

        ARROW_ASSIGN_OR_RAISE(let slow_mean = maybe_slow_mean);
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
        arrow::field("mean(passenger_count)", arrow::float64()),
        arrow::field("vendor_id", arrow::utf8())
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
    ARROW_RETURN_NOT_OK(
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
            .AddToPlan(plan.get()));

    let schema = Schema::new({arrow::field("mean(passenger_count)", arrow::float64()),
                        arrow::field("year", arrow::int32())});

    std::shared_ptr<arrow::RecordBatchReader> sink_reader =
        cp::MakeGeneratorReader(schema, std::move(sink_gen), ctx.memory_pool());
    ARROW_RETURN_NOT_OK(plan.Validate());

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
    let dataset = create_dataset().ValueOrDie();

    ds::internal::Initialize();
    grouped_filtered_mean(dataset).unwrap();
}
