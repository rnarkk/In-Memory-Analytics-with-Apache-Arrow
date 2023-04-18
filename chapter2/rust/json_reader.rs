use std::fs::File;
use arrow::json;

fn main() {
    let read_options = arrow::json::ReadOptions::Defaults();
    let parse_options = arrow::json::ParseOptions::Defaults();
    let filename = "sample.json";  // could be any json file

    let file = File::open(filename).unwrap();

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    let maybe_reader =
        arrow::json::TableReader::Make(pool, file, read_options, parse_options);
    if !maybe_reader.ok() {
        std::cerr << maybe_reader.status().message() << std::endl;
        return 1;
    }
    let reader = std::shared_ptr<arrow::json::TableReader> = *maybe_reader;
    let maybe_table = reader.read();
    if (!maybe_table.ok()) {
        std::cerr << maybe_table.status().message() << std::endl;
        return 1;
    }

    std::shared_ptr<arrow::Table> table = *maybe_table;
    println!("{}", table);
}
