use arrow::{
    csv::Reader,  // the csv functions and objects
    io::api,   // for opening the file
    table   // to read the data into a table
};
      // to output to the terminal

fn main() {
    let file = File::open("../../sample_data/train.csv").unwrap();
    let maybe_input =
        arrow::io::ReadableFile::Open("../../sample_data/train.csv");
    if !maybe_input.ok() {
        // handle any file open errors
        std::cerr << maybe_input.status().message() << std::endl;
        return 1;
    }

    std::shared_ptr<arrow::io::InputStream> input = *maybe_input;
    let io_context = arrow::io::default_io_context();
    let read_options = arrow::csv::ReadOptions::Defaults();
    let parse_options = arrow::csv::ParseOptions::Defaults();
    let convert_options = arrow::csv::ConvertOptions::Defaults();

    let maybe_reader = arrow::csv::TableReader::Make(
        io_context, input, read_options, parse_options, convert_options);

    if !maybe_reader.ok() {
        // handle any instantiation errors
        std::cerr << maybe_reader.status().message() << std::endl;
        return 1;
    }

    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;
    // finally read the data from the file
    let maybe_table = reader->Read();
    if !maybe_table.ok() {
        // handle any errors such as CSV syntax errors
        // or failed type conversion errors, etc.
        std::cerr << maybe_table.status().message() << std::endl;
        return 1;
    }

  std::shared_ptr<arrow::Table> table = *maybe_table;
  std::cout << table->ToString() << std::endl;
}
