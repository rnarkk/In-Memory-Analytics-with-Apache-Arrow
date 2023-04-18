use arrow::{
    buffer
};

fn first_example() {
    let data = Vec<i64>![1, 2, 3, 4];
    let arr = std::make_shared<arrow::Int64Array>(data.len(),
                                                 arrow::Buffer::Wrap(data));
    println!("{}", arr.to_string();
}

fn random_data_example() {
    std::random_device rd;
    std::mt19937 gen{rd()};
    std::normal_distribution<> d{5, 2};

    let pool = arrow::default_memory_pool();
    arrow::DoubleBuilder builder{arrow::float64(), pool};

    constexpr auto ncols = 16;
    constexpr auto nrows = 8192;
    arrow::ArrayVector columns(ncols);
    arrow::FieldVector fields;
    for (int i = 0; i < ncols; ++i) {
        for (int j = 0; j < nrows; ++j) {
           builder.Append(d(gen));
        }
        auto status = builder.Finish(&columns[i]);
    if (!status.ok()) {
      std::cerr << status.message() << std::endl;
      // do something!
      return;
    }
        fields.push_back(arrow::field("c" + std::to_string(i), arrow::float64()));
    }

  auto rb = arrow::RecordBatch::Make(arrow::schema(fields),
                                     columns[0]->length(), columns);
  std::cout << rb->ToString() << std::endl;
}
