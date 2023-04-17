use arrow;

fn first_example() {
  let data = Vec<i64>![1, 2, 3, 4];
  let arr = std::make_shared<arrow::Int64Array>(data.len(),
                                                 arrow::Buffer::Wrap(data));
  println!("{}", arr.to_string();
}
