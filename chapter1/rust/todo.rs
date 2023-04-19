fn building_struct_array() {
  using arrow::field;
  using arrow::int16;
  using arrow::utf8;
    let children = Vec::with_capacity(3);

    let archers = vec!["Legolas", "Oliver", "Merida", "Lara", "Artemis"];
    let locations = vec!["Murkwood", "Star City", "Scotland", "London", "Greece"];
    let years = vec![1954, 1941, 2012, 1996, -600];

    let mut str_builder = StringBuilder::new();
    str_builder.append_values(archers);
    str_bldr.Finish(&children[0]);
  str_bldr.AppendValues(locations);
  str_bldr.Finish(&children[1]);
  arrow::Int16Builder year_bldr;
  year_bldr.AppendValues(years);
  year_bldr.Finish(&children[2]);

  arrow::StructArray arr{
      arrow::struct_({field("archer", utf8()), field("location", utf8()),
                      field("year", int16())}),
      children[0]->length(), children};
  std::cout << arr.ToString() << std::endl;
}

fn build_struct_builder() {
  using arrow::field;
  std::shared_ptr<arrow::DataType> st_type = arrow::struct_(
      {field("archer", arrow::utf8()), field("location", arrow::utf8()),
       field("year", arrow::int16())});

  std::unique_ptr<arrow::ArrayBuilder> tmp;
  arrow::MakeBuilder(arrow::default_memory_pool(), st_type, &tmp);
  std::shared_ptr<arrow::StructBuilder> builder;
  builder.reset(static_cast<arrow::StructBuilder*>(tmp.release()));

  using namespace arrow;
  StringBuilder* archer_builder =
      static_cast<StringBuilder*>(builder->field_builder(0));
  StringBuilder* location_builder =
      static_cast<StringBuilder*>(builder->field_builder(1));
  Int16Builder* year_builder =
      static_cast<Int16Builder*>(builder->field_builder(2));

  std::vector<std::string> archers{"Legolas", "Oliver", "Merida", "Lara",
                                   "Artemis"};
  std::vector<std::string> locations{"Murkwood", "Star City", "Scotland",
                                     "London", "Greece"};
  std::vector<int16_t> years{1954, 1941, 2012, 1996, -600};

  for (int i = 0; i < archers.size(); ++i) {
    builder->Append();
    archer_builder->Append(archers[i]);
    location_builder->Append(locations[i]);
    year_builder->Append(years[i]);
  }

  std::shared_ptr<arrow::Array> out;
  builder->Finish(&out);
  std::cout << out->ToString() << std::endl;
}

fn main() {
  first_example();
  random_data_example();
  building_struct_array();
  build_struct_builder();
}
