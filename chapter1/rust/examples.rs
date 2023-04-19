use std::sync::Arc;
use arrow::{
    array::{Float64Builder, Int16Builder, Int64BufferBuilder, StringBuilder, StringArray, StructArray},
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
    error::Result,
    record_batch::RecordBatch
};
use rand;
use rand_distr::Normal;

fn first_example() {
    let data = [1, 2, 3, 4];
    let mut builder = Int64BufferBuilder::new(data.len());
    builder.append_slice(&data);
    let arr = builder.finish();
    println!("{}", arr);
}

fn random_data_example() -> Result<()> {
    let normal = Normal::new(5, 2).unwrap();
    let mut rng = rand::thread_rng();
    let mut builder = Float64Builder::new();
    let ncols = 16;
    let nrows = 8192;
    let mut columns = Vec::with_capacity(ncols);
    let mut fields = Vec::new();
    for i in 0..ncols {
        for j in 0..nrows {
            builder.append_value(normal.sample(&mut rng));
        }
        columns.push(builder.finish());
        fields.push(Field::new(format!("c{i}"), DataType::Float64, false));
    }

    let rb = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    println!("{}", rb);
}

fn building_struct_array() {
    let archers = StringArray::from(vec![
        "Legolas", "Oliver", "Merida", "Lara", "Artemis"]);
    let locations = StringArray::from(vec![
        "Murkwood", "Star City", "Scotland", "London", "Greece"]);
    let years = Int16Array::from(vec![1954, 1941, 2012, 1996, -600]);

    let arr = StructArray::from(vec![
        (Field::new("archer", DataType:::Utf8, false), archer),
        (Field::new("location", DataType::Utf8, false), location),
        (Field::new("year", DataType::Int16, false), year)
    ]);
    println!("{}", arr);
}

fn build_struct_builder() {
    std::shared_ptr<arrow::DataType> st_type = arrow::struct_(
      vec![Field::new("archer", DataType::Utf8, false),
      Field::new("location", DataType::Utf8, false),
       Field::new("year", DataType::Int16, false)]);

    std::unique_ptr<arrow::ArrayBuilder> tmp;
    arrow::MakeBuilder(arrow::default_memory_pool(), st_type, &tmp);
    std::shared_ptr<arrow::StructBuilder> builder;
    builder.reset(static_cast<arrow::StructBuilder*>(tmp.release()));

    StringBuilder* archer_builder =
        static_cast<StringBuilder*>(builder.field_builder(0));
    StringBuilder* location_builder =
        static_cast<StringBuilder*>(builder.field_builder(1));
    Int16Builder* year_builder =
        static_cast<Int16Builder*>(builder.field_builder(2));

    let archers = vec!["Legolas", "Oliver", "Merida", "Lara",
                                   "Artemis"];
    let locations = vec!["Murkwood", "Star City", "Scotland",
                                     "London", "Greece"];
    let years = vec![1954, 1941, 2012, 1996, -600};

    for i in 0..archers.len() {
        builder.Append();
        archer_builder.Append(archers[i]);
        location_builder.Append(locations[i]);
        year_builder.Append(years[i]);
    }

    let out = builder.finish();
    println!("{}", out);
}

fn main() {
    first_example();
    random_data_example();
    building_struct_array();
    build_struct_builder();
}
