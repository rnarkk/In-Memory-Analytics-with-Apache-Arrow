use arrow::{
    array::Int64Array,
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch
};

fn first_example() {
    let data = Vec<i64>![1, 2, 3, 4];
    let arr = std::make_shared<Int64Array>(data.len(), Buffer::Wrap(data));
    println!("{}", arr);
}

fn random_data_example() {
    let rd = std::random_device;
    let gen = std::mt19937 {rd()};
    let d = std::normal_distribution<> {5, 2};

    let builder = arrow::DoubleBuilder {DataType::Float64, pool};

    let ncols = 16;
    let nrows = 8192;
    let columns = arrow::ArrayVector (ncols);
    let mut fields = Vec::new();
    for i in 0..ncols {
        for j in 0..nrows {
            builder.Append(d(gen));
        }
        builder.Finish(&columns[i]).unwrap();
        fields.push(Field::new(format!("c{i}"), DataType::Float64, false));
    }

    let rb = RecordBatch::try_new(Schema::new(fields), columns).unwrap();
    println!("{}", rb);
}
