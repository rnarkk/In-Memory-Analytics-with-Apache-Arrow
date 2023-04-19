use arrow::{
    array::{Float64Builder, Int64Array},
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch
};
use rand;
use rand_distr::Normal;

fn first_example() {
    let data: Vec<i64> = vec![1, 2, 3, 4];
    let arr = std::make_shared<Int64Array>(data.len(), Buffer::Wrap(data));
    println!("{}", arr);
}

fn random_data_example() {
    let normal = Normal::new(5, 2).unwrap();
    let mut rng = rand::thread_rng();
    let builder = Float64Builder::new();
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

    let rb = RecordBatch::try_new(Schema::new(fields), columns).unwrap();
    println!("{}", rb);
}
