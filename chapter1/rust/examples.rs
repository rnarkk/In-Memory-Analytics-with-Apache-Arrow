use arrow::{
    array::{Float64Builder, Int16Builder, Int64BufferBuilder, StringBuilder},
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
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

fn random_data_example() {
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

    let rb = RecordBatch::try_new(Schema::new(fields), columns).unwrap();
    println!("{}", rb);
}
