use arrow::{
    array::Int64Array,
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
    let builder = arrow::DoubleBuilder {DataType::Float64, pool};
    let ncols = 16;
    let nrows = 8192;
    let columns = arrow::ArrayVector (ncols);
    let mut fields = Vec::new();
    for i in 0..ncols {
        for j in 0..nrows {
            builder.append(normal.sample(&mut rng));
        }
        builder.Finish(&columns[i]).unwrap();
        fields.push(Field::new(format!("c{i}"), DataType::Float64, false));
    }

    let rb = RecordBatch::try_new(Schema::new(fields), columns).unwrap();
    println!("{}", rb);
}
