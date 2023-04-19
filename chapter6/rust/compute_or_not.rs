use arrow::{
    array::{Int32Array, Int32Builder},
    buffer::Buffer,
    compute::*,
};
use super::timer;

fn main() {
    for n in (10000..=10000000).skip(10000) {
        println!("N: {}", n);
        let values: Vec<i32> = (0..n).collect();
        let arr = Int32Array::from(values);

        let res1 = {
            let t = timer;
            add_scalar(&arr, 2i32)
        };

        let res2 = {
            let t = timer;
            let b = Int32Builder::new();
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    b.append(arr.value(i) + 2);
                } else {
                    b.append_null();
                }
            }
            b.finish()
        };
        println!("{}", res1 == res2);

        let res3 =
        {
            let t = timer;
            let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
            let b = Int32Builder::with_capacity(arr.len());
            arr.for_each(|v: &arrow::util::optional<i32>| {
                    if v {
                        b.append(*v + 2);
                    } else {
                        b.append_null();
                    }
                });
            b.finish()
        };
        println!("{}", res1 == res3);

        let res4 =
        {
            let t = timer;
            let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
            let newbuf = arrow::AllocateBuffer(sizeof(i32) * arr.len())
                    .MoveValueUnsafe();
            let output = reinterpret_cast<*i32>(newbuf.mutable_data());
            std::transform(arr.raw_values(), arr.raw_values() + arr.len(),
                            output, |v: i32| v + 2);

            arrow::MakeArray(
                ArrayData::new(arr.r#type(), arr.len(),
                    Vec<std::shared_ptr<Buffer>>{arr.null_bitmap(), newbuf},
                    arr.null_count()))
        };
        println!("{}", res1 == res4);
    }
}
