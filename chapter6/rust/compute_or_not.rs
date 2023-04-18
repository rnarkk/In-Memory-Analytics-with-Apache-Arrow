use arrow::{
    array::{Array, ArrayData, Int32Builder},
    buffer::Buffer,
    compute as cp,
    data
};
use super::timer;

fn main() {
    for (int n = 10000; n <= 10000000; n += 10000) {
        let testvalues = Vec<i32>(n);
        std::iota(std::begin(testvalues), std::end(testvalues), 0);

        let nb = Int32Builder::new();
        nb.append_values(testvalues);
        let numarr = nb.finish();

        println!("N: {}", n);

        let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);

        let res1 = arrow::Datum;
        {
            let t = timer;
            res1 = cp::add(arr, arrow::Datum{2i32}).MoveValueUnsafe();
        }

        let res2 = arrow::Datum;
        {
            let t = timer;
            let b = Int32Builder::new();
            for i in 0..arr.len() {
                if arr.IsValid(i) {
                    b.append(arr.value(i) + 2);
                } else {
                    b.append_null();
                }
            }
            let output = b.finish();
            res2 = arrow::Datum{std::move(output)};
        }
        std::cout << std::boolalpha << (res1 == res2) << std::endl;

        let res3 = arrow::Datum;
        {
            let t = timer;
            let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
            let b = Int32Builder::with_capacity(arr.len());
            std::for_each(std::begin(*arr), std::end(*arr),
                [&b]|v: const arrow::util::optional<i32>&| {
                    if v {
                        b.append(*v + 2);
                    } else {
                        b.append_null();
                    }
                });
            let output = b.finish();
            res3 = arrow::Datum{std::move(output)};
        }
        std::cout << std::boolalpha << (res1 == res3) << std::endl;

        let res4 = arrow::Datum;
        {
            let t = timer;
            let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
            let newbuf = std::shared_ptr<Buffer> =
                arrow::AllocateBuffer(sizeof(i32) * arr.len())
                    .MoveValueUnsafe();
            let output = reinterpret_cast<*i32>(newbuf.mutable_data());
            std::transform(arr.raw_values(), arr.raw_values() + arr.len(),
                            output, |v: i32| v + 2);

            res4 = arrow::Datum{arrow::MakeArray(
                ArrayData::new(arr.r#type(), arr.len(),
                    Vec<std::shared_ptr<Buffer>>{arr.null_bitmap(), newbuf},
                    arr.null_count()))};
        }
        std::cout << std::boolalpha << (res1 == res4) << std::endl;
    }
}
