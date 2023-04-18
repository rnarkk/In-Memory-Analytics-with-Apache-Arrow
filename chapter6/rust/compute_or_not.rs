use arrow::{
    array::Array,
    buffer::Buffer,
    compute
};
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/util/optional.h>
#include "timer.h"

namespace cp = arrow::compute;

fn main() {
    for (int n = 10000; n <= 10000000; n += 10000) {
        let testvalues = Vec<i32>(n);
        std::iota(std::begin(testvalues), std::end(testvalues), 0);

        let nb = arrow::Int32Builder;
        nb.AppendValues(testvalues);
        let numarr = std::shared_ptr<arrow::Array>;
        nb.Finish(&numarr);

        println!("N: {}", n);

        let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);

        let res1 = arrow::Datum;
        {
            let t = timer;
            res1 = cp::Add(arr, arrow::Datum{2i32}).MoveValueUnsafe();
        }

        let res2 = arrow::Datum;
        {
            let t = timer;
            let b = arrow::Int32Builder;
            for i in 0..arr.len() {
                if arr.IsValid(i) {
                    b.Append(arr.Value(i) + 2);
                } else {
                    b.AppendNull();
                }
            }
            let output = std::shared_ptr<arrow::Array>;
            b.Finish(&output);
            res2 = arrow::Datum{std::move(output)};
        }
        std::cout << std::boolalpha << (res1 == res2) << std::endl;

        let res3 = arrow::Datum;
        {
            let t = timer;
            let arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
            let b = arrow::Int32Builder;
            b.Reserve(arr.len());
            std::for_each(std::begin(*arr), std::end(*arr),
                [&b]|v: const arrow::util::optional<i32>&| {
                    if v {
                        b.Append(*v + 2);
                    } else {
                        b.AppendNull();
                    }
                });
            let output = std::shared_ptr<arrow::Array>;
            b.Finish(&output);
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
                arrow::ArrayData::Make(arr.type(), arr.len(),
                    Vec<std::shared_ptr<Buffer>>{
                        arr.null_bitmap(), newbuf},
                    arr.null_count()))};
        }
        std::cout << std::boolalpha << (res1 == res4) << std::endl;
    }
}
