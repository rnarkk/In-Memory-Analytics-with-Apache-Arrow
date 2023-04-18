use alloc::vec::Vec;
use arrow::array::Array;
#include <arrow/api.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/util/optional.h>
#include <algorithm>
#include <iostream>
#include <numeric>
#include "timer.h"

namespace cp = arrow::compute;

fn main(int argc, char** argv) {
    for (int n = 10000; n <= 10000000; n += 10000) {
        let Vec<i32> testvalues(n);
        std::iota(std::begin(testvalues), std::end(testvalues), 0);

        arrow::Int32Builder nb;
        nb.AppendValues(testvalues);
        std::shared_ptr<arrow::Array> numarr;
        nb.Finish(&numarr);

    std::cout << "N: " << n << std::endl;

    auto arr = std::static_pointer_cast<arrow::Int32Array>(numarr);

    arrow::Datum res1;
    {
      timer t;
      res1 = cp::Add(arr, arrow::Datum{(int32_t)2}).MoveValueUnsafe();
    }

    arrow::Datum res2;
    {
      timer t;
      arrow::Int32Builder b;
      for (size_t i = 0; i < arr->length(); ++i) {
        if (arr->IsValid(i)) {
          b.Append(arr->Value(i) + 2);
        } else {
          b.AppendNull();
        }
      }
      std::shared_ptr<arrow::Array> output;
      b.Finish(&output);
      res2 = arrow::Datum{std::move(output)};
    }
    std::cout << std::boolalpha << (res1 == res2) << std::endl;

    arrow::Datum res3;
    {
      timer t;
      auto arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
      arrow::Int32Builder b;
      b.Reserve(arr->length());
      std::for_each(std::begin(*arr), std::end(*arr),
                    [&b](const arrow::util::optional<int32_t>& v) {
                      if (v) {
                        b.Append(*v + 2);
                      } else {
                        b.AppendNull();
                      }
                    });
      std::shared_ptr<arrow::Array> output;
      b.Finish(&output);
      res3 = arrow::Datum{std::move(output)};
    }
    std::cout << std::boolalpha << (res1 == res3) << std::endl;

    arrow::Datum res4;
    {
      timer t;
      auto arr = std::static_pointer_cast<arrow::Int32Array>(numarr);
      std::shared_ptr<arrow::Buffer> newbuf =
          arrow::AllocateBuffer(sizeof(int32_t) * arr->length())
              .MoveValueUnsafe();
      auto output = reinterpret_cast<int32_t*>(newbuf->mutable_data());
      std::transform(arr->raw_values(), arr->raw_values() + arr->length(),
                     output, [](const int32_t v) { return v + 2; });

      res4 = arrow::Datum{arrow::MakeArray(
          arrow::ArrayData::Make(arr->type(), arr->length(),
                                 std::vector<std::shared_ptr<arrow::Buffer>>{
                                     arr->null_bitmap(), newbuf},
                                 arr->null_count()))};
    }
    std::cout << std::boolalpha << (res1 == res4) << std::endl;
  }
}
