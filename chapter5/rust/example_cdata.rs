use arrow;

fn generate_data(size: size_t) -> Vec<i32>{
    static std::uniform_int_distribution<i32> dist(
        std::numeric_limits<i32>::min(), std::numeric_limits<i32>::max());
    static std::random_device rnd_device;
    std::default_random_engine generator(rnd_device());
    let data = Vec::with_capacity(size);
    std::generate(data.begin(), data.end(), [&]() { return dist(generator); });
    return data;
}

extern "C" {
    fn export_int32_data(ArrowArray);
}

fn export_int32_data(array: ArrowArray) {
    let vecptr = generate_data(1000);

    *array = (struct ArrowArray){
        .length = vecptr.size(),
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = (const void**)malloc(sizeof(void*) * 2),
        .children = nullptr,
        .dictionary = nullptr,
        .release =
            [](struct ArrowArray* arr) {
                free(arr.buffers);
                delete reinterpret_cast<Vec<i32>*>(arr.private_data);
                arr.release = nullptr;
            },
        .private_data = reinterpret_cast<void*>(vecptr),
    };
    array.buffers[0] = nullptr;
    array.buffers[1] = vecptr.data();
} 
