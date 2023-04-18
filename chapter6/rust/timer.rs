use std::time::{Duration, SystemTime};

struct Timer {
    start_: std::chrono::time_point<std::chrono::system_clock>
}

impl Timer {
    fn new() -> Self {
        Self {
            start: std::chrono::system_clock::now()
        }
    }
}

impl Drop for Timer {
    fn drop() {
        println!("{}s", std::chrono::duration<double>(
            std::chrono::system_clock::now() - self.start)
            .count());
    }
}
