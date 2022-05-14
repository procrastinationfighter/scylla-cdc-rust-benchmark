use anyhow;
use async_trait::async_trait;
use atomic_counter::{AtomicCounter, ConsistentCounter};
use std::sync::Arc;

use scylla_cdc::consumer::{CDCRow, Consumer, ConsumerFactory};

// I was unsure whether the ConsumerFactory would be destroyed as last every time.
// If we use custom struct, we can implement Drop for it
// and make sure that it is executed when the last reference dies.
pub struct MyCounter {
    pub counter: ConsistentCounter
}

impl Drop for MyCounter {
    fn drop(&mut self) {
        println!("Scylla-cdc-rust read {} rows.", self.counter.get());
    }
}

struct BenchmarkConsumer {
    read_rows: usize,
    counter: Arc<MyCounter>
}

#[async_trait]
impl Consumer for BenchmarkConsumer {
    async fn consume_cdc(&mut self, _: CDCRow<'_>) -> anyhow::Result<()> {
        self.read_rows = self.read_rows + 1;
        Ok(())
    }
}

impl Drop for BenchmarkConsumer {
    fn drop(&mut self) {
        self.counter.counter.add(self.read_rows);
    }
}

pub struct BenchmarkConsumerFactory {
    pub(crate) counter: Arc<MyCounter>
}

#[async_trait]
impl ConsumerFactory for BenchmarkConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(BenchmarkConsumer {read_rows: 0, counter: Arc::clone(&self.counter)})
    }
}
