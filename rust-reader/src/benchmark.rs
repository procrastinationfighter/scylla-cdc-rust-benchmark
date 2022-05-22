use anyhow;
use async_trait::async_trait;
use atomic_counter::{AtomicCounter, ConsistentCounter};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use scylla_cdc::consumer::{CDCRow, Consumer, ConsumerFactory};

struct BenchmarkConsumer {
    counter: Arc<ConsistentCounter>,
    limit: usize,
    sender: Sender<()>,
}

#[async_trait]
impl Consumer for BenchmarkConsumer {
    async fn consume_cdc(&mut self, _: CDCRow<'_>) -> anyhow::Result<()> {
        let old = self.counter.inc();
        if old + 1 >= self.limit {
            self.sender.send(()).await.unwrap();
        }
        Ok(())
    }
}

pub struct BenchmarkConsumerFactory {
    pub counter: Arc<ConsistentCounter>,
    pub limit: usize,
    pub sender: Sender<()>,
}

#[async_trait]
impl ConsumerFactory for BenchmarkConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(BenchmarkConsumer {
            counter: Arc::clone(&self.counter),
            limit: self.limit,
            sender: self.sender.clone(),
        })
    }
}
