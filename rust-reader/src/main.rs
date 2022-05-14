pub mod benchmark;

use std::sync::Arc;
use std::time::Duration;
use atomic_counter::ConsistentCounter;
use chrono::NaiveDateTime;

use clap::Parser;

use scylla::SessionBuilder;
use scylla_cdc::log_reader::CDCLogReaderBuilder;

use crate::benchmark::*;

#[derive(Parser)]
struct Args {
    /// Keyspace name
    #[clap(short, long)]
    keyspace: String,

    /// Table name
    #[clap(short, long)]
    table: String,

    /// Address of a node in source cluster
    #[clap(short, long)]
    hostname: String,

    /// Window size in seconds
    #[clap(long, default_value_t = 60.)]
    window_size: f64,

    /// Safety interval in seconds
    #[clap(long, default_value_t = 30.)]
    safety_interval: f64,

    /// Sleep interval in seconds
    #[clap(long, default_value_t = 10.)]
    sleep_interval: f64,

    /// Starting timestamp, format: %Y-%m-%d %H:%M:%S
    #[clap(short, long)]
    start_timestamp: String,

    /// Ending timestamp, format: %Y-%m-%d %H:%M:%S
    #[clap(short, long)]
    end_timestamp: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(args.hostname)
            .build()
            .await?,
    );
    let counter = Arc::new(MyCounter {counter: ConsistentCounter::new(0)});
    let factory = Arc::new(BenchmarkConsumerFactory {counter});

    let start_date_time = NaiveDateTime::parse_from_str(&args.start_timestamp, "%Y-%m-%d %H:%M:%S").unwrap();
    let end_date_time = NaiveDateTime::parse_from_str(&args.end_timestamp, "%Y-%m-%d %H:%M:%S").unwrap();

    let start = chrono::Duration::milliseconds(start_date_time.timestamp_millis());
    let end = chrono::Duration::milliseconds(end_date_time.timestamp_millis());
    let (_, handle) = CDCLogReaderBuilder::new()
        .session(session)
        .keyspace(&args.keyspace)
        .table_name(&args.table)
        .window_size(Duration::from_secs_f64(args.window_size))
        .safety_interval(Duration::from_secs_f64(args.safety_interval))
        .sleep_interval(Duration::from_secs_f64(args.sleep_interval))
        .consumer_factory(factory)
        .start_timestamp(start)
        .end_timestamp(end)
        .build()
        .await?;

    handle.await
}
