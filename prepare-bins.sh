#!/usr/bin/env bash
mkdir results

cd rust-reader
cargo build --release

cd ../java-reader/cdc-rust-benchmark
mvn package
chmod 744 cdc-rust-benchmark

cd ../../rust-optimized-reader/reader
cargo build --release

cd ../../scylla-bench
go install . && go build .
