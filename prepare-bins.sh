#!/usr/bin/env bash
cd rust-reader
cargo build --release

cd ../java-reader/cdc-rust-benchmark
mvn package

cd ../../scylla-bench
go install . && go build .
