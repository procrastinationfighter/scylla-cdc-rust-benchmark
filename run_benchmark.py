import argparse
import subprocess

# TODO: Configure parameters: safety interval etc.
rust_binary = "rust-reader/target/release/scylla-cdc-rust-benchmark"
java_binary = "java-reader/cdc-rust-benchmark"
scylla_bench_binary = "scylla-bench/scylla-bench"
keyspace = "scylla_bench"
table = "test"

time_command = "/usr/bin/time -v"


def write_to_database(source: str, partition_count: int, clustering_row_count: int):
    command = [scylla_bench_binary,
               "-nodes", source,
               "-mode", "write",
               "-workload", "sequential",
               "-partition-count", f"{partition_count}",
               "-clustering-row-count", f"{clustering_row_count}"]

    subprocess.run(command, stdout=subprocess.DEVNULL)


def run_rust(source: str, rows_count: int):
    # TODO: Add correct timestamp
    command = ["/usr/bin/time", "-v",
               rust_binary,
               "--keyspace", keyspace,
               "--table", table,
               "--hostname", f"{source}:9042",
               "--start-timestamp", "1970-01-01 10:00:00",
               "--rows_count", f"{rows_count}"]

    print("RUNNING THE BENCHMARK FOR SCYLLA-CDC-RUST:")
    subprocess.run(command, stdout=subprocess.STDOUT)
    print("\n\n\n")


def run_java(source: str, rows_count: int):
    command = ["/usr/bin/time", "-v",
               java_binary,
               "-k", keyspace,
               "-t", table,
               "-s", source,
               "-c", f"{rows_count}"]

    print("RUNNING THE BENCHMARK FOR SCYLLA-CDC-JAVA:")
    subprocess.run(command, stdout=subprocess.STDOUT)
    print("\n\n\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str)
    parser.add_argument("--partition_count", default=100, type=int)
    parser.add_argument("--clustering_row_count", default=100, type=int)

    args = parser.parse_args()
    source = args.source

    # TODO: Create the table

    write_to_database(source, args.partition_count, args.clustering_row_count)

    rows_count = args.partition_count * args.clustering_row_count
    run_rust(source, rows_count)
    run_java(source, rows_count)


main()
