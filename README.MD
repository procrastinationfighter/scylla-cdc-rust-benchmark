# Scylla-cdc-rust benchmark
To run the benchmark, do the following:
1. Download the repository
2. Prepare the binaries by running the script `./prepare-bins.sh`
3. Run the script: `python3.10 run_benchmark.py --source <S> --partition_count <P> --clustering_row_count <C> --max_rate <M>`
where `<S>` is the address of the cluster, `<P>` is the number of partitions in the benchmark,
`<C>` is the number of clustering keys on every partition in the benchmark, 
`<M>` is the expected number of requests performed by [scylla-bench](https://github.com/scylladb/scylla-bench).

Note: to run the python script, [scylla-driver](https://github.com/scylladb/python-driver/) module is required.

The results will be written to files `rust_<num>.txt` and `java_<num>.txt` where `<num>` is the size of reading window in a given benchmark.
For example, `rust_15.txt` will contain results for scylla-cdc-rust with the time window equal to 15 seconds.