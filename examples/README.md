# Examples

Start with the local FakeSend example:

```sh
cargo run --example simple
```

It does not require a Doris cluster.

Other runnable examples:

```sh
cargo run --example async_full_config
cargo run --example shared_producers
cargo run --example shared_producers_async
cargo run --example sync_throughput_benchmark
cargo run --example async_throughput_benchmark
cargo run --example async_parallel_benchmark -- --threads 100000
```

The benchmark examples use FakeSend too, but they intentionally enqueue large
record counts for throughput testing.
