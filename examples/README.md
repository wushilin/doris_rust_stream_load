# Example: simple

This example demonstrates a full `Config` builder flow and shows how to:

- configure endpoint/database/table or stream load URL
- add columns and custom headers
- set CSV mode and validation
- enable fake send for local testing
- submit a batch of records
- submit a single record with callback
- close the client cleanly
- print runtime statistics

## Run the example

From the repository root:

```bash
cargo run --example simple
```

Run the more complete example:

```bash
cargo run --example sample_use
```

## Example file

See `examples/simple.rs` and `examples/sample_use.rs` for runnable samples.
