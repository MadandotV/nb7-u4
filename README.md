# Shopping Sessions

An entry for the Shopping Sessions track: dirty click-stream CSVs in `/data`
become the two daily Parquet tables in `/output`, as `rules.md` defines them.

It is one Rust binary with no engine underneath.

- `src/main.rs`: the pipeline that is timed. Links are joined first, in a
  lock-free hash table and union-find. Events and clicks are then parsed once:
  every event line leaves a 16-byte dedup record partitioned by event ID, and
  every valid event and click a record partitioned by person. Each event-ID
  partition is deduped and the losing copies tombstoned; each person partition
  is grouped, sorted, sessionized, credited and written out.
- `src/scan.rs`: memory-mapped input and the AVX2 pass that indexes the
  separators and quotes of a block of lines.
- `src/mem.rs`: the partitioned record streams, filled a cache line at a time.
- `src/keys.rs`: IDs as exact 64-bit keys.
- `src/pqw.rs`: a small Parquet writer (uncompressed, PLAIN pages).
- `src/slow.rs`: a plain single-threaded implementation that follows
  `rules.md` line by line. The fast pipeline is checked against it, and falls
  back to it for input it does not represent exactly (for example a window
  months long).

## Check it

- `check.sh DATASET` runs both implementations on a dataset and diffs the tables.
- `tests/fuzz.py ROUNDS` does the same on small random datasets full of
  quoting, malformed lines, bad timestamps and odd IDs.
- `bench.sh BINARY DATASET` times repeated runs.

## Run it

```sh
docker build -t shopping-sessions .
docker run --rm -v "$PWD/data/dev":/data:ro -v "$PWD/out":/output shopping-sessions
```

Without Docker: `cargo build --release`, then
`target/release/shopsess DATA_DIR OUTPUT_DIR`. `target/release/reference`
takes the same arguments and runs the slow implementation.

## Third-party code

The only dependency is the [`libc`](https://crates.io/crates/libc) crate
(MIT or Apache-2.0), for `mmap`. Everything else was written during the
buildathon, with Claude Code.

## License

MIT, see `LICENSE`.
