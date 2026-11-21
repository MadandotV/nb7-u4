# Shopping Sessions

An entry for the Shopping Sessions track: dirty click-stream CSVs in `/data`
become the two daily Parquet tables in `/output`, as `rules.md` defines them.

It is one Rust binary with no engine underneath.

- `src/main.rs`: the pipeline that is timed. It scans the CSVs with AVX2,
  dedups events by ID, joins linked IDs with a union-find, groups events and
  clicks by person, sessionizes, credits and assists, and writes Parquet.
- `src/slow.rs`: a plain single-threaded implementation that follows
  `rules.md` line by line. The fast pipeline is checked against it, and falls
  back to it for input it does not represent exactly (for example a window
  months long).
- `src/pqw.rs`: a small Parquet writer (uncompressed, PLAIN pages).
- `src/keys.rs`: IDs as exact 64-bit keys.

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
