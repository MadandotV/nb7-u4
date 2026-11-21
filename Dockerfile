# Shopping Sessions: one static Rust binary, started with no arguments.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
# Static glibc, so the run image needs nothing but the binary.
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --bin shopsess

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/shopsess /shopsess
ENV SHOPSESS_QUIET=1
CMD ["/shopsess"]
