FROM rust:1.73.0 as builder
WORKDIR /usr/src/

RUN rustup component add rustfmt
RUN apt-get update && apt-get install -y protobuf-compiler

# temporary disable dependancy cache
# RUN echo "fn main() {}" > dummy.rs

COPY Cargo.* ./
COPY ./migrations ./migrations

# RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
# RUN cargo build -j4 --release
# RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml

COPY ./src ./src

RUN cargo install --path .

FROM debian:12.1-slim
WORKDIR /app
RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev
RUN /usr/sbin/update-ca-certificates

COPY --from=builder /usr/local/cargo/bin/service .
COPY --from=builder /usr/local/cargo/bin/consumer .
COPY --from=builder /usr/local/cargo/bin/migration .
COPY --from=builder /usr/src/migrations ./migrations/

CMD ["./consumer"]
