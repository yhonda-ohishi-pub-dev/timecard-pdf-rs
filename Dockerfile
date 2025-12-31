# Stage 1: Build
FROM rust:1.83-bookworm AS builder

WORKDIR /app

# 依存関係のキャッシュ（ソースコード変更時にビルドを高速化）
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release || true
RUN rm -rf src

# ソースコードとフォントをコピーしてビルド
COPY src ./src
COPY fonts ./fonts
RUN touch src/main.rs && cargo build --release

# Stage 2: Runtime (debian-slim - zlib含む)
FROM debian:bookworm-slim

# 必要なランタイムライブラリをインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# ビルド済みバイナリをコピー
COPY --from=builder /app/target/release/timecard-pdf-rs /timecard-pdf-rs

# HTTPサーバーポート
EXPOSE 8080

# HTTPサーバーモードで起動
ENTRYPOINT ["/timecard-pdf-rs", "server"]
