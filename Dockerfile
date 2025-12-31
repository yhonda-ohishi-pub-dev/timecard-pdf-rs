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

# Stage 2: Runtime (distroless - glibcとSSL含む)
FROM gcr.io/distroless/cc-debian12

# ビルド済みバイナリをコピー
COPY --from=builder /app/target/release/timecard-pdf-rs /timecard-pdf-rs

# 環境変数のデフォルト値
ENV PROD_DB_HOST=172.18.21.35
ENV PROD_DB_PORT=3306
ENV PROD_DB_USER=root
ENV PROD_DB_PASSWORD=ohishi
ENV PROD_DB_NAME=db1

# HTTPサーバーポート
EXPOSE 8080

# HTTPサーバーモードで起動
ENTRYPOINT ["/timecard-pdf-rs", "server"]
