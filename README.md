# timecard-pdf-rs

タイムカードPDF生成サービス（Rust実装）

## 概要

PHPのTCPDFで生成していたタイムカードPDFをRustで置き換え。MySQLデータベースから直接データを取得してPDFを生成する。

## クイックスタート

### Docker Compose

```bash
docker compose up -d
```

### 直接実行

```bash
# ビルド
cargo build --release

# CLIモード
cargo run -- pdf 2025 12              # PDF生成（3人/ページ）
cargo run -- pdf-shukei 2025 12 1071  # 集計レイアウト（driver_id指定）

# HTTPサーバーモード
cargo run -- server 8080
```

## API エンドポイント

| エンドポイント | メソッド | 説明 |
|---------------|---------|------|
| `/health` | GET | ヘルスチェック |
| `/api/pdf` | POST | PDF生成（3人/ページ） |
| `/api/pdf-shukei` | POST | 集計レイアウトPDF生成 |

### リクエスト例

```bash
# 全ドライバーのタイムカード
curl -X POST http://localhost:8080/api/pdf \
  -H "Content-Type: application/json" \
  -d '{"year":2025,"month":12}' \
  -o timecard.pdf

# 特定ドライバーのタイムカード
curl -X POST http://localhost:8080/api/pdf \
  -H "Content-Type: application/json" \
  -d '{"year":2025,"month":12,"driver_id":1071}' \
  -o timecard_1071.pdf

# 集計レイアウト
curl -X POST http://localhost:8080/api/pdf-shukei \
  -H "Content-Type: application/json" \
  -d '{"year":2025,"month":12,"driver_id":1071}' \
  -o shukei_1071.pdf
```

## 環境変数

| 変数名 | デフォルト | 説明 |
|--------|-----------|------|
| `PROD_DB_HOST` | 172.18.21.35 | データベースホスト |
| `PROD_DB_PORT` | 3306 | データベースポート |
| `PROD_DB_USER` | root | データベースユーザー |
| `PROD_DB_PASSWORD` | ohishi | データベースパスワード |
| `PROD_DB_NAME` | db1 | データベース名 |

## リリース手順

VERSIONファイルを更新してpushするだけで自動リリース：

```bash
echo "1.0.0" > VERSION
git add VERSION
git commit -m "Release 1.0.0"
git push origin main
```

pre-pushフックが自動で：
1. Cargo.tomlのバージョンを同期
2. Gitタグ（v1.0.0）を作成・プッシュ
3. Dockerイメージをビルド・GHCRへプッシュ

## ライセンス

Private
