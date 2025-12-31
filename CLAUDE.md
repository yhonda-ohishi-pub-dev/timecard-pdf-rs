# タイムカードPDF生成 - Rust実装

## プロジェクト概要
PHPのTCPDFで生成していたタイムカードPDFをRustで置き換える。
MySQLデータベースから直接データを取得してPDFを生成する。

## 禁止事項
本番DBへのデータ挿入。Docker DBに挿入して`.claude/tools/db_verify.py`で比較・修正すること。

## 引継ぎ情報（2025-12-31 最終更新）

### HTTPサーバー実装完了
- axum HTTPサーバー追加（src/server.rs）
- エンドポイント: `/health`, `/api/pdf`, `/api/pdf-shukei`
- 本番DB（読み取り）とDocker DB（書き込み）を分離
- PDF生成時にallowance/kosokuを自動INSERT

### リリース自動化
- VERSIONファイル変更でCargo.toml自動同期（pre-commitフック）
- git push時にタグ作成＆Dockerイメージビルド・GHCR push（pre-pushフック）
- 現在バージョン: 0.3.0

### 検証結果（time_card_allowance）
```
一致: 94件 / 96件
不一致: 2件
  - driver_id=9998（テストデータ、無視）
  - driver_id=1026: trail_payment PHP=14, Rust=15（1日差）
```

### 未解決の差異（driver_id=1026）
- トレーラー手当が1日多い
- 原因調査: PHPのDatePeriodは帰庫日を含む（出庫日0時〜帰庫日時まで）
- Rustも同じロジック（`while current <= end_date`）だが結果が1日多い
- 12月のけん引運行: 14件、PHPは14日、Rustは15日
- 要追加調査

### 完了事項
- 家畜・トレーラー手当の「先月最後の運行から継続」ロジック実装完了
- PHPのTimeCardTable.php:114〜と同等のロジックをdb.rs:482〜、602〜に実装
- ryohi_rowsとの結合は運行NOで行う（CONCAT(dr.運行NO, dr.対象乗務員区分)）
- HTTPサーバー（axum）実装
- フォント埋め込み（include_bytes!）でDockerコンテナ対応
- pre-commit/pre-pushフックによるリリース自動化

## 検証コマンド
```bash
# CLI
cargo run -- pdf 2025 12                                          # PDF生成 + allowance INSERT
cargo run -- pdf-shukei 2025 12 1071                              # 集計レイアウトPDF生成
cargo run -- db 2025 12 1026                                      # 特定ドライバー確認

# HTTPサーバー
cargo run -- server 8080
curl -X POST http://localhost:8080/api/pdf -H "Content-Type: application/json" -d '{"year":2025,"month":12}' -o timecard.pdf

# 検証
python3 .claude/tools/db_verify.py --compare-allowance --year 2025 --month 12

# リリース
echo "0.4.0" > VERSION && git add VERSION && git commit -m "Release 0.4.0" && git push origin main
```

## ID体系（重要）
- **driver_id**: driversテーブルの主キー（例: 1071=中谷邦博, 1645=入口六治）
- **kyuyo_shain_id**: 給与社員ID、PDF表示用（例: 710=中谷邦博, 1673=入口六治）
- コマンドラインでは**driver_id**を使用する

## データベース接続

| 環境 | ホスト | 用途 |
|------|--------|------|
| 本番DB | 172.18.21.35 | 読み取り専用 |
| Docker DB | 127.0.0.1:3306 | 書き込み用 |

認証: root / ohishi / db1

## ソースファイル

| ファイル | 説明 |
|----------|------|
| src/main.rs | CLI（db/pdf/server） |
| src/server.rs | HTTPサーバー（axum） |
| src/db.rs | DB接続、拘束時間計算、手当計算 |
| src/timecard_data.rs | データ構造、集計計算 |
| src/tcpdf_compat.rs | PDF生成 |

## 関連PHPファイル
- TimeCardController.php - createPdf():1576〜, _makeTimeCardDisplayArray():2708〜
- TimeCardTable.php - _count_teate():114〜（手当計算）

## Docker/リリース関連
- Dockerfile: マルチステージビルド（distroless）
- docker-compose.yml: 本番デプロイ用
- .git/hooks/pre-commit: VERSION→Cargo.toml同期
- .git/hooks/pre-push: タグ作成＆Dockerイメージビルド・push
- GHCR: ghcr.io/yhonda-ohishi-pub-dev/timecard-pdf-rs
