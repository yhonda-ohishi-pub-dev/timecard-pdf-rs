# タイムカードPDF生成 - Rust実装

## プロジェクト概要
PHPのTCPDFで生成していたタイムカードPDFをRustで置き換える。
MySQLデータベースから直接データを取得してPDFを生成する。

## 禁止事項
本番DBへのデータ挿入。Docker DBに挿入して`.claude/tools/db_verify.py`で比較・修正すること。

## 引継ぎ情報（2025-12-31）

### 検証結果（time_card_allowance）
```
一致: 95件 / 96件
不一致: 1件（テストデータ driver_id=9998）
```

### 完了事項
- 家畜・トレーラー手当の「先月最後の運行から継続」ロジック実装完了
- PHPのTimeCardTable.php:114〜と同等のロジックをdb.rs:482〜、602〜に実装
- ryohi_rowsとの結合は運行NOで行う（CONCAT(dr.運行NO, dr.対象乗務員区分)）

### 残りの差異
- driver_id=9998（テストデータ）のみ

## 検証コマンド
```bash
cargo run -- pdf 2025 12                                          # PDF生成 + allowance INSERT
cargo run -- pdf-shukei 2025 12 1071                              # 集計レイアウトPDF生成（driver_id指定）
python3 .claude/tools/db_verify.py --compare-allowance --year 2025 --month 12  # 比較
cargo run -- db 2025 12 1123                                      # 特定ドライバー確認
```

## ID体系（重要）
- **driver_id**: driversテーブルの主キー（例: 1071=中谷邦博, 1645=入口六治）
- **kyuyo_shain_id**: 給与社員ID、PDF表示用（例: 710=中谷邦博, 1673=入口六治）
- コマンドラインでは**driver_id**を使用する

## データベース接続

| 環境 | ホスト | 用途 |
|------|--------|------|
| 本番DB | 172.18.21.35 | 読み取り専用 |
| Docker DB | 127.0.0.1:3306 | 開発・検証用 |

認証: root / ohishi / db1

## ソースファイル

| ファイル | 説明 |
|----------|------|
| src/main.rs | CLI（db/pdf/verify/verify-dtako） |
| src/db.rs | DB接続、拘束時間計算、手当計算 |
| src/timecard_data.rs | データ構造、集計計算 |
| src/tcpdf_compat.rs | PDF生成 |

## 関連PHPファイル
- TimeCardController.php - createPdf():1576〜, _makeTimeCardDisplayArray():2708〜
- TimeCardTable.php - _count_teate():114〜（手当計算）
