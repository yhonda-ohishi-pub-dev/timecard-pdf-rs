#!/usr/bin/env python3
"""
PHP → Rust カバレッジレポート生成
- COVERAGE_MAP.json を読み込み（読み取り専用）
- COVERAGE.md を生成
"""

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict

# パス設定
PROJECT_ROOT = Path("/home/yhonda/timecard-pdf-rs")
COVERAGE_MAP = PROJECT_ROOT / "COVERAGE_MAP.json"
OUTPUT_FILE = PROJECT_ROOT / "COVERAGE.md"

STATUS_ICONS = {
    "ok": "✅",
    "diff": "⚠️",
    "todo": "❌",
    "skip": "-"
}


def load_coverage_map() -> Dict:
    """COVERAGE_MAP.json を読み込み"""
    if not COVERAGE_MAP.exists():
        print(f"[ERROR] {COVERAGE_MAP} が見つかりません")
        return {}

    with open(COVERAGE_MAP, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_db_verify(year: int, month: int) -> Dict:
    """db_verify.py を実行して検証結果を取得"""
    results = {}
    scripts_dir = Path(__file__).parent

    for cmd_type, label in [('--compare', 'tc_dc'), ('--compare-dtako', 'digitacho'), ('--compare-allowance', 'allowance')]:
        try:
            result = subprocess.run(
                ['python3', str(scripts_dir / 'db_verify.py'), cmd_type, '--year', str(year), '--month', str(month)],
                capture_output=True, text=True, timeout=60
            )
            results[label] = parse_verify_output(result.stdout)
        except Exception as e:
            print(f"  [WARN] {label}検証エラー: {e}")
            results[label] = {}

    return results


def parse_verify_output(output: str) -> Dict:
    """db_verify.py の出力をパース"""
    import re
    result = {'match': 0, 'mismatch': 0, 'prod_only': 0, 'docker_only': 0}
    for line in output.split('\n'):
        if '一致:' in line:
            m = re.search(r'一致:\s*(\d+)', line)
            if m: result['match'] = int(m.group(1))
        elif '不一致:' in line:
            m = re.search(r'不一致:\s*(\d+)', line)
            if m: result['mismatch'] = int(m.group(1))
        elif '本番のみ:' in line:
            m = re.search(r'本番のみ:\s*(\d+)', line)
            if m: result['prod_only'] = int(m.group(1))
        elif 'Dockerのみ:' in line:
            m = re.search(r'Dockerのみ:\s*(\d+)', line)
            if m: result['docker_only'] = int(m.group(1))
    return result


def generate_report(data: Dict, verify_results: Dict, year: int, month: int) -> str:
    """マークダウンレポートを生成"""
    lines = []

    # ヘッダー
    lines.append("# PHP → Rust カバレッジレポート")
    lines.append("")
    lines.append(f"生成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"対象データ: {year}年{month}月")
    lines.append("")

    # 検証結果サマリー
    lines.append("## 検証結果サマリー")
    lines.append("")
    lines.append("| 種別 | 一致 | 不一致 | 本番のみ | Dockerのみ |")
    lines.append("|------|------|--------|----------|------------|")
    for key, label in [('tc_dc', 'TC_DC版'), ('digitacho', 'デジタコ版'), ('allowance', 'allowance')]:
        v = verify_results.get(key, {})
        lines.append(f"| {label} | {v.get('match', '-')} | {v.get('mismatch', '-')} | {v.get('prod_only', '-')} | {v.get('docker_only', '-')} |")
    lines.append("")

    # PHP → Rust マッピング
    lines.append("---")
    lines.append("")
    lines.append("## PHP → Rust 関数マッピング")
    lines.append("")

    php_to_rust = data.get("php_to_rust", {})
    for php_file, funcs in php_to_rust.items():
        lines.append(f"### {php_file}")
        lines.append("")
        lines.append("| PHP関数 | Rust関数 | ファイル | 状態 | 備考 |")
        lines.append("|---------|----------|----------|------|------|")
        for func_name, info in funcs.items():
            rust_func = info.get("rust") or "-"
            rust_file = info.get("file", "-")
            status = STATUS_ICONS.get(info.get("status", ""), "")
            note = info.get("note", "")
            lines.append(f"| `{func_name}()` | `{rust_func}` | {rust_file} | {status} | {note} |")
        lines.append("")

    # Rust → PHP マッピング
    lines.append("---")
    lines.append("")
    lines.append("## Rust → PHP 関数マッピング")
    lines.append("")

    rust_to_php = data.get("rust_to_php", {})
    for rust_file, funcs in rust_to_php.items():
        lines.append(f"### {rust_file}")
        lines.append("")
        lines.append("| Rust関数 | PHP関数 | 状態 |")
        lines.append("|----------|---------|------|")
        for func_name, info in funcs.items():
            php_func = info.get("php", "-")
            status = STATUS_ICONS.get(info.get("status", ""), "")
            lines.append(f"| `{func_name}()` | `{php_func}` | {status} |")
        lines.append("")

    # テーブルマッピング
    lines.append("---")
    lines.append("")
    lines.append("## DBテーブル参照")
    lines.append("")
    lines.append("| テーブル | PHP | Rust |")
    lines.append("|----------|-----|------|")
    tables = data.get("tables", {})
    for table, refs in sorted(tables.items()):
        php_mark = "✓" if refs.get("php") else ""
        rust_mark = "✓" if refs.get("rust") else ""
        lines.append(f"| {table} | {php_mark} | {rust_mark} |")
    lines.append("")

    # 凡例
    lines.append("---")
    lines.append("")
    lines.append("## 凡例")
    lines.append("")
    lines.append("- ✅ 完全一致")
    lines.append("- ⚠️ 差異あり")
    lines.append("- ❌ 未実装")
    lines.append("- `-` 対象外")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='カバレッジレポート生成（COVERAGE_MAP.json読み取り専用）')
    parser.add_argument('--year', type=int, default=2025, help='検証対象年')
    parser.add_argument('--month', type=int, default=12, help='検証対象月')
    parser.add_argument('--no-verify', action='store_true', help='DB検証をスキップ')
    args = parser.parse_args()

    print("[1/3] COVERAGE_MAP.json 読み込み...")
    data = load_coverage_map()
    if not data:
        return 1

    verify_results = {}
    if not args.no_verify:
        print("[2/3] DB検証実行...")
        verify_results = run_db_verify(args.year, args.month)
    else:
        print("[2/3] DB検証スキップ")

    print("[3/3] レポート生成...")
    report = generate_report(data, verify_results, args.year, args.month)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"\n[OK] {OUTPUT_FILE} を生成しました")
    return 0


if __name__ == '__main__':
    exit(main())
