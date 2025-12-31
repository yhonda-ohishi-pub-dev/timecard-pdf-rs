#!/usr/bin/env python3
"""
DB検証スクリプト
- 本番DB（PHP計算結果）とDocker DB（Rust計算結果）を比較
"""

import argparse
import os
import sys
from typing import Dict, List
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

# DB接続設定（環境変数から取得）
PROD_DB_CONFIG = {
    'host': os.getenv('PROD_DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('PROD_DB_PORT', '3306')),
    'user': os.getenv('PROD_DB_USER', 'root'),
    'password': os.getenv('PROD_DB_PASSWORD', ''),
    'database': os.getenv('PROD_DB_NAME', 'db1'),
}

DOCKER_DB_CONFIG = {
    'host': os.getenv('DOCKER_DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DOCKER_DB_PORT', '3306')),
    'user': os.getenv('DOCKER_DB_USER', 'root'),
    'password': os.getenv('DOCKER_DB_PASSWORD', ''),
    'database': os.getenv('DOCKER_DB_NAME', 'db1'),
}


class DbVerifier:
    """DB検証クラス"""

    def __init__(self):
        self.prod_conn = None
        self.docker_conn = None

    def connect_prod(self) -> bool:
        """本番DBに接続"""
        try:
            self.prod_conn = mysql.connector.connect(**PROD_DB_CONFIG)
            print(f"[OK] 本番DB接続成功: {PROD_DB_CONFIG['host']}")
            return True
        except Error as e:
            print(f"[ERROR] 本番DB接続失敗: {e}")
            return False

    def connect_docker(self) -> bool:
        """Docker DBに接続"""
        try:
            self.docker_conn = mysql.connector.connect(**DOCKER_DB_CONFIG)
            print(f"[OK] Docker DB接続成功: {DOCKER_DB_CONFIG['host']}")
            return True
        except Error as e:
            print(f"[ERROR] Docker DB接続失敗: {e}")
            return False

    def close(self):
        """DB接続を閉じる"""
        if self.prod_conn:
            self.prod_conn.close()
        if self.docker_conn:
            self.docker_conn.close()

    def get_active_driver_ids(self, year: int, month: int) -> List[int]:
        """アクティブなドライバーIDを取得（本番DBから）"""
        if not self.prod_conn:
            if not self.connect_prod():
                return []

        first_of_month = f"{year}-{month:02d}-01"
        if month == 12:
            next_month_first = f"{year+1}-01-01"
        else:
            next_month_first = f"{year}-{month+1:02d}-01"

        query = f"""
            SELECT d.id
            FROM drivers d
            INNER JOIN kyuyo_shain ks ON ks.driver_id = d.id
            LEFT JOIN time_card_yakin tcy ON tcy.parent_kyuyo_shain_id = ks.id AND tcy.parent_firm_id = ks.firm_id
            LEFT JOIN time_card_exception tce ON tce.kyuyo_shain_id = ks.id AND tce.firm_id = ks.firm_id
              AND tce.start_month <= '{first_of_month}'
              AND (tce.end_month > '{first_of_month}' OR tce.end_month IS NULL)
            WHERE ks.eigyosho_c = 1
              AND ks.category_c != 1
              AND (ks.retire_date IS NULL OR ks.retire_date > '{first_of_month}')
              AND ks.hire_date < '{next_month_first}'
              AND tcy.kyuyo_shain_id IS NULL
              AND tce.kyuyo_shain_id IS NULL
            ORDER BY ks.firm_id ASC, ks.category_c ASC, ks.id ASC
        """

        cursor = self.prod_conn.cursor()
        cursor.execute(query)
        driver_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()

        print(f"[INFO] アクティブドライバー数: {len(driver_ids)}")
        return driver_ids

    def compare_kosoku(self, year: int, month: int, driver_ids: List[int]) -> Dict:
        """
        本番DB（PHP計算）とDocker DB（Rust計算）の拘束時間を比較
        """
        print("\n" + "="*60)
        print("拘束時間比較: 本番DB(PHP) vs Docker DB(Rust)")
        print("="*60)

        if not self.connect_prod() or not self.connect_docker():
            return {}

        first_of_month = f"{year}-{month:02d}-01"
        days_in_month = self._get_days_in_month(year, month)
        last_of_month = f"{year}-{month:02d}-{days_in_month:02d}"

        results = {'match': 0, 'mismatch': 0, 'prod_only': 0, 'docker_only': 0, 'details': []}

        for driver_id in driver_ids:
            # 本番DBから取得（TC_DCのみ比較）
            prod_cursor = self.prod_conn.cursor(dictionary=True)
            prod_cursor.execute(f"""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') as date, SUM(minutes) as minutes
                FROM time_card_kosoku
                WHERE driver_id = {driver_id}
                AND type = 'TC_DC'
                AND date >= '{first_of_month}' AND date <= '{last_of_month}'
                GROUP BY date
                ORDER BY date
            """)
            prod_data = {row['date']: row['minutes'] for row in prod_cursor.fetchall()}
            prod_cursor.close()

            # Docker DBから取得（Rust計算結果）
            docker_cursor = self.docker_conn.cursor(dictionary=True)
            docker_cursor.execute(f"""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') as date, SUM(minutes) as minutes
                FROM time_card_kosoku
                WHERE driver_id = {driver_id}
                AND type = 'Rust計算'
                AND date >= '{first_of_month}' AND date <= '{last_of_month}'
                GROUP BY date
                ORDER BY date
            """)
            docker_data = {row['date']: row['minutes'] for row in docker_cursor.fetchall()}
            docker_cursor.close()

            # 比較
            all_dates = set(prod_data.keys()) | set(docker_data.keys())
            for date in sorted(all_dates):
                prod_val = prod_data.get(date)
                docker_val = docker_data.get(date)

                if prod_val == docker_val:
                    results['match'] += 1
                elif prod_val is None:
                    results['docker_only'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'date': date,
                        'prod': None,
                        'docker': docker_val,
                        'status': 'Docker only'
                    })
                elif docker_val is None:
                    results['prod_only'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'date': date,
                        'prod': prod_val,
                        'docker': None,
                        'status': 'Prod only'
                    })
                else:
                    results['mismatch'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'date': date,
                        'prod': prod_val,
                        'docker': docker_val,
                        'diff': docker_val - prod_val,
                        'status': 'MISMATCH'
                    })

        # サマリー表示
        print(f"\n一致: {results['match']}件")
        print(f"不一致: {results['mismatch']}件")
        print(f"本番のみ: {results['prod_only']}件")
        print(f"Dockerのみ: {results['docker_only']}件")

        if results['details']:
            print("\n--- 差異詳細（先頭20件）---")
            for d in results['details'][:20]:
                if d['status'] == 'MISMATCH':
                    print(f"  Driver {d['driver_id']} / {d['date']}: PHP={d['prod']}分, Rust={d['docker']}分 (差:{d['diff']}分)")
                else:
                    print(f"  Driver {d['driver_id']} / {d['date']}: {d['status']} (PHP={d['prod']}, Rust={d['docker']})")

        return results

    def compare_kosoku_digitacho(self, year: int, month: int, driver_ids: List[int]) -> Dict:
        """
        本番DB（PHPデジタコ計算）とDocker DB（Rustデジタコ計算）の拘束時間を比較
        """
        print("\n" + "="*60)
        print("拘束時間比較（デジタコ版）: 本番DB(PHP) vs Docker DB(Rust)")
        print("="*60)

        if not self.connect_prod() or not self.connect_docker():
            return {}

        first_of_month = f"{year}-{month:02d}-01"
        days_in_month = self._get_days_in_month(year, month)
        last_of_month = f"{year}-{month:02d}-{days_in_month:02d}"

        results = {'match': 0, 'mismatch': 0, 'prod_only': 0, 'docker_only': 0, 'details': []}

        for driver_id in driver_ids:
            # 本番DBから取得（デジタコ）
            prod_cursor = self.prod_conn.cursor(dictionary=True)
            prod_cursor.execute(f"""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') as date, SUM(minutes) as minutes
                FROM time_card_kosoku
                WHERE driver_id = {driver_id}
                AND type = 'デジタコ'
                AND date >= '{first_of_month}' AND date <= '{last_of_month}'
                GROUP BY date
                ORDER BY date
            """)
            prod_data = {row['date']: row['minutes'] for row in prod_cursor.fetchall()}
            prod_cursor.close()

            # Docker DBから取得（デジタコRust計算結果）
            docker_cursor = self.docker_conn.cursor(dictionary=True)
            docker_cursor.execute(f"""
                SELECT DATE_FORMAT(date, '%Y-%m-%d') as date, SUM(minutes) as minutes
                FROM time_card_kosoku
                WHERE driver_id = {driver_id}
                AND type = 'デジタコRust'
                AND date >= '{first_of_month}' AND date <= '{last_of_month}'
                GROUP BY date
                ORDER BY date
            """)
            docker_data = {row['date']: row['minutes'] for row in docker_cursor.fetchall()}
            docker_cursor.close()

            # 比較
            all_dates = set(prod_data.keys()) | set(docker_data.keys())
            for date in sorted(all_dates):
                prod_val = prod_data.get(date)
                docker_val = docker_data.get(date)

                if prod_val == docker_val:
                    results['match'] += 1
                elif prod_val is None:
                    results['docker_only'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'date': date,
                        'prod': None,
                        'docker': docker_val,
                        'status': 'Docker only'
                    })
                elif docker_val is None:
                    results['prod_only'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'date': date,
                        'prod': prod_val,
                        'docker': None,
                        'status': 'Prod only'
                    })
                else:
                    results['mismatch'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'date': date,
                        'prod': prod_val,
                        'docker': docker_val,
                        'diff': docker_val - prod_val,
                        'status': 'MISMATCH'
                    })

        # サマリー表示
        print(f"\n一致: {results['match']}件")
        print(f"不一致: {results['mismatch']}件")
        print(f"本番のみ: {results['prod_only']}件")
        print(f"Dockerのみ: {results['docker_only']}件")

        if results['details']:
            print("\n--- 差異詳細（先頭20件）---")
            for d in results['details'][:20]:
                if d['status'] == 'MISMATCH':
                    print(f"  Driver {d['driver_id']} / {d['date']}: PHP={d['prod']}分, Rust={d['docker']}分 (差:{d['diff']}分)")
                else:
                    print(f"  Driver {d['driver_id']} / {d['date']}: {d['status']} (PHP={d['prod']}, Rust={d['docker']})")

        return results

    def compare_allowance(self, year: int, month: int, driver_ids: List[int]) -> Dict:
        """
        本番DB（PHP計算）とDocker DB（Rust計算）のtime_card_allowanceを比較
        """
        print("\n" + "="*60)
        print("time_card_allowance比較: 本番DB(PHP) vs Docker DB(Rust)")
        print("="*60)

        if not self.connect_prod() or not self.connect_docker():
            return {}

        first_of_month = f"{year}-{month:02d}-01"

        results = {'match': 0, 'mismatch': 0, 'prod_only': 0, 'docker_only': 0, 'details': []}

        # 比較するカラム
        compare_cols = [
            'shukkin_count', 'dayoff_count', 'paidoff_count', 'absence_count',
            'overtime_count', 'holidaywork_count', 'additionalwork_payment',
            'kachiku_payment', 'trail_payment', 'chikoku_count', 'soutai_count', 'tokukyu_count'
        ]

        for driver_id in driver_ids:
            # 本番DBから取得
            prod_cursor = self.prod_conn.cursor(dictionary=True)
            prod_cursor.execute(f"""
                SELECT {', '.join(compare_cols)}
                FROM time_card_allowance
                WHERE driver_id = {driver_id}
                AND datetime = '{first_of_month}'
            """)
            prod_row = prod_cursor.fetchone()
            prod_cursor.close()

            # Docker DBから取得
            docker_cursor = self.docker_conn.cursor(dictionary=True)
            docker_cursor.execute(f"""
                SELECT {', '.join(compare_cols)}
                FROM time_card_allowance
                WHERE driver_id = {driver_id}
                AND datetime = '{first_of_month}'
            """)
            docker_row = docker_cursor.fetchone()
            docker_cursor.close()

            if prod_row is None and docker_row is None:
                continue
            elif prod_row is None:
                results['docker_only'] += 1
                results['details'].append({
                    'driver_id': driver_id,
                    'status': 'Docker only',
                    'docker': docker_row
                })
            elif docker_row is None:
                results['prod_only'] += 1
                results['details'].append({
                    'driver_id': driver_id,
                    'status': 'Prod only',
                    'prod': prod_row
                })
            else:
                # 各カラムを比較
                diffs = {}
                for col in compare_cols:
                    prod_val = prod_row[col] if prod_row[col] is not None else 0
                    docker_val = docker_row[col] if docker_row[col] is not None else 0
                    # 浮動小数点の比較は誤差を考慮
                    if isinstance(prod_val, float) or isinstance(docker_val, float):
                        if abs(float(prod_val) - float(docker_val)) > 0.01:
                            diffs[col] = {'prod': prod_val, 'docker': docker_val}
                    else:
                        if prod_val != docker_val:
                            diffs[col] = {'prod': prod_val, 'docker': docker_val}

                if diffs:
                    results['mismatch'] += 1
                    results['details'].append({
                        'driver_id': driver_id,
                        'status': 'MISMATCH',
                        'diffs': diffs
                    })
                else:
                    results['match'] += 1

        # サマリー表示
        print(f"\n一致: {results['match']}件")
        print(f"不一致: {results['mismatch']}件")
        print(f"本番のみ: {results['prod_only']}件")
        print(f"Dockerのみ: {results['docker_only']}件")

        if results['details']:
            print("\n--- 差異詳細（先頭20件）---")
            for d in results['details'][:20]:
                if d['status'] == 'MISMATCH':
                    print(f"  Driver {d['driver_id']}:")
                    for col, vals in d['diffs'].items():
                        print(f"    {col}: PHP={vals['prod']}, Rust={vals['docker']}")
                elif d['status'] == 'Docker only':
                    print(f"  Driver {d['driver_id']}: Docker only")
                else:
                    print(f"  Driver {d['driver_id']}: Prod only")

        return results

    def init_docker_kosoku(self, year: int, month: int):
        """Docker DBのtime_card_kosokuを初期化"""
        print("\n" + "="*60)
        print("Docker DB time_card_kosoku 初期化")
        print("="*60)

        if not self.connect_docker():
            return False

        first_of_month = f"{year}-{month:02d}-01"
        days_in_month = self._get_days_in_month(year, month)
        last_of_month = f"{year}-{month:02d}-{days_in_month:02d}"

        cursor = self.docker_conn.cursor()
        cursor.execute(f"""
            DELETE FROM time_card_kosoku
            WHERE date >= '{first_of_month}' AND date <= '{last_of_month}'
        """)
        deleted = cursor.rowcount
        self.docker_conn.commit()
        cursor.close()

        print(f"[OK] {deleted}件削除完了")
        return True

    def _get_days_in_month(self, year: int, month: int) -> int:
        """月の日数を取得"""
        from datetime import datetime, timedelta
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)
        return last_day.day


def main():
    parser = argparse.ArgumentParser(description='DB検証スクリプト')
    parser.add_argument('--init', action='store_true', help='Docker DBのtime_card_kosokuを初期化')
    parser.add_argument('--compare', action='store_true', help='本番DB vs Docker DB の拘束時間比較（TC_DC版）')
    parser.add_argument('--compare-dtako', action='store_true', help='本番DB vs Docker DB の拘束時間比較（デジタコ版）')
    parser.add_argument('--compare-allowance', action='store_true', help='本番DB vs Docker DB のtime_card_allowance比較')
    parser.add_argument('--year', type=int, default=2025, help='対象年（デフォルト: 2025）')
    parser.add_argument('--month', type=int, default=12, help='対象月（デフォルト: 12）')
    parser.add_argument('--driver-ids', type=str, help='ドライバーID（カンマ区切り、省略時は全ドライバー）')

    args = parser.parse_args()

    verifier = DbVerifier()

    try:
        # ドライバーID取得
        if args.driver_ids:
            driver_ids = [int(x.strip()) for x in args.driver_ids.split(',')]
        else:
            driver_ids = verifier.get_active_driver_ids(args.year, args.month)

        if not driver_ids:
            print("[ERROR] ドライバーIDが取得できません")
            return 1

        # Docker DB初期化
        if args.init:
            verifier.init_docker_kosoku(args.year, args.month)

        # 比較（TC_DC版）
        if args.compare:
            verifier.compare_kosoku(args.year, args.month, driver_ids)

        # 比較（デジタコ版）
        if args.compare_dtako:
            verifier.compare_kosoku_digitacho(args.year, args.month, driver_ids)

        # 比較（allowance）
        if args.compare_allowance:
            verifier.compare_allowance(args.year, args.month, driver_ids)

        # 引数なしの場合はヘルプ表示
        if not any([args.init, args.compare, args.compare_dtako, args.compare_allowance]):
            parser.print_help()
            print("\n使用例:")
            print("  # TC_DC版: Docker DB初期化 → Rust実行 → 比較")
            print("  python db_verify.py --init --year 2025 --month 12")
            print("  cargo run -- verify 2025 12")
            print("  python db_verify.py --compare --year 2025 --month 12")
            print("")
            print("  # デジタコ版: Rust実行 → 比較")
            print("  cargo run -- verify-dtako 2025 12")
            print("  python db_verify.py --compare-dtako --year 2025 --month 12")

    finally:
        verifier.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
