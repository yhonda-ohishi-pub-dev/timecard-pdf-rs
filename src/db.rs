use mysql::*;
use mysql::prelude::*;
use chrono::{NaiveDateTime, NaiveDate, Datelike, Weekday};
use std::env;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use crate::timecard_data::{Driver, DayRecord, MonthlyTimecard, TimecardSummary};

/// time_card_allowanceのハッシュ比較用構造体
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AllowanceData {
    pub driver_id: i32,
    pub shukkin_count: i64,      // f64 * 10 で整数化（比較用）
    pub dayoff_count: i64,
    pub paidoff_count: i64,
    pub absence_count: i64,
    pub overtime_count: i64,
    pub holidaywork_count: i64,
    pub additionalwork_payment: i32,
    pub kachiku_payment: i32,
    pub trail_payment: i32,
    pub chikoku_count: i32,
    pub soutai_count: i32,
    pub tokukyu_count: i32,
}

impl AllowanceData {
    /// MonthlyTimecardから生成
    pub fn from_timecard(tc: &MonthlyTimecard) -> Self {
        Self {
            driver_id: tc.driver.id,
            shukkin_count: (tc.summary.shukkin * 10.0) as i64,
            dayoff_count: (tc.summary.kyuka as f64 * 10.0) as i64,
            paidoff_count: (tc.summary.yukyu * 10.0) as i64,
            absence_count: (tc.summary.kekkin as f64 * 10.0) as i64,
            overtime_count: (tc.summary.total_zangyo * 10.0) as i64,
            holidaywork_count: (tc.summary.kyushutsu * 10.0) as i64,
            additionalwork_payment: tc.summary.tsuika,
            kachiku_payment: tc.summary.kachiku,
            trail_payment: tc.summary.trailer,
            chikoku_count: tc.summary.chikoku,
            soutai_count: tc.summary.soutai,
            tokukyu_count: tc.summary.tokukyu,
        }
    }

    /// ハッシュ値を計算
    pub fn compute_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// データベース接続設定
#[derive(Clone)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl DbConfig {
    /// 環境変数から設定を読み込み（プレフィックス付き）
    /// 例: PROD_DB_HOST, DOCKER_DB_HOST
    fn from_env_with_prefix(prefix: &str) -> Self {
        Self {
            host: env::var(format!("{}_DB_HOST", prefix)).unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: env::var(format!("{}_DB_PORT", prefix))
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3306),
            user: env::var(format!("{}_DB_USER", prefix)).unwrap_or_else(|_| "root".to_string()),
            password: env::var(format!("{}_DB_PASSWORD", prefix)).unwrap_or_else(|_| "".to_string()),
            database: env::var(format!("{}_DB_NAME", prefix)).unwrap_or_else(|_| "db1".to_string()),
        }
    }

    /// 本番DB設定（読み取り専用）
    /// 環境変数: PROD_DB_HOST, PROD_DB_PORT, PROD_DB_USER, PROD_DB_PASSWORD, PROD_DB_NAME
    pub fn production() -> Self {
        Self::from_env_with_prefix("PROD")
    }

    /// Docker DB設定（開発用）
    /// 環境変数: DOCKER_DB_HOST, DOCKER_DB_PORT, DOCKER_DB_USER, DOCKER_DB_PASSWORD, DOCKER_DB_NAME
    pub fn docker() -> Self {
        Self::from_env_with_prefix("DOCKER")
    }

    /// 接続URLを生成
    fn connection_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

/// タイムカードデータベースアクセス
pub struct TimecardDb {
    pool: Pool,
}

impl TimecardDb {
    /// データベースに接続
    pub fn connect(config: &DbConfig) -> Result<Self> {
        let opts = Opts::from_url(&config.connection_url())?;
        let pool = Pool::new(opts)?;
        Ok(Self { pool })
    }

    /// 基礎日数を取得（kyuyo_kiso_dateテーブルから）
    /// PHPの_getKisoDate()と同等
    pub fn get_kiso_date(&self, year: i32, month: u32) -> Result<i32> {
        let mut conn = self.pool.get_conn()?;
        let date_str = format!("{}-{:02}-01", year, month);

        let kiso_date: Option<i32> = conn.query_first(
            format!(
                "SELECT kiso_date FROM kyuyo_kiso_date WHERE month = '{}'",
                date_str
            )
        )?;

        Ok(kiso_date.unwrap_or(0))
    }

    /// アクティブなドライバー一覧を取得（給与番号順にソート）
    /// PHPと同じロジック: kyuyo_shainテーブルのretire_dateで判定
    /// フィルター条件:
    ///   - eigyosho_c = 1 (営業所コード1のみ)
    ///   - category_c != 1 (役員除外)
    ///   - retire_date > 対象月 OR NULL (退職者除外)
    ///   - hire_date < 対象月翌月 (入社済みのみ)
    ///   - TimeCardExceptionテーブルで除外された人を除外
    ///   - time_card_yakinでparent_kyuyo_shain_idがあるものを除外
    /// ソート順: firm_id ASC, category_c ASC, id ASC
    pub fn get_active_drivers(&self, year: i32, month: u32) -> Result<Vec<Driver>> {
        let mut conn = self.pool.get_conn()?;

        // 対象月の初日
        let first_of_month = format!("{}-{:02}-01", year, month);
        // 対象月の翌月初日
        let next_month_first = if month == 12 {
            format!("{}-01-01", year + 1)
        } else {
            format!("{}-{:02}-01", year, month + 1)
        };

        // PHPと同じフィルター条件
        let drivers: Vec<Driver> = conn.query_map(
            format!(
                "SELECT d.id, d.name, d.bumon, ks.category_c, ks.eigyosho_c, ks.id as kyuyo_shain_id
                 FROM drivers d
                 INNER JOIN kyuyo_shain ks ON ks.driver_id = d.id
                 LEFT JOIN time_card_yakin tcy ON tcy.parent_kyuyo_shain_id = ks.id AND tcy.parent_firm_id = ks.firm_id
                 LEFT JOIN time_card_exception tce ON tce.kyuyo_shain_id = ks.id AND tce.firm_id = ks.firm_id
                   AND tce.start_month <= '{0}'
                   AND (tce.end_month > '{0}' OR tce.end_month IS NULL)
                 WHERE ks.eigyosho_c = 1
                   AND ks.category_c != 1
                   AND (ks.retire_date IS NULL OR ks.retire_date > '{0}')
                   AND ks.hire_date < '{1}'
                   AND tcy.kyuyo_shain_id IS NULL
                   AND tce.kyuyo_shain_id IS NULL
                 ORDER BY ks.firm_id ASC,
                          ks.category_c ASC,
                          ks.id ASC",
                first_of_month, next_month_first
            ),
            |(id, name, bumon, category_c, eigyosho_c, kyuyo_shain_id): (i32, String, Option<i32>, Option<i32>, Option<i32>, Option<i32>)| {
                Driver { id, name, bumon, category_c, eigyosho_c, kyuyo_shain_id }
            }
        )?;

        Ok(drivers)
    }

    /// 指定ドライバーの月別タイムカードデータを取得
    pub fn get_monthly_timecard(&self, driver: &Driver, year: i32, month: u32) -> Result<MonthlyTimecard> {
        let mut conn = self.pool.get_conn()?;

        // 月の日数を取得
        let days_in_month = get_days_in_month(year, month);

        // 各日のレコードを初期化
        let mut days: Vec<DayRecord> = (1..=days_in_month)
            .map(|day| {
                let date = NaiveDate::from_ymd_opt(year, month, day as u32).unwrap();
                let weekday = weekday_to_japanese(date.weekday());
                DayRecord::new(day, &weekday)
            })
            .collect();

        // 打刻データを取得 (time_card_dstate)
        let start_date = format!("{}-{:02}-01 00:00:00", year, month);
        let end_date = format!("{}-{:02}-{:02} 23:59:59", year, month, days_in_month);

        // datetimeを文字列として取得し、手動でパース
        let punches: Vec<(String, i32)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(datetime, '%Y-%m-%d %H:%i:%s') as dt, state FROM time_card_dstate
                 WHERE id = {}
                 AND datetime BETWEEN '{}' AND '{}'
                 ORDER BY datetime",
                driver.id, start_date, end_date
            ),
            |(datetime, state): (String, i32)| (datetime, state)
        )?;

        // 打刻データを日毎に振り分け
        for (datetime_str, state) in punches {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S") {
                let day = datetime.day() as usize;
                if day >= 1 && day <= days.len() {
                    let time_str = datetime.format("%H:%M").to_string();
                    let record = &mut days[day - 1];

                    match state {
                        30 => { // 始業
                            if record.clock_in.len() < 2 {
                                record.clock_in.push(time_str);
                            }
                        }
                        31 => { // 終業
                            if record.clock_out.len() < 2 {
                                record.clock_out.push(time_str);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // 手動入力データを取得 (time_card_inject)
        let injects: Vec<String> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(datetime, '%Y-%m-%d %H:%i:%s') as dt FROM time_card_inject
                 WHERE driver_id = {}
                 AND datetime BETWEEN '{}' AND '{}'
                 ORDER BY datetime",
                driver.id, start_date, end_date
            ),
            |datetime: String| datetime
        )?;

        // 手動入力データを日毎に振り分け（出勤/退勤を交互に）
        for datetime_str in injects {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S") {
                let day = datetime.day() as usize;
                if day >= 1 && day <= days.len() {
                    let time_str = datetime.format("%H:%M").to_string();
                    let record = &mut days[day - 1];

                    // 出勤が少なければ出勤に、そうでなければ退勤に追加
                    if record.clock_in.len() <= record.clock_out.len() && record.clock_in.len() < 2 {
                        record.clock_in.push(time_str);
                    } else if record.clock_out.len() < 2 {
                        record.clock_out.push(time_str);
                    }
                }
            }
        }

        // 休暇データを取得 (daily_report_other_detail)
        let holidays: Vec<(String, String)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(act_date, '%Y-%m-%d') as dt, detail FROM daily_report_other_detail
                 WHERE driver_id = {}
                 AND act_date BETWEEN '{}-{:02}-01' AND '{}-{:02}-{:02}'
                 ORDER BY act_date",
                driver.id, year, month, year, month, days_in_month
            ),
            |(act_date, detail): (String, String)| (act_date, detail)
        )?;

        // 休暇データを備考に設定
        for (date_str, detail) in holidays {
            if let Ok(act_date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let day = act_date.day() as usize;
                if day >= 1 && day <= days.len() {
                    days[day - 1].remarks = detail;
                }
            }
        }

        // 拘束時間をDocker DBのtime_card_kosokuテーブルから取得
        // Rust計算とデジタコRustの両方を取得し、デジタコRustがあればデジタコRust、なければRust計算を使用
        let docker_config = DbConfig::docker();
        let docker_pool = Pool::new(Opts::from_url(&docker_config.connection_url())?)?;
        let mut docker_conn = docker_pool.get_conn()?;

        let kosoku_digitacho: Vec<(u32, i32)> = docker_conn.query_map(
            format!(
                "SELECT DAY(date), minutes FROM time_card_kosoku
                 WHERE driver_id = {}
                 AND date >= '{}-{:02}-01'
                 AND date < '{}-{:02}-01'
                 AND type = 'デジタコRust'",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |(day, minutes): (u32, i32)| (day, minutes)
        )?;

        let kosoku_tcdc: Vec<(u32, i32)> = docker_conn.query_map(
            format!(
                "SELECT DAY(date), minutes FROM time_card_kosoku
                 WHERE driver_id = {}
                 AND date >= '{}-{:02}-01'
                 AND date < '{}-{:02}-01'
                 AND type = 'Rust計算'",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |(day, minutes): (u32, i32)| (day, minutes)
        )?;

        // デジタコRustを優先、なければRust計算を使用
        let mut kosoku_map: std::collections::HashMap<u32, i32> = std::collections::HashMap::new();
        for (day, minutes) in kosoku_tcdc {
            kosoku_map.insert(day, minutes);
        }
        for (day, minutes) in kosoku_digitacho {
            kosoku_map.insert(day, minutes); // デジタコRustで上書き
        }

        for (day, minutes) in kosoku_map {
            if day >= 1 && day <= days.len() as u32 {
                days[day as usize - 1].kosoku_minutes = Some(minutes);
            }
        }

        // デジタコデータがある日を取得（本番DBのtime_card_kosokuテーブル、type='デジタコ'）
        // PHPの$drive配列と同等: 出退勤記号を[/]にするか</>にするかの判定に使用
        let digitacho_days: Vec<u32> = conn.query_map(
            format!(
                "SELECT DAY(date) FROM time_card_kosoku
                 WHERE driver_id = {}
                 AND date >= '{}-{:02}-01'
                 AND date < '{}-{:02}-01'
                 AND type = 'デジタコ'",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |day: u32| day
        )?;

        for day in digitacho_days {
            if day >= 1 && day <= days.len() as u32 {
                days[day as usize - 1].has_digitacho = true;
            }
        }

        // 「出」マーク（出張中）を取得 - ryohi_rowsの開始日時〜終了日時が複数日にまたがる場合
        // PHPの_make_ryohi_zangyo関数と同じロジック
        let start_month_parsed = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let end_month_parsed = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
        };

        // ryohi_row_split_lineがある場合
        let split_lines: Vec<(String, String)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(rsl.start_datetime, '%Y-%m-%d') as start_dt,
                        DATE_FORMAT(rsl.end_datetime, '%Y-%m-%d') as end_dt
                 FROM ryohi_row_split_line rsl
                 INNER JOIN ryohi_rows rr ON rr.id = rsl.ryohi_row_id
                 WHERE rr.driver_id = '{}'
                 AND (
                     (rsl.start_datetime >= '{}-{:02}-01' AND rsl.start_datetime < '{}-{:02}-01')
                     OR (rsl.end_datetime >= '{}-{:02}-01' AND rsl.end_datetime < '{}-{:02}-01')
                 )",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 },
                year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |(start_dt, end_dt): (String, String)| (start_dt, end_dt)
        )?;

        // split_lineのある旅費IDを取得
        let ryohi_ids_with_split: Vec<String> = conn.query_map(
            format!(
                "SELECT DISTINCT rr.id
                 FROM ryohi_rows rr
                 INNER JOIN ryohi_row_split_line rsl ON rsl.ryohi_row_id = rr.id
                 WHERE rr.driver_id = '{}'",
                driver.id
            ),
            |id: String| id
        )?;

        // ryohi_row_split_lineがない場合のryohi_rows
        let ryohi_direct: Vec<(String, String, String, Option<String>, i32)> = conn.query_map(
            format!(
                "SELECT rr.id, DATE_FORMAT(rr.開始日時, '%Y-%m-%d') as start_dt,
                        DATE_FORMAT(rr.終了日時, '%Y-%m-%d') as end_dt,
                        rr.適用, rr.fl_show
                 FROM ryohi_rows rr
                 WHERE rr.driver_id = '{}'
                 AND rr.開始日時 IS NOT NULL
                 AND (
                     (rr.開始日時 >= '{}-{:02}-01' AND rr.開始日時 < '{}-{:02}-01')
                     OR (rr.終了日時 >= '{}-{:02}-01' AND rr.終了日時 < '{}-{:02}-01')
                 )",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 },
                year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |(id, start_dt, end_dt, tekiyo, fl_show): (String, String, String, Option<String>, i32)| {
                (id, start_dt, end_dt, tekiyo, fl_show)
            }
        )?;

        // 「出」マークを設定
        // split_lineから
        for (start_str, end_str) in split_lines {
            if let (Ok(start_date), Ok(end_date)) = (
                NaiveDate::parse_from_str(&start_str, "%Y-%m-%d"),
                NaiveDate::parse_from_str(&end_str, "%Y-%m-%d")
            ) {
                // 複数日にまたがる場合のみ
                if end_date > start_date {
                    let mut current = start_date;
                    while current <= end_date {
                        if current >= start_month_parsed && current < end_month_parsed {
                            let day = current.day() as usize;
                            if day >= 1 && day <= days.len() && days[day - 1].remarks.is_empty() {
                                days[day - 1].remarks = "出".to_string();
                            }
                        }
                        current = current.succ_opt().unwrap();
                    }
                }
            }
        }

        // ryohi_rowsから直接（split_lineがないもののみ）
        for (id, start_str, end_str, tekiyo, fl_show) in ryohi_direct {
            // split_lineがあるものはスキップ
            if ryohi_ids_with_split.contains(&id) {
                continue;
            }
            // 適用が「北海道残業」またはfl_show=0の場合はスキップ
            if tekiyo.as_deref() == Some("北海道残業") || fl_show == 0 {
                continue;
            }
            if let (Ok(start_date), Ok(end_date)) = (
                NaiveDate::parse_from_str(&start_str, "%Y-%m-%d"),
                NaiveDate::parse_from_str(&end_str, "%Y-%m-%d")
            ) {
                // 複数日にまたがる場合のみ
                if end_date > start_date {
                    let mut current = start_date;
                    while current <= end_date {
                        if current >= start_month_parsed && current < end_month_parsed {
                            let day = current.day() as usize;
                            if day >= 1 && day <= days.len() && days[day - 1].remarks.is_empty() {
                                days[day - 1].remarks = "出".to_string();
                            }
                        }
                        current = current.succ_opt().unwrap();
                    }
                }
            }
        }

        // 残業データを取得 (ryohi_rows + time_card_zangyo)
        // PHPの_make_ryohi_zangyo関数と同じロジック
        let zangyo_from_ryohi: Vec<(String, f64)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(残業適用日, '%Y-%m-%d') as dt, 残業
                 FROM ryohi_rows
                 WHERE driver_id = '{}'
                 AND (適用 IS NULL OR 適用 != '除外')
                 AND 残業適用日 >= '{}-{:02}-01'
                 AND 残業適用日 < '{}-{:02}-01'
                 AND 残業 <> 0",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |(date, zangyo): (String, f64)| (date, zangyo)
        )?;

        let zangyo_from_tc: Vec<(String, f64)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(shori_date, '%Y-%m-%d') as dt, zangyo
                 FROM time_card_zangyo
                 WHERE driver_id = {}
                 AND shori_date >= '{}-{:02}-01'
                 AND shori_date < '{}-{:02}-01'
                 AND zangyo <> 0",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |(date, zangyo): (String, f64)| (date, zangyo)
        )?;

        // 残業を設定（同じ日の値は加算）
        for (date_str, zangyo) in zangyo_from_ryohi.into_iter().chain(zangyo_from_tc.into_iter()) {
            if let Ok(date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let day = date.day() as usize;
                if day >= 1 && day <= days.len() {
                    let current = days[day - 1].zangyo.unwrap_or(0.0);
                    days[day - 1].zangyo = Some(current + zangyo);
                }
            }
        }

        // ドライバーカテゴリを取得（家畜車=1, トレーラー=2）
        // driver_category + driver_category_name で現在有効なカテゴリを取得
        let driver_category: Option<String> = conn.query_first(
            format!(
                "SELECT dcn.name FROM driver_category dc
                 JOIN driver_category_name dcn ON dc.category_c = dcn.id
                 WHERE dc.driver_id = {}
                 AND (dc.end_date IS NULL OR dc.end_date > '{}-{:02}-01')",
                driver.id, year, month
            )
        )?;

        // ドライバーカテゴリに基づくマーク（dtako_rowsの運行日全てにフラグ）
        // PHPの_count_teateと同様、先月最後の運行から継続するロジックを実装
        if let Some(ref cat_name) = driver_category {
            if cat_name == "家畜車" || cat_name == "トレーラー" {
                // dtako_rowsから運行期間を取得（休暇日を除外）
                let kyuka_dates: Vec<String> = conn.query_map(
                    format!(
                        "SELECT DATE_FORMAT(act_date, '%Y-%m-%d') FROM daily_report_other_detail
                         WHERE driver_id = {}
                         AND act_date >= '{}-{:02}-01'
                         AND act_date < '{}-{:02}-01'
                         AND detail IN ('公休', '有休', '泊休')",
                        driver.id, year, month,
                        if month == 12 { year + 1 } else { year },
                        if month == 12 { 1 } else { month + 1 }
                    ),
                    |date: String| date
                )?;
                let kyuka_set: std::collections::HashSet<String> = kyuka_dates.into_iter().collect();

                // PHPの_count_teateと同じロジック: 先月最後のdtako_rowを取得
                // 旅費が「除外」のものは除く（運行NOで結合）
                let last_dtako_datetime: Option<String> = conn.query_first(
                    format!(
                        "SELECT DATE_FORMAT(dr.出庫日時, '%Y-%m-%d %H:%i:%s')
                         FROM dtako_rows dr
                         LEFT JOIN ryohi_rows rr ON rr.運行NO = CONCAT(dr.運行NO, dr.対象乗務員区分) AND rr.適用 = '除外'
                         WHERE dr.対象乗務員CD = {}
                         AND dr.出庫日時 < '{}-{:02}-01'
                         AND rr.id IS NULL
                         ORDER BY dr.出庫日時 DESC
                         LIMIT 1",
                        driver.id, year, month
                    )
                )?;

                // 先月分がない場合は今月最初のdtako_rowを取得
                let last_dtako_datetime = if last_dtako_datetime.is_none() {
                    conn.query_first::<String, _>(
                        format!(
                            "SELECT DATE_FORMAT(dr.出庫日時, '%Y-%m-%d %H:%i:%s')
                             FROM dtako_rows dr
                             LEFT JOIN ryohi_rows rr ON rr.運行NO = CONCAT(dr.運行NO, dr.対象乗務員区分) AND rr.適用 = '除外'
                             WHERE dr.対象乗務員CD = {}
                             AND dr.出庫日時 >= '{}-{:02}-01'
                             AND rr.id IS NULL
                             ORDER BY dr.出庫日時 ASC
                             LIMIT 1",
                            driver.id, year, month
                        )
                    )?
                } else {
                    last_dtako_datetime
                };

                // last_dtako_datetime以降のdtako_rowsを取得
                if let Some(ref last_dt) = last_dtako_datetime {
                    let dtako_periods: Vec<(String, String)> = conn.query_map(
                        format!(
                            "SELECT DATE_FORMAT(dr.出庫日時, '%Y-%m-%d'), DATE_FORMAT(dr.帰庫日時, '%Y-%m-%d')
                             FROM dtako_rows dr
                             LEFT JOIN ryohi_rows rr ON rr.運行NO = CONCAT(dr.運行NO, dr.対象乗務員区分) AND rr.適用 = '除外'
                             WHERE dr.対象乗務員CD = {}
                             AND dr.出庫日時 >= '{}'
                             AND rr.id IS NULL",
                            driver.id, last_dt
                        ),
                        |(start, end): (String, String)| (start, end)
                    )?;

                    for (start_str, end_str) in dtako_periods {
                        if let (Ok(start_date), Ok(end_date)) = (
                            NaiveDate::parse_from_str(&start_str, "%Y-%m-%d"),
                            NaiveDate::parse_from_str(&end_str, "%Y-%m-%d")
                        ) {
                            let mut current = start_date;
                            // PHPのDatePeriodは帰庫日も含む（出庫日0時から帰庫日時までループ）
                            while current <= end_date {
                                if current >= start_month_parsed && current < end_month_parsed {
                                    let date_key = current.format("%Y-%m-%d").to_string();
                                    if !kyuka_set.contains(&date_key) {
                                        let day = current.day() as usize;
                                        if day >= 1 && day <= days.len() {
                                            if cat_name == "家畜車" {
                                                days[day - 1].is_kachiku = true;
                                            } else if cat_name == "トレーラー" {
                                                days[day - 1].is_trailer = true;
                                            }
                                        }
                                    }
                                }
                                current = current.succ_opt().unwrap();
                            }
                        }
                    }
                }
            }
        }

        // 家畜マーク追加: daily_report_other_detail.detail = '家畜'の日付
        let kachiku_dates: Vec<String> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(act_date, '%Y-%m-%d') FROM daily_report_other_detail
                 WHERE driver_id = {}
                 AND act_date >= '{}-{:02}-01'
                 AND act_date < '{}-{:02}-01'
                 AND detail = '家畜'",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |date: String| date
        )?;

        for date_str in kachiku_dates {
            if let Ok(date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let day = date.day() as usize;
                if day >= 1 && day <= days.len() {
                    days[day - 1].is_kachiku = true;
                }
            }
        }

        // トレーラーマーク追加: dtako_rows + cars.旅費分類 = 'けん引' または daily_report_other_detail.detail = 'けん引'
        // PHPの_count_teateと同様、先月最後の運行から継続するロジックを実装
        // 休暇リストを取得（PHPと同じ: 公休, 有休, 泊休）
        let kyuka_for_trailer: Vec<String> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(act_date, '%Y-%m-%d') FROM daily_report_other_detail
                 WHERE driver_id = {}
                 AND act_date >= '{}-{:02}-01'
                 AND act_date < '{}-{:02}-01'
                 AND detail IN ('公休', '有休', '泊休')",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |date: String| date
        )?;
        let kyuka_set_trailer: std::collections::HashSet<String> = kyuka_for_trailer.into_iter().collect();

        // PHPの_count_teateと同じロジック: 先月最後のdtako_rowを取得（車種問わず任意の運行）
        // 旅費が「除外」のものは除く（運行NOで結合）
        let last_trailer_dtako_datetime: Option<String> = conn.query_first(
            format!(
                "SELECT DATE_FORMAT(dr.出庫日時, '%Y-%m-%d %H:%i:%s')
                 FROM dtako_rows dr
                 LEFT JOIN ryohi_rows rr ON rr.運行NO = CONCAT(dr.運行NO, dr.対象乗務員区分) AND rr.適用 = '除外'
                 WHERE dr.対象乗務員CD = {}
                 AND dr.出庫日時 < '{}-{:02}-01'
                 AND rr.id IS NULL
                 ORDER BY dr.出庫日時 DESC
                 LIMIT 1",
                driver.id, year, month
            )
        )?;

        // 先月分がない場合は今月最初のけん引dtako_rowを取得
        let last_trailer_dtako_datetime = if last_trailer_dtako_datetime.is_none() {
            conn.query_first::<String, _>(
                format!(
                    "SELECT DATE_FORMAT(dr.出庫日時, '%Y-%m-%d %H:%i:%s')
                     FROM dtako_rows dr
                     INNER JOIN cars c ON c.id = dr.車輌CC
                     INNER JOIN ryohi_sharyo_bunrui_rows rsbr ON rsbr.車輌R = c.name_R
                     LEFT JOIN ryohi_rows rr ON rr.運行NO = CONCAT(dr.運行NO, dr.対象乗務員区分) AND rr.適用 = '除外'
                     WHERE dr.対象乗務員CD = {}
                     AND dr.出庫日時 >= '{}-{:02}-01'
                     AND rsbr.旅費分類 = 'けん引'
                     AND rr.id IS NULL
                     ORDER BY dr.出庫日時 ASC
                     LIMIT 1",
                    driver.id, year, month
                )
            )?
        } else {
            last_trailer_dtako_datetime
        };

        // last_trailer_dtako_datetime以降のけん引dtako_rowsを取得
        if let Some(ref last_dt) = last_trailer_dtako_datetime {
            let trailer_from_dtako: Vec<(String, String)> = conn.query_map(
                format!(
                    "SELECT DATE_FORMAT(dr.出庫日時, '%Y-%m-%d'), DATE_FORMAT(dr.帰庫日時, '%Y-%m-%d')
                     FROM dtako_rows dr
                     INNER JOIN cars c ON c.id = dr.車輌CC
                     INNER JOIN ryohi_sharyo_bunrui_rows rsbr ON rsbr.車輌R = c.name_R
                     LEFT JOIN ryohi_rows rr ON rr.運行NO = CONCAT(dr.運行NO, dr.対象乗務員区分) AND rr.適用 = '除外'
                     WHERE dr.対象乗務員CD = {}
                     AND dr.出庫日時 >= '{}'
                     AND rsbr.旅費分類 = 'けん引'
                     AND rr.id IS NULL",
                    driver.id, last_dt
                ),
                |(start, end): (String, String)| (start, end)
            )?;

            for (start_str, end_str) in trailer_from_dtako {
                if let (Ok(start_date), Ok(end_date)) = (
                    NaiveDate::parse_from_str(&start_str, "%Y-%m-%d"),
                    NaiveDate::parse_from_str(&end_str, "%Y-%m-%d")
                ) {
                    let mut current = start_date;
                    // PHPのDatePeriodは帰庫日も含む（出庫日0時から帰庫日時までループ）
                    while current <= end_date {
                        if current >= start_month_parsed && current < end_month_parsed {
                            let date_key = current.format("%Y-%m-%d").to_string();
                            // 休暇日を除外（PHPと同じロジック）
                            if !kyuka_set_trailer.contains(&date_key) {
                                let day = current.day() as usize;
                                if day >= 1 && day <= days.len() {
                                    days[day - 1].is_trailer = true;
                                }
                            }
                        }
                        current = current.succ_opt().unwrap();
                    }
                }
            }
        }

        // daily_report_other_detail.detail = 'けん引'からもトレーラーマーク
        let trailer_from_detail: Vec<String> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(act_date, '%Y-%m-%d') FROM daily_report_other_detail
                 WHERE driver_id = {}
                 AND act_date >= '{}-{:02}-01'
                 AND act_date < '{}-{:02}-01'
                 AND detail = 'けん引'",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            ),
            |date: String| date
        )?;

        for date_str in trailer_from_detail {
            if let Ok(date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let day = date.day() as usize;
                if day >= 1 && day <= days.len() {
                    days[day - 1].is_trailer = true;
                }
            }
        }

        // 手当データ集計 - 日別フラグからカウント
        let mut summary = TimecardSummary::default();

        // 家畜・トレーラー手当カウント（日別フラグから集計）
        for day in &days {
            if day.is_kachiku {
                summary.kachiku += 1;
            }
            if day.is_trailer {
                summary.trailer += 1;
            }
        }

        // 追加作業: ryohi_ichiban_rows.type='追加作業'のレコード数（PHPの_make_tsuikaと同じ）
        let tsuika_count: i64 = conn.query_first(
            format!(
                "SELECT COUNT(*) FROM ryohi_ichiban_rows
                 WHERE driver_id = {}
                 AND type = '追加作業'
                 AND end_date >= '{}-{:02}-01'
                 AND end_date < '{}-{:02}-01'",
                driver.id, year, month,
                if month == 12 { year + 1 } else { year },
                if month == 12 { 1 } else { month + 1 }
            )
        )?.unwrap_or(0);
        summary.tsuika = tsuika_count as i32;

        let mut timecard = MonthlyTimecard {
            driver: driver.clone(),
            year,
            month,
            days,
            summary,
        };

        // 集計を計算（基礎日数なし - 後でcalculate_summary_with_kisoを呼ぶ）
        timecard.calculate_summary();

        Ok(timecard)
    }

    /// ドライバーの入社前日数と退職後日数を計算
    /// PHPのmakeTaishoku/makeMidJoinと同等
    fn get_hire_retire_counts(&self, driver_id: i32, year: i32, month: u32) -> Result<(i32, i32)> {
        let mut conn = self.pool.get_conn()?;

        // 月の初日と最終日
        let first_of_month = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let days_in_month = get_days_in_month(year, month) as i32;
        let last_of_month = NaiveDate::from_ymd_opt(year, month, days_in_month as u32).unwrap();

        // kyuyo_shainから入社日と退職日を取得
        // 有効なレコード（退職日がNULLまたは月初より後）を取得
        let hire_retire: Option<(Option<String>, Option<String>)> = conn.query_first(
            format!(
                "SELECT DATE_FORMAT(hire_date, '%Y-%m-%d'), DATE_FORMAT(retire_date, '%Y-%m-%d')
                 FROM kyuyo_shain
                 WHERE driver_id = {}
                 AND (retire_date IS NULL OR retire_date > '{}-{:02}-01')
                 ORDER BY hire_date DESC
                 LIMIT 1",
                driver_id, year, month
            )
        )?;

        let (before_hire, after_retire) = if let Some((hire_date_str, retire_date_str)) = hire_retire {
            let before_hire = if let Some(hire_str) = hire_date_str {
                if let Ok(hire_date) = NaiveDate::parse_from_str(&hire_str, "%Y-%m-%d") {
                    // 入社日が月初より後の場合、入社前日数を計算
                    if hire_date > first_of_month {
                        // 入社日の前日までの日数
                        let diff = (hire_date - first_of_month).num_days() as i32;
                        diff.min(days_in_month)
                    } else {
                        0
                    }
                } else {
                    0
                }
            } else {
                0
            };

            let after_retire = if let Some(retire_str) = retire_date_str {
                if let Ok(retire_date) = NaiveDate::parse_from_str(&retire_str, "%Y-%m-%d") {
                    // 退職日が月末より前の場合、退職後日数を計算
                    if retire_date <= last_of_month {
                        // 退職日の翌日から月末までの日数
                        let diff = (last_of_month - retire_date).num_days() as i32;
                        diff.max(0).min(days_in_month)
                    } else {
                        0
                    }
                } else {
                    0
                }
            } else {
                0
            };

            (before_hire, after_retire)
        } else {
            (0, 0)
        };

        Ok((before_hire, after_retire))
    }

    /// 指定ドライバーの月別タイムカードデータを取得（基礎日数付き）
    pub fn get_monthly_timecard_with_kiso(&self, driver: &Driver, year: i32, month: u32, kiso_date: i32) -> Result<MonthlyTimecard> {
        let mut timecard = self.get_monthly_timecard(driver, year, month)?;

        // 入社前日数・退職後日数を取得
        let (before_hire, after_retire) = self.get_hire_retire_counts(driver.id, year, month)?;

        // 基礎日数を使って再計算
        timecard.calculate_summary_with_kiso(kiso_date, before_hire, after_retire);
        Ok(timecard)
    }

    /// 全ドライバーの月別タイムカードを取得
    pub fn get_all_monthly_timecards(&self, year: i32, month: u32) -> Result<Vec<MonthlyTimecard>> {
        let drivers = self.get_active_drivers(year, month)?;
        let mut timecards = Vec::new();

        for driver in &drivers {
            let timecard = self.get_monthly_timecard(driver, year, month)?;
            timecards.push(timecard);
        }

        Ok(timecards)
    }

    /// 全ドライバーの月別タイムカードを取得（基礎日数付き）
    pub fn get_all_monthly_timecards_with_kiso(&self, year: i32, month: u32) -> Result<Vec<MonthlyTimecard>> {
        let drivers = self.get_active_drivers(year, month)?;
        let kiso_date = self.get_kiso_date(year, month)?;
        let mut timecards = Vec::new();

        for driver in &drivers {
            let timecard = self.get_monthly_timecard_with_kiso(driver, year, month, kiso_date)?;
            timecards.push(timecard);
        }

        Ok(timecards)
    }

    /// 打刻データから拘束時間を計算（PHPの_make_tc_to_tcと同等のロジック）
    /// 始業→終業、始業→運行開始、運行終了→終業、運行終了→運行開始、休息開始→終業の時間を計算
    fn calculate_kosoku_from_punches(&self, driver_id: i32, year: i32, month: u32, days_in_month: u8) -> Result<Vec<(u32, i32)>> {
        let mut conn = self.pool.get_conn()?;

        let start_date = format!("{}-{:02}-01", year, month);
        let end_date = if month == 12 {
            format!("{}-01-01", year + 1)
        } else {
            format!("{}-{:02}-01", year, month + 1)
        };

        // time_card_dstate から始業(30)・終業(31)を取得
        // PHPのTimeCardDtakoStateテーブルを参照してstate名を取得
        let tc_dstate: Vec<(String, String)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(tcd.datetime, '%Y-%m-%d %H:%i:%s') as dt, tcds.name as st
                 FROM time_card_dstate tcd
                 INNER JOIN time_card_dtako_state tcds ON tcds.id = tcd.state
                 WHERE tcd.id = {}
                 AND tcd.datetime >= '{}'
                 AND tcd.datetime < '{}'
                 ORDER BY tcd.datetime",
                driver_id, start_date, end_date
            ),
            |(datetime, state_name): (String, String)| (datetime, state_name)
        )?;

        // time_card_dtako から運行開始(10)・運行終了(11)・休息開始(20)・休息終了(21)を取得
        // TimeCardKosokuExpに登録されているレコードは除外（PHPのnotMatching("TimeCardKosokuExp")と同等）
        // time_card_kosoku_expは(datetime, driver_id, state)の複合主キー
        let tc_dtako: Vec<(String, String)> = conn.query_map(
            format!(
                "SELECT DATE_FORMAT(tcd.datetime, '%Y-%m-%d %H:%i:%s') as dt, tcds.name as st
                 FROM time_card_dtako tcd
                 INNER JOIN time_card_dtako_state tcds ON tcds.id = tcd.state
                 LEFT JOIN time_card_kosoku_exp tcke ON tcke.datetime = tcd.datetime
                     AND tcke.driver_id = tcd.driver_id
                     AND tcke.state = tcd.state
                 WHERE tcd.driver_id = {}
                 AND tcd.datetime >= '{}'
                 AND tcd.datetime < '{}'
                 AND tcke.datetime IS NULL
                 ORDER BY tcd.datetime",
                driver_id, start_date, end_date
            ),
            |(datetime, state_name): (String, String)| (datetime, state_name)
        )?;

        // 両方のデータをマージしてソート
        #[derive(Debug, Clone)]
        struct TimeEvent {
            datetime: NaiveDateTime,
            event_type: String, // "始業", "終業", "運行開始", "運行終了", "休息開始"
        }

        let mut events: Vec<TimeEvent> = Vec::new();

        for (dt_str, state_name) in tc_dstate {
            if let Ok(dt) = NaiveDateTime::parse_from_str(&dt_str, "%Y-%m-%d %H:%M:%S") {
                events.push(TimeEvent { datetime: dt, event_type: state_name });
            }
        }

        for (dt_str, state_name) in tc_dtako {
            if let Ok(dt) = NaiveDateTime::parse_from_str(&dt_str, "%Y-%m-%d %H:%M:%S") {
                events.push(TimeEvent { datetime: dt, event_type: state_name });
            }
        }

        // 日時順にソート
        events.sort_by(|a, b| a.datetime.cmp(&b.datetime));

        // 運行開始→始業がある日を特定（マイナス用）
        let mut minus_unko_day: std::collections::HashMap<u32, i32> = std::collections::HashMap::new();
        for i in 0..events.len() {
            let current = &events[i];
            if current.event_type == "運行開始" {
                if i + 1 < events.len() {
                    let next = &events[i + 1];
                    if next.event_type == "始業" && current.datetime.date() == next.datetime.date() {
                        // 運行開始→始業の時間をマイナス用に記録
                        let duration = next.datetime.signed_duration_since(current.datetime);
                        let minutes = duration.num_minutes().abs() as i32;
                        minus_unko_day.insert(current.datetime.day(), minutes);
                    }
                }
            }
        }

        // 日毎の拘束時間を計算
        let mut day_minutes: std::collections::HashMap<u32, i32> = std::collections::HashMap::new();

        for i in 0..events.len() {
            let current = &events[i];

            if i + 1 >= events.len() {
                continue;
            }
            let next = &events[i + 1];

            match (current.event_type.as_str(), next.event_type.as_str()) {
                // 始業→運行開始: 同時刻重複や運行開始→始業はスキップ
                ("始業", "運行開始") => {
                    // 同時刻なら重複スキップ
                    if current.datetime == next.datetime {
                        continue;
                    }
                    // 運行開始が始業より前ならスキップ
                    if next.datetime < current.datetime {
                        continue;
                    }
                    let duration = next.datetime.signed_duration_since(current.datetime);
                    let days_diff = (next.datetime.date() - current.datetime.date()).num_days();
                    let hours_diff = duration.num_hours();

                    // PHPと同じ条件: d < 2 && h < 14
                    if days_diff < 2 && hours_diff < 14 {
                        if current.datetime.date() == next.datetime.date() {
                            let minutes = duration.num_minutes() as i32;
                            *day_minutes.entry(next.datetime.day()).or_insert(0) += minutes;
                        }
                    }
                }

                // 始業→終業
                ("始業", "終業") => {
                    let duration = next.datetime.signed_duration_since(current.datetime);
                    let days_diff = (next.datetime.date() - current.datetime.date()).num_days();

                    // PHPと同じ条件: d < 1 (同じ日) または日跨ぎ (d == 1)
                    if days_diff <= 1 {
                        if current.datetime.date() == next.datetime.date() {
                            let minutes = duration.num_minutes() as i32;
                            *day_minutes.entry(next.datetime.day()).or_insert(0) += minutes;

                            // 昼休み(12:00-13:00)の控除
                            let noon_start = current.datetime.date().and_hms_opt(12, 0, 0).unwrap();
                            let noon_end = current.datetime.date().and_hms_opt(13, 0, 0).unwrap();

                            if current.datetime < noon_start {
                                if next.datetime > noon_end {
                                    // 昼休みを完全に含む場合、60分控除
                                    *day_minutes.entry(next.datetime.day()).or_insert(0) -= 60;
                                } else if next.datetime > noon_start {
                                    // 終業が12時〜13時の間: 12時から終業までを控除
                                    let overlap = next.datetime.signed_duration_since(noon_start).num_minutes() as i32;
                                    *day_minutes.entry(next.datetime.day()).or_insert(0) -= overlap;
                                }
                                // 終業が12時より前の場合は控除なし
                            }
                        } else {
                            // 日付を跨ぐ場合
                            let midnight = current.datetime.date().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap();
                            let before_midnight = midnight.signed_duration_since(current.datetime).num_minutes() as i32;
                            let next_midnight = next.datetime.date().and_hms_opt(0, 0, 0).unwrap();
                            let after_midnight = next.datetime.signed_duration_since(next_midnight).num_minutes() as i32;

                            if before_midnight > 0 {
                                *day_minutes.entry(current.datetime.day()).or_insert(0) += before_midnight;
                            }
                            if after_midnight > 0 && next.datetime.day() <= days_in_month as u32 {
                                *day_minutes.entry(next.datetime.day()).or_insert(0) += after_midnight;
                            }
                        }
                    }
                }

                // 運行終了→終業
                ("運行終了", "終業") => {
                    let duration = next.datetime.signed_duration_since(current.datetime);
                    let days_diff = (next.datetime.date() - current.datetime.date()).num_days();
                    let hours_diff = duration.num_hours();

                    // PHPと同じ条件: d < 2 && h < 14
                    if days_diff < 2 && hours_diff < 14 {
                        if current.datetime.date() == next.datetime.date() {
                            let minutes = duration.num_minutes() as i32;
                            *day_minutes.entry(next.datetime.day()).or_insert(0) += minutes;
                        }
                    }
                }

                // 運行終了→運行開始
                ("運行終了", "運行開始") => {
                    let duration = next.datetime.signed_duration_since(current.datetime);
                    // PHPのdate_diff->dは経過時間から計算した日数（24時間単位）
                    let total_hours = duration.num_hours();
                    let days_in_duration = total_hours / 24;
                    let hours_remainder = total_hours % 24;

                    // PHPと同じ条件: d < 1 && h < 12
                    // d は経過時間ベースの日数、h は残り時間
                    if days_in_duration < 1 && hours_remainder < 12 {
                        let minutes = duration.num_minutes() as i32;
                        // 日を跨いでいても、next（運行開始）の日に加算
                        *day_minutes.entry(next.datetime.day()).or_insert(0) += minutes;
                    }
                }

                // 休息開始→終業
                ("休息開始", "終業") => {
                    let duration = next.datetime.signed_duration_since(current.datetime);
                    let days_diff = (next.datetime.date() - current.datetime.date()).num_days();
                    let hours_diff = duration.num_hours();

                    // PHPと同じ条件: d < 2 && h < 14
                    if days_diff < 2 && hours_diff < 14 {
                        if current.datetime.date() == next.datetime.date() {
                            let minutes = duration.num_minutes() as i32;
                            *day_minutes.entry(next.datetime.day()).or_insert(0) += minutes;
                        }
                    }
                }

                // 運行開始→運行終了
                // 注意: PHPの_make_tc_to_tc()ではこのパターンは計算しない
                // 運行開始→運行終了は_make_kosoku_time()でデジタコ版として計算される
                // TC_DCとの一致を優先し、ここでは何もしない
                ("運行開始", "運行終了") => {
                    // PHPと同様、TC_DCでは運行開始→運行終了を計算しない
                }

                _ => {}
            }
        }

        // マイナス処理を適用（運行開始→始業がある日）
        for (day, minus_minutes) in minus_unko_day {
            if let Some(total) = day_minutes.get_mut(&day) {
                *total -= minus_minutes;
            }
        }

        Ok(day_minutes.into_iter().collect())
    }

    /// デジタコ版拘束時間を計算（PHPの_make_kosoku_time()と同等のロジック）
    /// DtakoRows/DtakoEventsテーブルから計算
    pub fn calculate_kosoku_digitacho(&self, driver_id: i32, year: i32, month: u32) -> Result<std::collections::HashMap<u32, i32>> {
        let mut conn = self.pool.get_conn()?;

        let start_date = format!("{}-{:02}-01", year, month);
        let end_date = if month == 12 {
            format!("{}-01-01", year + 1)
        } else {
            format!("{}-{:02}-01", year, month + 1)
        };

        // 日ごとの拘束時間
        let mut day_minutes: std::collections::HashMap<u32, i32> = std::collections::HashMap::new();

        // dtako_rowsから当月の運行データを取得（出庫or帰庫が月内）
        // dtako_events.運行NO = dtako_rows.運行NO + 対象乗務員区分
        let unko_list: Vec<(String, i32)> = conn.query_map(
            format!(
                "SELECT 運行NO, 対象乗務員区分 FROM dtako_rows
                 WHERE 対象乗務員CD = {}
                 AND (
                     (帰庫日時 >= '{}' AND 帰庫日時 < '{}')
                     OR (出庫日時 >= '{}' AND 出庫日時 < '{}')
                 )
                 ORDER BY 出庫日時",
                driver_id, start_date, end_date, start_date, end_date
            ),
            |(unko_no, kubun): (String, i32)| (unko_no, kubun)
        )?;

        for (unko_no, kubun) in &unko_list {
            let event_unko_no = format!("{}{}", unko_no, kubun);

            // dtako_eventsから対象イベントを取得
            // イベント名: 積み、降し、休憩、運転、その他、待機
            let mut events: Vec<(NaiveDateTime, NaiveDateTime, i32)> = conn.query_map(
                format!(
                    "SELECT DATE_FORMAT(開始日時, '%Y-%m-%d %H:%i:%s'),
                            DATE_FORMAT(終了日時, '%Y-%m-%d %H:%i:%s'),
                            区間時間
                     FROM dtako_events
                     WHERE 運行NO = '{}'
                     AND 対象乗務員CD = {}
                     AND イベント名 IN ('積み', '降し', '休憩', '運転', 'その他', '待機')
                     ORDER BY 開始日時",
                    event_unko_no, driver_id
                ),
                |(start_str, end_str, interval): (String, String, i32)| {
                    let start = NaiveDateTime::parse_from_str(&start_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    let end = NaiveDateTime::parse_from_str(&end_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    (start, end, interval)
                }
            )?;

            // time_card_kosoku_expでマッチする休息を追加（除外した休息を拘束に戻す）
            let exp_kyusoku: Vec<(NaiveDateTime, NaiveDateTime, i32)> = conn.query_map(
                format!(
                    "SELECT DATE_FORMAT(de.開始日時, '%Y-%m-%d %H:%i:%s'),
                            DATE_FORMAT(de.終了日時, '%Y-%m-%d %H:%i:%s'),
                            de.区間時間
                     FROM dtako_events de
                     INNER JOIN time_card_kosoku_exp tcke ON tcke.datetime = de.開始日時
                         AND tcke.driver_id = de.対象乗務員CD
                     WHERE de.運行NO = '{}'
                     AND de.対象乗務員CD = {}
                     AND de.イベント名 = '休息'
                     ORDER BY de.開始日時",
                    event_unko_no, driver_id
                ),
                |(start_str, end_str, interval): (String, String, i32)| {
                    let start = NaiveDateTime::parse_from_str(&start_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    let end = NaiveDateTime::parse_from_str(&end_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    (start, end, interval)
                }
            )?;
            events.extend(exp_kyusoku);

            // time_card_dtakoのchng_state=99の除外期間を取得
            let exp_events: Vec<(NaiveDateTime, String, Option<i32>)> = conn.query_map(
                format!(
                    "SELECT DATE_FORMAT(datetime, '%Y-%m-%d %H:%i:%s'), event_name, state
                     FROM time_card_dtako
                     WHERE unko_no = '{}'
                     AND driver_id = {}
                     AND chng_state = 99
                     ORDER BY datetime",
                    event_unko_no, driver_id
                ),
                |(dt_str, event_name, state): (String, String, Option<i32>)| {
                    let dt = NaiveDateTime::parse_from_str(&dt_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    (dt, event_name, state)
                }
            )?;

            // 除外期間を特定（運行開始/休息終了 → 運行終了/休息開始）
            let mut exclude_ranges: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
            let mut i = 0;
            while i < exp_events.len() {
                let (dt1, event1, state1) = &exp_events[i];
                // 運行開始 or 休息終了(state=21)
                let is_start = event1 == "運行開始" || (event1 == "休息" && *state1 == Some(21));
                if is_start && i + 1 < exp_events.len() {
                    let (dt2, event2, state2) = &exp_events[i + 1];
                    // 運行終了 or 休息開始(state=20)
                    let is_end = event2 == "運行終了" || (event2 == "休息" && *state2 == Some(20));
                    if is_end {
                        exclude_ranges.push((*dt1, *dt2));
                        i += 2;
                        continue;
                    }
                }
                i += 1;
            }

            // 除外期間のイベントをフィルタ
            events.retain(|(start, _, _)| {
                !exclude_ranges.iter().any(|(ex_start, ex_end)| start >= ex_start && start <= ex_end)
            });

            // イベントを日時順にソート
            events.sort_by(|a, b| a.0.cmp(&b.0));

            // 日ごとに集計
            let start_date_parsed = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
            let end_date_parsed = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
            };

            for (start, end, interval) in &events {
                if start.date() == end.date() {
                    // 日付が同じ場合
                    if start.date() >= start_date_parsed && end.date() < end_date_parsed {
                        *day_minutes.entry(start.day()).or_insert(0) += interval;
                    }
                } else {
                    // 日付を跨いだ場合
                    if start.date() >= start_date_parsed && start.date() < end_date_parsed {
                        // 開始日の0時から翌日0時までの時間
                        let tomorrow = start.date().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap();
                        let before_midnight = tomorrow.signed_duration_since(*start).num_minutes() as i32;
                        *day_minutes.entry(start.day()).or_insert(0) += before_midnight;
                    }
                    if end.date() >= start_date_parsed && end.date() < end_date_parsed {
                        // 終了日の0時から終了時刻までの時間
                        let midnight = end.date().and_hms_opt(0, 0, 0).unwrap();
                        let after_midnight = end.signed_duration_since(midnight).num_minutes() as i32;
                        *day_minutes.entry(end.day()).or_insert(0) += after_midnight;
                    }
                }
            }

            // フェリー時間を控除（4時間未満の場合）
            let ferries: Vec<(NaiveDateTime, NaiveDateTime)> = conn.query_map(
                format!(
                    "SELECT DATE_FORMAT(開始日時, '%Y-%m-%d %H:%i:%s'),
                            DATE_FORMAT(終了日時, '%Y-%m-%d %H:%i:%s')
                     FROM dtako_ferry_rows
                     WHERE 運行NO = '{}'",
                    event_unko_no
                ),
                |(start_str, end_str): (String, String)| {
                    let start = NaiveDateTime::parse_from_str(&start_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    let end = NaiveDateTime::parse_from_str(&end_str, "%Y-%m-%d %H:%M:%S").unwrap();
                    (start, end)
                }
            )?;

            for (ferry_start, ferry_end) in ferries {
                let duration = ferry_end.signed_duration_since(ferry_start);
                let hours = duration.num_hours();

                if ferry_start.date() == ferry_end.date() {
                    // 同日フェリー
                    if ferry_start.date() >= start_date_parsed && ferry_start.date() < end_date_parsed {
                        if hours < 4 {
                            let minutes = duration.num_minutes() as i32;
                            *day_minutes.entry(ferry_start.day()).or_insert(0) -= minutes;
                        }
                    }
                } else {
                    // 日跨ぎフェリー
                    let days_in_duration = duration.num_days();
                    if hours < 4 && days_in_duration == 0 {
                        // 開始日分
                        if ferry_start.date() >= start_date_parsed && ferry_start.date() < end_date_parsed {
                            let tomorrow = ferry_start.date().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap();
                            let before_midnight = tomorrow.signed_duration_since(ferry_start).num_minutes() as i32;
                            if before_midnight / 60 < 4 {
                                *day_minutes.entry(ferry_start.day()).or_insert(0) -= before_midnight;
                            }
                        }
                        // 終了日分
                        if ferry_end.date() >= start_date_parsed && ferry_end.date() < end_date_parsed {
                            let midnight = ferry_end.date().and_hms_opt(0, 0, 0).unwrap();
                            let after_midnight = ferry_end.signed_duration_since(midnight).num_minutes() as i32;
                            *day_minutes.entry(ferry_end.day()).or_insert(0) -= after_midnight;
                        }
                    }
                }
            }
        }

        Ok(day_minutes)
    }

    /// デジタコ版拘束時間をDocker DBにINSERT
    pub fn insert_digitacho_kosoku_to_docker(&self, driver_id: i32, year: i32, month: u32) -> Result<usize> {
        let kosoku_data = self.calculate_kosoku_digitacho(driver_id, year, month)?;

        let docker_config = DbConfig::docker();
        let docker_pool = Pool::new(Opts::from_url(&docker_config.connection_url())?)?;
        let mut conn = docker_pool.get_conn()?;

        let mut inserted = 0;

        for (day, minutes) in kosoku_data {
            let date = format!("{}-{:02}-{:02}", year, month, day);

            conn.exec_drop(
                r"INSERT INTO time_card_kosoku (driver_id, date, minutes, type)
                  VALUES (?, ?, ?, 'デジタコRust')
                  ON DUPLICATE KEY UPDATE minutes = VALUES(minutes)",
                (driver_id, &date, minutes)
            )?;
            inserted += 1;
        }

        Ok(inserted)
    }

    /// time_card_allowanceテーブルにINSERT（Docker DB）（PHPの_insertTimeCardAllowance相当）
    /// PDF生成時に集計データを保存し、他システム（一覧表示等）が参照する
    pub fn insert_time_card_allowance_to_docker(
        &self,
        datetime: NaiveDate,      // 月初日
        driver_id: i32,
        shukkin_count: f64,
        dayoff_count: f64,        // 公休日数
        paidoff_count: f64,       // 有休日数
        absence_count: f64,       // 欠勤日数
        overtime_count: f64,      // 残業合計
        holidaywork_count: f64,   // 休出日数
        additionalwork_payment: i32, // 追加作業金額
        kachiku_payment: i32,     // 家畜手当日数
        trail_payment: i32,       // トレーラー手当日数
        chikoku_count: i32,       // 遅刻日数
        soutai_count: i32,        // 早退日数
        tokukyu_count: i32,       // 特休日数
    ) -> Result<()> {
        // Docker DBに接続
        let docker_config = DbConfig::docker();
        let docker_pool = Pool::new(Opts::from_url(&docker_config.connection_url())?)?;
        let mut conn = docker_pool.get_conn()?;

        let date_str = datetime.format("%Y-%m-%d").to_string();

        // params!マクロで名前付きパラメータを使用
        conn.exec_drop(
            r"INSERT INTO time_card_allowance
              (datetime, driver_id, shukkin_count, dayoff_count, paidoff_count, absence_count,
               overtime_count, holidaywork_count, additionalwork_payment, kachiku_payment,
               trail_payment, chikoku_count, soutai_count, tokukyu_count)
              VALUES (:datetime, :driver_id, :shukkin_count, :dayoff_count, :paidoff_count, :absence_count,
               :overtime_count, :holidaywork_count, :additionalwork_payment, :kachiku_payment,
               :trail_payment, :chikoku_count, :soutai_count, :tokukyu_count)
              ON DUPLICATE KEY UPDATE
                shukkin_count = VALUES(shukkin_count),
                dayoff_count = VALUES(dayoff_count),
                paidoff_count = VALUES(paidoff_count),
                absence_count = VALUES(absence_count),
                overtime_count = VALUES(overtime_count),
                holidaywork_count = VALUES(holidaywork_count),
                additionalwork_payment = VALUES(additionalwork_payment),
                kachiku_payment = VALUES(kachiku_payment),
                trail_payment = VALUES(trail_payment),
                chikoku_count = VALUES(chikoku_count),
                soutai_count = VALUES(soutai_count),
                tokukyu_count = VALUES(tokukyu_count)",
            params! {
                "datetime" => &date_str,
                "driver_id" => driver_id,
                "shukkin_count" => shukkin_count,
                "dayoff_count" => dayoff_count,
                "paidoff_count" => paidoff_count,
                "absence_count" => absence_count,
                "overtime_count" => overtime_count,
                "holidaywork_count" => holidaywork_count,
                "additionalwork_payment" => additionalwork_payment,
                "kachiku_payment" => kachiku_payment,
                "trail_payment" => trail_payment,
                "chikoku_count" => chikoku_count,
                "soutai_count" => soutai_count,
                "tokukyu_count" => tokukyu_count,
            }
        )?;

        Ok(())
    }

    /// MonthlyTimecardからtime_card_allowanceにINSERT（Docker DB）
    pub fn insert_timecard_allowance_to_docker(&self, timecard: &MonthlyTimecard) -> Result<()> {
        let first_of_month = NaiveDate::from_ymd_opt(timecard.year, timecard.month, 1).unwrap();

        self.insert_time_card_allowance_to_docker(
            first_of_month,
            timecard.driver.id,
            timecard.summary.shukkin,       // 既にf64
            timecard.summary.kyuka as f64,
            timecard.summary.yukyu,         // 既にf64
            timecard.summary.kekkin as f64,
            timecard.summary.total_zangyo,
            timecard.summary.kyushutsu,     // 既にf64
            timecard.summary.tsuika,
            timecard.summary.kachiku,
            timecard.summary.trailer,
            timecard.summary.chikoku,
            timecard.summary.soutai,
            timecard.summary.tokukyu,
        )
    }

    /// Docker DBから該当月のallowanceをハッシュマップで取得
    fn fetch_existing_allowances_from_docker(&self, year: i32, month: u32) -> Result<HashMap<i32, u64>> {
        let docker_config = DbConfig::docker();
        let docker_pool = Pool::new(Opts::from_url(&docker_config.connection_url())?)?;
        let mut conn = docker_pool.get_conn()?;

        let first_of_month = format!("{}-{:02}-01", year, month);

        // MySQLのFromRowはタプル12個まで。query_mapで個別に取得
        let mut result = HashMap::new();
        conn.exec_map(
            r"SELECT driver_id, shukkin_count, dayoff_count, paidoff_count, absence_count,
                     overtime_count, holidaywork_count, additionalwork_payment, kachiku_payment,
                     trail_payment, chikoku_count, soutai_count, tokukyu_count
              FROM time_card_allowance
              WHERE datetime = ?",
            (&first_of_month,),
            |row: mysql::Row| {
                let driver_id: i32 = row.get(0).unwrap();
                let data = AllowanceData {
                    driver_id,
                    shukkin_count: (row.get::<f64, _>(1).unwrap_or(0.0) * 10.0) as i64,
                    dayoff_count: (row.get::<f64, _>(2).unwrap_or(0.0) * 10.0) as i64,
                    paidoff_count: (row.get::<f64, _>(3).unwrap_or(0.0) * 10.0) as i64,
                    absence_count: (row.get::<f64, _>(4).unwrap_or(0.0) * 10.0) as i64,
                    overtime_count: (row.get::<f64, _>(5).unwrap_or(0.0) * 10.0) as i64,
                    holidaywork_count: (row.get::<f64, _>(6).unwrap_or(0.0) * 10.0) as i64,
                    additionalwork_payment: row.get(7).unwrap_or(0),
                    kachiku_payment: row.get(8).unwrap_or(0),
                    trail_payment: row.get(9).unwrap_or(0),
                    chikoku_count: row.get(10).unwrap_or(0),
                    soutai_count: row.get(11).unwrap_or(0),
                    tokukyu_count: row.get(12).unwrap_or(0),
                };
                (driver_id, data.compute_hash())
            }
        )?.into_iter().for_each(|(id, hash)| { result.insert(id, hash); });

        Ok(result)
    }

    /// 指定タイムカードのallowanceを差分更新（Docker DB）
    /// 削除は行わない（新データに含まれるドライバーのみ追加/更新）
    /// 戻り値: (inserted, updated, unchanged)
    pub fn sync_all_timecard_allowances_to_docker(&self, timecards: &[MonthlyTimecard]) -> Result<(usize, usize, usize)> {
        if timecards.is_empty() {
            return Ok((0, 0, 0));
        }

        let year = timecards[0].year;
        let month = timecards[0].month;

        // 既存データをハッシュマップで取得
        let existing = self.fetch_existing_allowances_from_docker(year, month)?;

        // 新データのdriver_idセットとハッシュマップを作成
        let mut new_data: HashMap<i32, AllowanceData> = HashMap::new();
        for tc in timecards {
            new_data.insert(tc.driver.id, AllowanceData::from_timecard(tc));
        }

        let mut inserted = 0;
        let mut updated = 0;
        let mut unchanged = 0;

        // 追加/更新（新データに含まれるドライバーのみ処理）
        for (driver_id, new_allowance) in &new_data {
            let new_hash = new_allowance.compute_hash();

            match existing.get(driver_id) {
                Some(old_hash) if *old_hash == new_hash => {
                    // 変更なし
                    unchanged += 1;
                }
                Some(_) => {
                    // 変更あり: UPDATE
                    let tc = timecards.iter().find(|t| t.driver.id == *driver_id).unwrap();
                    self.insert_timecard_allowance_to_docker(tc)?;
                    updated += 1;
                }
                None => {
                    // 新規: INSERT
                    let tc = timecards.iter().find(|t| t.driver.id == *driver_id).unwrap();
                    self.insert_timecard_allowance_to_docker(tc)?;
                    inserted += 1;
                }
            }
        }

        Ok((inserted, updated, unchanged))
    }

    /// 全タイムカードのallowanceをINSERT（Docker DB）- 後方互換用
    pub fn insert_all_timecard_allowances_to_docker(&self, timecards: &[MonthlyTimecard]) -> Result<usize> {
        let (inserted, updated, _unchanged) = self.sync_all_timecard_allowances_to_docker(timecards)?;
        Ok(inserted + updated)
    }

    /// タイムカードの拘束時間をDocker DBにINSERT
    pub fn insert_kosoku_to_docker(&self, timecards: &[MonthlyTimecard]) -> Result<usize> {
        let docker_config = DbConfig::docker();
        let docker_pool = Pool::new(Opts::from_url(&docker_config.connection_url())?)?;
        let mut conn = docker_pool.get_conn()?;

        let mut inserted = 0;

        for tc in timecards {
            for day in &tc.days {
                if let Some(minutes) = day.kosoku_minutes {
                    let date = format!("{}-{:02}-{:02}", tc.year, tc.month, day.day);

                    // INSERT（重複時はUPDATE）
                    conn.exec_drop(
                        r"INSERT INTO time_card_kosoku (driver_id, date, minutes, type)
                          VALUES (?, ?, ?, 'Rust計算')
                          ON DUPLICATE KEY UPDATE minutes = VALUES(minutes)",
                        (tc.driver.id, &date, minutes)
                    )?;
                    inserted += 1;
                }
            }
        }

        Ok(inserted)
    }
}

/// 曜日を日本語に変換
fn weekday_to_japanese(weekday: Weekday) -> String {
    match weekday {
        Weekday::Mon => "月",
        Weekday::Tue => "火",
        Weekday::Wed => "水",
        Weekday::Thu => "木",
        Weekday::Fri => "金",
        Weekday::Sat => "土",
        Weekday::Sun => "日",
    }.to_string()
}

/// 月の日数を取得
fn get_days_in_month(year: i32, month: u32) -> u8 {
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    };

    next_month
        .unwrap()
        .pred_opt()
        .unwrap()
        .day() as u8
}

/// 月末日を取得
fn get_end_of_month(year: i32, month: u32) -> NaiveDate {
    let days = get_days_in_month(year, month);
    NaiveDate::from_ymd_opt(year, month, days as u32).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_in_month() {
        assert_eq!(get_days_in_month(2024, 1), 31);
        assert_eq!(get_days_in_month(2024, 2), 29); // うるう年
        assert_eq!(get_days_in_month(2025, 2), 28);
        assert_eq!(get_days_in_month(2024, 4), 30);
        assert_eq!(get_days_in_month(2024, 12), 31);
    }

    #[test]
    fn test_weekday_japanese() {
        assert_eq!(weekday_to_japanese(Weekday::Sun), "日");
        assert_eq!(weekday_to_japanese(Weekday::Mon), "月");
        assert_eq!(weekday_to_japanese(Weekday::Sat), "土");
    }
}
