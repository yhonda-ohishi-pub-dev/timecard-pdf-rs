/// ドライバー（従業員）情報
#[derive(Debug, Clone)]
pub struct Driver {
    pub id: i32,
    pub name: String,
    // ソート用フィールド（給与番号順）
    pub bumon: Option<i32>,       // 部門
    pub category_c: Option<i32>,  // 給与区分
    pub eigyosho_c: Option<i32>,  // 営業所コード
    pub kyuyo_shain_id: Option<i32>, // 給与社員ID
}

/// 1日分の勤怠記録
#[derive(Debug, Clone)]
pub struct DayRecord {
    pub day: u8,                    // 日（1-31）
    pub weekday: String,            // 曜日（日,月,火,水,木,金,土）
    pub clock_in: Vec<String>,      // 出勤時刻（最大2回）
    pub clock_out: Vec<String>,     // 退勤時刻（最大2回）
    pub remarks: String,            // 備考（公休、有休等）
    pub is_sunday: bool,            // 日曜日フラグ
    pub kosoku_minutes: Option<i32>, // 拘束時間（分）- 表示用（TC_DC + デジタコ合算）
    pub kosoku_tcdc: Option<i32>,   // TC_DC版拘束時間（分）- INSERT用
    pub kosoku_digitacho: Option<i32>, // デジタコ版拘束時間（分）- INSERT用
    pub zangyo: Option<f64>,        // 残業時間（旅費から取得）
    pub is_kachiku: bool,           // 家畜車フラグ（「畜」マーク）
    pub is_trailer: bool,           // トレーラーフラグ（「引」マーク）
    pub has_digitacho: bool,        // デジタコデータありフラグ（リンク表示用）
    pub has_daily_report: bool,     // 作業日報フラグ（「作」マーク）
    pub tsuika_count: i32,          // 追加作業件数
}

impl DayRecord {
    pub fn new(day: u8, weekday: &str) -> Self {
        let is_sunday = weekday == "日";
        Self {
            day,
            weekday: weekday.to_string(),
            clock_in: Vec::new(),
            clock_out: Vec::new(),
            remarks: String::new(),
            is_sunday,
            kosoku_minutes: None,
            kosoku_tcdc: None,
            kosoku_digitacho: None,
            zangyo: None,
            is_kachiku: false,
            is_trailer: false,
            has_digitacho: false,
            has_daily_report: false,
            tsuika_count: 0,
        }
    }

    /// 拘束時間を "HH:MM" 形式で取得
    pub fn kosoku_str(&self) -> String {
        match self.kosoku_minutes {
            Some(minutes) if minutes > 0 => {
                let hours = minutes / 60;
                let mins = minutes % 60;
                format!("{:02}:{:02}", hours, mins)
            }
            _ => String::new(),
        }
    }

    /// 残業時間を文字列で取得（整数の場合は整数表示）
    pub fn zangyo_str(&self) -> String {
        match self.zangyo {
            Some(z) if z != 0.0 => {
                if z.fract() == 0.0 {
                    format!("{}", z as i32)
                } else {
                    format!("{:.1}", z)
                }
            }
            _ => String::new(),
        }
    }

    /// 追加作業を表示用文字列に変換
    /// 1件=「〇」、2件=「〇〇」、3件以上=「〇n」
    pub fn tsuika_str(&self) -> String {
        match self.tsuika_count {
            0 => String::new(),
            1 => "〇".to_string(),
            2 => "〇〇".to_string(),
            n => format!("〇{}", n),
        }
    }

    /// 残業+追加作業の連結表示（PHPのdispZangyo_st相当）
    pub fn zangyo_with_tsuika_str(&self) -> String {
        let zangyo = self.zangyo_str();
        let tsuika = self.tsuika_str();
        if zangyo.is_empty() {
            tsuika
        } else {
            format!("{}{}", zangyo, tsuika)
        }
    }
}

/// 月別タイムカードデータ
#[derive(Debug, Clone)]
pub struct MonthlyTimecard {
    pub driver: Driver,
    pub year: i32,
    pub month: u32,
    pub days: Vec<DayRecord>,
    pub summary: TimecardSummary,
}

/// 集計データ
#[derive(Debug, Clone, Default)]
pub struct TimecardSummary {
    pub shukkin: f64,      // 出勤日数（半休対応のためf64）
    pub kyuka: i32,        // 公休日数
    pub yukyu: f64,        // 有休日数（半休対応のためf64）
    pub kekkin: i32,       // 欠勤日数
    pub chikoku: i32,      // 遅刻日数
    pub soutai: i32,       // 早退日数
    pub tokukyu: i32,      // 特休日数
    pub total_zangyo: f64, // 残業合計
    pub kyushutsu: f64,    // 休出日数（半休対応のためf64）
    pub total_kosoku: i32, // 拘束時間合計（分）
    pub trailer: i32,      // トレーラー手当日数
    pub kachiku: i32,      // 家畜車手当日数
    pub tsuika: i32,       // 追加作業
}

impl TimecardSummary {
    /// 拘束時間合計を "HHH:MM" 形式で取得
    pub fn total_kosoku_str(&self) -> String {
        if self.total_kosoku > 0 {
            let hours = self.total_kosoku / 60;
            let mins = self.total_kosoku % 60;
            format!("{:02}:{:02}", hours, mins)
        } else {
            String::new()
        }
    }
}

impl MonthlyTimecard {
    pub fn year_month_str(&self) -> String {
        format!("{}年{:02}月", self.year, self.month)
    }

    /// 日別データから集計を計算（基礎日数なしの基本集計）
    /// 休出計算は別途calculate_summary_with_kiso()を使用
    pub fn calculate_summary(&mut self) {
        self.calculate_summary_with_kiso(0, 0, 0);
    }

    /// 日別データから集計を計算（基礎日数ベースで休出計算）
    /// PHPのShukkinboRowクラスと同じロジック
    ///
    /// # Arguments
    /// * `kiso_date` - 基礎日数（kyuyo_kiso_dateテーブルから）
    /// * `before_hire_count` - 入社前日数
    /// * `after_retire_count` - 退職後日数
    pub fn calculate_summary_with_kiso(&mut self, kiso_date: i32, before_hire_count: i32, after_retire_count: i32) {
        // 既存の手当データを保持
        let existing_kachiku = self.summary.kachiku;
        let existing_trailer = self.summary.trailer;
        let existing_tsuika = self.summary.tsuika;

        let mut summary = TimecardSummary::default();

        // 日別データから各種カウントを集計（ShukkinboRowのmakeDisplayData相当）
        for day in &self.days {
            // 拘束時間合計
            if let Some(minutes) = day.kosoku_minutes {
                summary.total_kosoku += minutes;
            }

            // 残業合計
            if let Some(zangyo) = day.zangyo {
                summary.total_zangyo += zangyo;
            }

            // 備考から休暇種別をカウント（PHPの_makeTimeCardDisplayArray switch文と同じ）
            // TimeCardController.php:2922-2954
            match day.remarks.as_str() {
                "公休" | "泊休" | "積置泊休" | "指休" => summary.kyuka += 1,
                "有休" => summary.yukyu += 1.0,
                "欠勤" => summary.kekkin += 1,
                "遅刻" => summary.chikoku += 1,
                "早退" => summary.soutai += 1,
                "特休" => summary.tokukyu += 1,
                "前休" | "後休" | "前休作" | "後休作" => {
                    // 半休は0.5日（PHPでは前休作/後休作も0.5）
                    summary.yukyu += 0.5;
                }
                _ => {}
            }

            // 日別の手当フラグからカウント
            if day.is_kachiku {
                summary.kachiku += 1;
            }
            if day.is_trailer {
                summary.trailer += 1;
            }
        }

        // 既存の手当データがあれば使用（日別データから再計算されなかった場合のフォールバック）
        if summary.kachiku == 0 && existing_kachiku > 0 {
            summary.kachiku = existing_kachiku;
        }
        if summary.trailer == 0 && existing_trailer > 0 {
            summary.trailer = existing_trailer;
        }
        // 追加作業は既存値を保持（日別データにはない）
        summary.tsuika = existing_tsuika;

        // PHPの計算式に従って出勤日数と休出日数を計算
        // kyujitsu_shukkin = 月の日数 - 公休 - 基礎日数 - 欠勤 - 入社前 - 退職後
        // shukkin = 月の日数 - 公休 - 有休 - 休出 - 欠勤 - 特休 - 入社前 - 退職後
        let days_in_month = self.days.len() as f64;

        // 休出日数計算
        let kyushutsu = days_in_month
            - summary.kyuka as f64
            - kiso_date as f64
            - summary.kekkin as f64
            - before_hire_count as f64
            - after_retire_count as f64;
        summary.kyushutsu = if kyushutsu > 0.0 { kyushutsu } else { 0.0 };

        // 出勤日数計算
        let shukkin = days_in_month
            - summary.kyuka as f64
            - summary.yukyu
            - summary.kyushutsu
            - summary.kekkin as f64
            - summary.tokukyu as f64
            - before_hire_count as f64
            - after_retire_count as f64;
        summary.shukkin = if shukkin > 0.0 { shukkin } else { 0.0 };

        self.summary = summary;
    }
}
