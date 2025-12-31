mod tcpdf_compat;
mod coordinate_data;
mod db;
mod timecard_data;
mod server;

use std::fs;
use std::env;
use coordinate_data::CoordinateData;
use tcpdf_compat::TcpdfCompat;
use db::{DbConfig, TimecardDb};

#[tokio::main]
async fn main() {
    // .envファイルから環境変数を読み込み
    dotenvy::dotenv().ok();
    let args: Vec<String> = env::args().collect();

    // コマンドライン引数でモードを切り替え
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("");

    match mode {
        "server" => {
            // HTTPサーバーモード
            let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8080);
            server::run(port).await;
        }
        "db" => {
            // DBモード: タイムカードデータを取得して表示
            run_db_mode(&args);
        }
        "pdf" => {
            // PDFモード: DBからタイムカードを取得してPDF生成（3人/ページ）
            run_pdf_mode(&args);
        }
        "pdf-shukei" => {
            // PDF集計モード: DBからタイムカードを取得してPDF生成（1人/ページ、日付横並び）
            run_pdf_shukei_mode(&args);
        }
        "verify" => {
            // 検証モード: 本番DBから計算してDocker DBにINSERT（TC_DC版）
            run_verify_mode(&args);
        }
        "verify-dtako" => {
            // 検証モード: デジタコ版計算 → Docker DBにINSERT
            run_verify_digitacho_mode(&args);
        }
        _ => {
            // JSONモード: 座標JSONからPDF生成（従来の動作）
            run_json_mode();
        }
    }
}

/// DBモード: 本番DBからタイムカードデータを取得
fn run_db_mode(args: &[String]) {
    // 年月を引数から取得（デフォルト: 2025年12月）
    let year: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2025);
    let month: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);
    // 特定のドライバーIDを指定可能
    let target_driver_id: Option<i32> = args.get(4).and_then(|s| s.parse().ok());

    println!("=== タイムカードデータ取得 ===");
    println!("対象: {}年{}月", year, month);
    if let Some(id) = target_driver_id {
        println!("ドライバーID: {}", id);
    }
    println!();

    // 本番DBに接続
    let config = DbConfig::production();
    println!("接続先: {}:{}", config.host, config.port);

    let db = match TimecardDb::connect(&config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("DB接続エラー: {}", e);
            return;
        }
    };
    println!("接続成功！");
    println!();

    // ドライバー一覧を取得
    let drivers = match db.get_active_drivers(year, month) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("ドライバー取得エラー: {}", e);
            return;
        }
    };

    println!("アクティブドライバー数: {}", drivers.len());
    println!();

    // 特定のドライバーIDが指定されていればそのドライバーを、なければ最初の3人を表示
    let target_drivers: Vec<&timecard_data::Driver> = if let Some(id) = target_driver_id {
        drivers.iter().filter(|d| d.id == id).collect()
    } else {
        drivers.iter().take(3).collect()
    };

    for driver in target_drivers {
        let timecard = match db.get_monthly_timecard(driver, year, month) {
            Ok(tc) => tc,
            Err(e) => {
                eprintln!("タイムカード取得エラー ({}): {}", driver.name, e);
                continue;
            }
        };

        println!("=== {} ({}) ===", timecard.driver.name, timecard.year_month_str());
        println!("{:>2} {:>2} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {}",
            "日", "曜", "出勤1", "退社1", "出勤2", "退社2", "残業", "拘束", "備考");
        println!("{}", "-".repeat(70));

        for day in &timecard.days {
            let in1 = day.clock_in.get(0).map(|s| s.as_str()).unwrap_or("");
            let out1 = day.clock_out.get(0).map(|s| s.as_str()).unwrap_or("");
            let in2 = day.clock_in.get(1).map(|s| s.as_str()).unwrap_or("");
            let out2 = day.clock_out.get(1).map(|s| s.as_str()).unwrap_or("");
            let zangyo = day.zangyo_str();
            let kosoku = day.kosoku_str();

            let sunday_mark = if day.is_sunday { "*" } else { " " };
            println!("{}{:>2} {:>2} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {}",
                sunday_mark, day.day, day.weekday, in1, out1, in2, out2, zangyo, kosoku, day.remarks);
        }
        println!();
    }
}

/// PDFモード: DBからタイムカードを取得してPDF生成
fn run_pdf_mode(args: &[String]) {
    // 年月を引数から取得（デフォルト: 2025年12月）
    let year: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2025);
    let month: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);
    // 特定のドライバーIDを指定可能
    let target_driver_id: Option<i32> = args.get(4).and_then(|s| s.parse().ok());

    println!("=== タイムカードPDF生成 ===");
    println!("対象: {}年{}月", year, month);
    if let Some(id) = target_driver_id {
        println!("ドライバーID: {}", id);
    }
    println!();

    // 本番DBに接続
    let config = DbConfig::production();
    println!("接続先: {}:{}", config.host, config.port);

    let db = match TimecardDb::connect(&config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("DB接続エラー: {}", e);
            return;
        }
    };
    println!("接続成功！");
    println!();

    // 基礎日数を取得
    let kiso_date = match db.get_kiso_date(year, month) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("基礎日数取得エラー: {}", e);
            return;
        }
    };
    println!("基礎日数: {}", kiso_date);
    println!();

    // タイムカードを取得
    let mut timecards = match db.get_all_monthly_timecards_with_kiso(year, month) {
        Ok(tc) => tc,
        Err(e) => {
            eprintln!("タイムカード取得エラー: {}", e);
            return;
        }
    };

    // 特定ドライバーのみにフィルタリング
    if let Some(driver_id) = target_driver_id {
        timecards.retain(|tc| tc.driver.id == driver_id);
    }

    println!("取得したタイムカード数: {}", timecards.len());
    println!();

    // time_card_allowanceテーブルを差分更新（Docker DB）
    println!("time_card_allowance（Docker DB）を差分更新...");
    match db.sync_all_timecard_allowances_to_docker(&timecards) {
        Ok((inserted, updated, unchanged)) => {
            println!("[OK] 追加: {}, 更新: {}, 変更なし: {}",
                     inserted, updated, unchanged);
        }
        Err(e) => {
            eprintln!("[ERROR] 同期失敗: {}", e);
        }
    }
    println!();

    // PDF生成
    // A4横向き: 297mm x 210mm
    let mut pdf = TcpdfCompat::new(297.0, 210.0, "L");
    pdf.render_timecards(&timecards);

    let output_path = if let Some(id) = target_driver_id {
        format!("timecard_{}_{:02}_{}.pdf", year, month, id)
    } else {
        format!("timecard_{}_{:02}.pdf", year, month)
    };
    pdf.save(&output_path).expect("Failed to save PDF");

    println!();
    println!("PDF saved to {}", output_path);
}

/// PDF集計モード: DBからタイムカードを取得してPDF生成（1人/ページ、日付横並び）
fn run_pdf_shukei_mode(args: &[String]) {
    // 年月を引数から取得（デフォルト: 2025年12月）
    let year: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2025);
    let month: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);
    // 特定のドライバーIDを指定可能（テスト用）
    let target_driver_id: Option<i32> = args.get(4).and_then(|s| s.parse().ok());

    println!("=== タイムカードPDF生成（集計モード）===");
    println!("対象: {}年{}月", year, month);
    println!("形式: 1人1ページ、日付横並び");
    if let Some(id) = target_driver_id {
        println!("ドライバーID: {}", id);
    }
    println!();

    // 本番DBに接続
    let config = DbConfig::production();
    println!("接続先: {}:{}", config.host, config.port);

    let db = match TimecardDb::connect(&config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("DB接続エラー: {}", e);
            return;
        }
    };
    println!("接続成功！");
    println!();

    // 全ドライバーのタイムカードを取得（基礎日数付き）
    let all_timecards = match db.get_all_monthly_timecards_with_kiso(year, month) {
        Ok(tc) => tc,
        Err(e) => {
            eprintln!("タイムカード取得エラー: {}", e);
            return;
        }
    };

    // 特定のドライバーIDが指定されていればフィルタ
    let timecards: Vec<_> = if let Some(id) = target_driver_id {
        all_timecards.into_iter().filter(|tc| tc.driver.id == id).collect()
    } else {
        all_timecards
    };

    println!("取得したタイムカード数: {}", timecards.len());
    println!();

    // PDF生成（集計モード）
    // A4横向き: 297mm x 210mm
    let mut pdf = TcpdfCompat::new(297.0, 210.0, "L");
    pdf.render_timecards_shukei(&timecards);

    let output_path = format!("timecard_shukei_{}_{:02}.pdf", year, month);
    pdf.save(&output_path).expect("Failed to save PDF");

    println!();
    println!("PDF saved to {}", output_path);
}

/// 検証モード: 本番DBから計算してDocker DBにINSERT
fn run_verify_mode(args: &[String]) {
    let year: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2025);
    let month: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);

    println!("=== 検証モード: 拘束時間計算 → Docker DB INSERT ===");
    println!("対象: {}年{}月", year, month);
    println!();

    // 本番DBに接続
    let config = DbConfig::production();
    println!("本番DB接続先: {}:{}", config.host, config.port);

    let db = match TimecardDb::connect(&config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("DB接続エラー: {}", e);
            return;
        }
    };
    println!("本番DB接続成功！");
    println!();

    // 全ドライバーのタイムカードを取得（拘束時間計算含む）
    let timecards = match db.get_all_monthly_timecards(year, month) {
        Ok(tc) => tc,
        Err(e) => {
            eprintln!("タイムカード取得エラー: {}", e);
            return;
        }
    };

    println!("取得したタイムカード数: {}", timecards.len());

    // Docker DBにINSERT
    println!();
    println!("Docker DBに拘束時間をINSERT...");
    match db.insert_kosoku_to_docker(&timecards) {
        Ok(count) => {
            println!("[OK] {}件INSERT完了", count);
        }
        Err(e) => {
            eprintln!("[ERROR] INSERT失敗: {}", e);
        }
    }

    println!();
    println!("検証コマンド:");
    println!("  python3 scripts/db_verify.py --compare --year {} --month {}", year, month);
}

/// 検証モード（デジタコ版）: 本番DBから計算してDocker DBにINSERT
fn run_verify_digitacho_mode(args: &[String]) {
    let year: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2025);
    let month: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);

    println!("=== 検証モード（デジタコ版）: DtakoEvents計算 → Docker DB INSERT ===");
    println!("対象: {}年{}月", year, month);
    println!();

    // 本番DBに接続
    let config = DbConfig::production();
    println!("本番DB接続先: {}:{}", config.host, config.port);

    let db = match TimecardDb::connect(&config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("DB接続エラー: {}", e);
            return;
        }
    };
    println!("本番DB接続成功！");
    println!();

    // アクティブドライバーを取得
    let drivers = match db.get_active_drivers(year, month) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("ドライバー取得エラー: {}", e);
            return;
        }
    };

    println!("アクティブドライバー数: {}", drivers.len());
    println!();

    // Docker DBにデジタコ版拘束時間をINSERT
    println!("Docker DBにデジタコ版拘束時間をINSERT...");
    let mut total_inserted = 0;
    let mut error_count = 0;

    for (i, driver) in drivers.iter().enumerate() {
        match db.insert_digitacho_kosoku_to_docker(driver.id, year, month) {
            Ok(count) => {
                total_inserted += count;
                if (i + 1) % 10 == 0 {
                    println!("  進捗: {}/{} ドライバー処理完了", i + 1, drivers.len());
                }
            }
            Err(e) => {
                eprintln!("[ERROR] driver_id={}: {}", driver.id, e);
                error_count += 1;
            }
        }
    }

    println!();
    println!("[OK] {}件INSERT完了 (エラー: {}件)", total_inserted, error_count);

    println!();
    println!("検証コマンド:");
    println!("  python3 scripts/db_verify.py --compare-dtako --year {} --month {}", year, month);
}

/// JSONモード: 座標JSONからPDF生成
fn run_json_mode() {
    // PHPから出力された座標JSONを読み込む
    let json_path = "pdf_coordinates_20251230_172511.json";
    let json_str = fs::read_to_string(json_path)
        .expect("Failed to read coordinate JSON");

    let data: CoordinateData = serde_json::from_str(&json_str)
        .expect("Failed to parse JSON");

    println!("Page size: {}mm x {}mm", data.page_width_mm, data.page_height_mm);
    println!("Orientation: {}", data.orientation);
    println!("Total pages: {}", data.total_pages);
    println!("Total elements: {}", data.elements.len());

    // PDF生成
    let mut pdf = TcpdfCompat::new(
        data.page_width_mm,
        data.page_height_mm,
        &data.orientation,
    );

    pdf.render_elements(&data.elements);
    pdf.save("output_y05.pdf").expect("Failed to save PDF");

    println!("PDF saved to output_y05.pdf");
}
