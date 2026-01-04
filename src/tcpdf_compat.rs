use printpdf::*;
use printpdf::path::{PaintMode, WindingOrder};
use lopdf::{Document, Object, Dictionary, StringFormat};
use std::fs::File;
use std::io::{BufWriter, Cursor};

use crate::coordinate_data::*;
use crate::timecard_data::MonthlyTimecard;

/// 埋め込みフォント（MS明朝）- バイナリに静的に埋め込む
static MSMINCHO_FONT: &[u8] = include_bytes!("../fonts/msmincho01.ttf");

/// mm → Mm型
fn mm(val: f64) -> Mm {
    Mm(val as f32)
}

/// mm → pt (1mm = 2.834645669pt)
fn mm_to_pt(mm: f64) -> f64 {
    mm * 2.834645669
}

/// serde_json::Value からテキストを取得（String, Number, null対応）
fn get_text_from_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => {
            if s.is_empty() || s.trim().is_empty() {
                None
            } else {
                Some(s.clone())
            }
        }
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None, // null や他の型は無視
    }
}

/// テキストのX座標を計算（align対応）
/// align: "L" = 左揃え, "C" = 中央揃え, "R" = 右揃え
fn calc_text_x(cell_x: f64, cell_w: f64, text: &str, font_size_pt: f32, align: &str) -> f64 {
    // 文字幅の概算（日本語は全角、英数字は半角として計算）
    let char_width_mm = font_size_pt as f64 * 0.352778; // 1pt = 0.352778mm
    let text_width: f64 = text.chars().map(|c| {
        if c.is_ascii() {
            char_width_mm * 0.5 // 半角
        } else {
            char_width_mm // 全角
        }
    }).sum();

    let padding = 0.5; // パディング

    match align {
        "C" => cell_x + (cell_w - text_width) / 2.0,
        "R" => cell_x + cell_w - text_width - padding,
        _ => cell_x + padding, // "L" またはその他は左揃え
    }
}

/// TCPDF座標系(左上原点) → PDF座標系(左下原点) 変換
/// テキストはベースライン基準、セル内で垂直中央揃え
fn y_convert_text(y_mm: f64, h_mm: f64, font_size_pt: f32, page_height_mm: f64) -> Mm {
    // フォントサイズ(pt)をmmに変換: 1pt = 0.352778mm
    let font_size_mm = font_size_pt as f64 * 0.352778;
    // セルの中央にテキストを配置（ベースライン調整込み）
    // セルの上端からの距離 = (セル高さ + フォント高さ) / 2 - ディセンダ分
    let descender = font_size_mm * 0.2; // ディセンダ（下に出る部分）
    let text_y = y_mm + (h_mm + font_size_mm) / 2.0 - descender;
    mm(page_height_mm - text_y)
}

/// 矩形・線用のY座標変換
fn y_convert(y_mm: f64, page_height_mm: f64) -> Mm {
    mm(page_height_mm - y_mm)
}

/// リンク情報を保持する構造体
#[derive(Debug, Clone)]
pub struct LinkInfo {
    pub page: u32,      // 1-indexed
    pub x_mm: f64,
    pub y_mm: f64,
    pub w_mm: f64,
    pub h_mm: f64,
    pub url: String,
}

pub struct TcpdfCompat {
    doc: PdfDocumentReference,
    page_width_mm: f64,
    page_height_mm: f64,
    current_layer: Option<PdfLayerReference>,
    font: Option<IndirectFontRef>,
    font_size: f32,
    fill_color: Color,
    page_count: u32,
    first_page_layer: Option<PdfLayerReference>,
    links: Vec<LinkInfo>,  // リンク情報を保存
}

impl TcpdfCompat {
    pub fn new(page_width_mm: f64, page_height_mm: f64, _orientation: &str) -> Self {
        let (doc, page, layer) = PdfDocument::new(
            "TimeCard PDF",
            mm(page_width_mm),
            mm(page_height_mm),
            "Layer 1",
        );

        // 最初のページのレイヤーを保存
        let first_layer = doc.get_page(page).get_layer(layer);

        TcpdfCompat {
            doc,
            page_width_mm,
            page_height_mm,
            current_layer: None,
            font: None,
            font_size: 10.0,
            fill_color: Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)),
            page_count: 0,
            first_page_layer: Some(first_layer),
            links: Vec::new(),
        }
    }

    pub fn render_elements(&mut self, elements: &[Element]) {
        // 埋め込みフォントを使用
        let cursor = Cursor::new(MSMINCHO_FONT.to_vec());
        self.font = Some(
            self.doc
                .add_external_font(cursor)
                .expect("Failed to add font"),
        );

        for element in elements {
            match element.element_type.as_str() {
                "AddPage" => self.handle_add_page(&element.params),
                "MultiCell" => self.handle_multi_cell(&element.params),
                "Cell" => self.handle_cell(&element.params),
                "Line" => self.handle_line(&element.params),
                "Link" => self.handle_link(&element.params),
                "SetFont" => self.handle_set_font(&element.params),
                "setFontSize" => self.handle_set_font_size(&element.params),
                "setFillColor" => self.handle_set_fill_color(&element.params),
                "setAbsX" => {}
                "setAbsY" => {}
                "Ln" => {}
                _ => {}
            }
        }
    }

    fn handle_add_page(&mut self, _params: &serde_json::Value) {
        self.page_count += 1;

        if self.page_count == 1 {
            // 最初のAddPageは、PdfDocument::newで作成済みのページを使う
            self.current_layer = self.first_page_layer.take();
        } else {
            // 2ページ目以降は新しいページを追加
            let (page, layer) = self.doc.add_page(
                mm(self.page_width_mm),
                mm(self.page_height_mm),
                "Layer 1",
            );
            self.current_layer = Some(self.doc.get_page(page).get_layer(layer));
        }
    }

    fn handle_multi_cell(&mut self, params: &serde_json::Value) {
        let p: MultiCellParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };

        // Y座標を5mm単位のグリッドに揃える（セルの高さは5mm）
        // 例: 15.93 → 15, 16.0 → 15, 16.1 → 15
        let y_adjusted = (p.y / 5.0).floor() * 5.0;

        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            // 塗りつぶし描画（テキストや枠線より先に描画）
            if p.fill {
                self.draw_filled_rect(p.x, y_adjusted, p.w, p.h);
            }

            // テキスト描画（塗りつぶし後は色を黒に戻す）
            if let Some(text) = get_text_from_value(&p.text) {
                layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                let x = calc_text_x(p.x, p.w, &text, self.font_size, &p.align);
                let x_mm = mm(x);
                let y_mm = y_convert_text(y_adjusted, p.h, self.font_size, self.page_height_mm);
                layer.use_text(&text, self.font_size, x_mm, y_mm, font);
            }

            // 枠線描画
            if let Some(border) = p.border.as_i64() {
                if border == 1 {
                    self.draw_rect(p.x, y_adjusted, p.w, p.h);
                }
            }
        }
    }

    fn handle_cell(&mut self, params: &serde_json::Value) {
        // Cell はTCPDFのMultiCell内部から呼ばれるため、テキストのみ描画
        // 枠線はMultiCellで既に描画済み
        let p: CellParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };

        // Cellは枠線を描画しない（MultiCellで描画済み）
        // テキストも重複するのでスキップ
        let _ = p; // unused warning を抑制
    }

    fn handle_line(&mut self, params: &serde_json::Value) {
        let p: LineParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };

        if let Some(layer) = &self.current_layer {
            let points = vec![
                (Point::new(mm(p.x1), y_convert(p.y1, self.page_height_mm)), false),
                (Point::new(mm(p.x2), y_convert(p.y2, self.page_height_mm)), false),
            ];
            let line = Line {
                points,
                is_closed: false,
            };
            layer.add_line(line);
        }
    }

    fn handle_link(&mut self, params: &serde_json::Value) {
        let p: LinkParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };

        // リンク情報を保存（後でlopdfで追加）
        self.links.push(LinkInfo {
            page: self.page_count,
            x_mm: p.x,
            y_mm: p.y,
            w_mm: p.w,
            h_mm: p.h,
            url: p.link,
        });
    }

    fn handle_set_font(&mut self, params: &serde_json::Value) {
        let p: SetFontParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };
        if let Some(size) = p.size {
            self.font_size = size as f32;
        }
    }

    fn handle_set_font_size(&mut self, params: &serde_json::Value) {
        let p: SetFontSizeParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };
        self.font_size = p.size as f32;
    }

    fn handle_set_fill_color(&mut self, params: &serde_json::Value) {
        let p: SetFillColorParams = match serde_json::from_value(params.clone()) {
            Ok(p) => p,
            Err(_) => return,
        };
        if p.col2 == -1 {
            let gray = p.col1 as f32 / 255.0;
            self.fill_color = Color::Rgb(Rgb::new(gray, gray, gray, None));
        } else {
            let r = p.col1 as f32 / 255.0;
            let g = p.col2 as f32 / 255.0;
            let b = p.col3 as f32 / 255.0;
            self.fill_color = Color::Rgb(Rgb::new(r, g, b, None));
        }
    }

    fn draw_rect(&self, x: f64, y: f64, w: f64, h: f64) {
        if let Some(layer) = &self.current_layer {
            // 線幅を設定（TCPDFのデフォルトは約0.2mm）
            layer.set_outline_thickness(0.2);

            let points = vec![
                (Point::new(mm(x), y_convert(y, self.page_height_mm)), false),
                (Point::new(mm(x + w), y_convert(y, self.page_height_mm)), false),
                (Point::new(mm(x + w), y_convert(y + h, self.page_height_mm)), false),
                (Point::new(mm(x), y_convert(y + h, self.page_height_mm)), false),
            ];
            let rect = Line {
                points,
                is_closed: true,
            };
            layer.add_line(rect);
        }
    }

    fn draw_filled_rect(&self, x: f64, y: f64, w: f64, h: f64) {
        if let Some(layer) = &self.current_layer {
            let points = vec![
                (Point::new(mm(x), y_convert(y, self.page_height_mm)), false),
                (Point::new(mm(x + w), y_convert(y, self.page_height_mm)), false),
                (Point::new(mm(x + w), y_convert(y + h, self.page_height_mm)), false),
                (Point::new(mm(x), y_convert(y + h, self.page_height_mm)), false),
            ];
            let polygon = Polygon {
                rings: vec![points],
                mode: PaintMode::Fill,
                winding_order: WindingOrder::NonZero,
            };
            layer.set_fill_color(self.fill_color.clone());
            layer.add_polygon(polygon);
        }
    }

    /// タイムカードデータからPDFを生成
    /// 1ページに3人分のタイムカードを配置
    pub fn render_timecards(&mut self, timecards: &[MonthlyTimecard]) {
        // 埋め込みフォントを使用
        let cursor = Cursor::new(MSMINCHO_FONT.to_vec());
        self.font = Some(
            self.doc
                .add_external_font(cursor)
                .expect("Failed to add font"),
        );

        // レイアウト定数
        const PERSON_WIDTH: f64 = 99.0;  // 1人分の幅（297mm / 3）
        const HEADER_HEIGHT: f64 = 10.0; // ヘッダー高さ
        const ROW_HEIGHT: f64 = 5.0;     // 行高さ
        const TOP_MARGIN: f64 = 5.0;     // 上マージン

        // カラム幅（合計 = 93mm）
        const COL_DAY: f64 = 8.0;        // 日
        const COL_WEEKDAY: f64 = 6.0;    // 曜
        const COL_TIME: f64 = 11.0;      // 出勤/退社（4列 = 44mm）
        const COL_OVERTIME: f64 = 11.0;  // 残業
        const COL_REMARKS: f64 = 11.0;   // 備考
        const COL_KOSOKU: f64 = 13.0;    // 拘束時間
        const TABLE_WIDTH: f64 = COL_DAY + COL_WEEKDAY + COL_TIME * 4.0 + COL_OVERTIME + COL_REMARKS + COL_KOSOKU; // 93mm
        const LEFT_MARGIN: f64 = PERSON_WIDTH - TABLE_WIDTH;  // 右寄せ

        // 3人ずつページを作成
        for (chunk_idx, chunk) in timecards.chunks(3).enumerate() {
            // ページ追加
            self.page_count += 1;
            if self.page_count == 1 {
                self.current_layer = self.first_page_layer.take();
            } else {
                let (page, layer) = self.doc.add_page(
                    mm(self.page_width_mm),
                    mm(self.page_height_mm),
                    "Layer 1",
                );
                self.current_layer = Some(self.doc.get_page(page).get_layer(layer));
            }

            // ページを3等分する縦線を描画（PHPのmakeIniLine相当）
            self.draw_vertical_line(PERSON_WIDTH, 0.0, self.page_height_mm);
            self.draw_vertical_line(PERSON_WIDTH * 2.0, 0.0, self.page_height_mm);

            // 各人のタイムカードを描画
            for (person_idx, timecard) in chunk.iter().enumerate() {
                let x_offset = person_idx as f64 * PERSON_WIDTH + LEFT_MARGIN;

                // ヘッダー描画
                self.render_timecard_header(timecard, x_offset, TOP_MARGIN, TABLE_WIDTH, HEADER_HEIGHT);

                // カラムヘッダー描画
                let col_header_y = TOP_MARGIN + HEADER_HEIGHT;
                self.render_column_headers(x_offset, col_header_y, ROW_HEIGHT,
                    COL_DAY, COL_WEEKDAY, COL_TIME, COL_OVERTIME, COL_REMARKS, COL_KOSOKU);

                // データ行描画
                let data_start_y = col_header_y + ROW_HEIGHT;
                self.render_timecard_data(timecard, x_offset, data_start_y, ROW_HEIGHT,
                    COL_DAY, COL_WEEKDAY, COL_TIME, COL_OVERTIME, COL_REMARKS, COL_KOSOKU);

                // 集計部分を描画（31日分のデータの下）
                let summary_y = data_start_y + 31.0 * ROW_HEIGHT;
                self.render_timecard_summary(timecard, x_offset, summary_y, ROW_HEIGHT, TABLE_WIDTH);
            }

            println!("Page {} rendered ({} people)", chunk_idx + 1, chunk.len());
        }
    }

    /// タイムカードヘッダー（氏名、年月）を描画
    fn render_timecard_header(&self, timecard: &MonthlyTimecard, x: f64, y: f64, w: f64, h: f64) {
        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            // 枠線
            self.draw_rect(x, y, w, h);

            // 氏名（左側）
            let name = &timecard.driver.name;
            layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
            let name_x = mm(x + 2.0);
            let name_y = y_convert_text(y, h, 12.0, self.page_height_mm);
            layer.use_text(name, 12.0, name_x, name_y, font);

            // 氏名にリンクを追加（PHPのTimeCardController.php:3629相当）
            let year_month_link = format!("{}-{:02}", timecard.year, timecard.month);
            let link_w = 30.0;
            layer.add_link_annotation(printpdf::LinkAnnotation::new(
                printpdf::Rect::new(
                    mm(x + 2.0),
                    mm(self.page_height_mm - y - h),
                    mm(x + 2.0 + link_w),
                    mm(self.page_height_mm - y),
                ),
                None,
                None,
                printpdf::Actions::uri(format!("/time-card?driver_id={}&month={}", timecard.driver.id, year_month_link)),
                None,
            ));

            // 年月（右側）
            let year_month = timecard.year_month_str();
            let ym_x = mm(x + w - 35.0);
            layer.use_text(&year_month, 10.0, ym_x, name_y, font);
        }
    }

    /// カラムヘッダーを描画
    fn render_column_headers(&self, x: f64, y: f64, h: f64,
        col_day: f64, col_weekday: f64, col_time: f64, col_overtime: f64, col_remarks: f64, col_kosoku: f64) {

        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));

            let headers = ["日", "曜", "出勤1", "退社1", "出勤2", "退社2", "残業", "備考", "拘束"];
            let widths = [col_day, col_weekday, col_time, col_time, col_time, col_time, col_overtime, col_remarks, col_kosoku];

            let mut current_x = x;
            for (header, width) in headers.iter().zip(widths.iter()) {
                // 枠線
                self.draw_rect(current_x, y, *width, h);

                // テキスト（中央揃え）
                let text_x = calc_text_x(current_x, *width, header, 10.0, "C");
                let text_y = y_convert_text(y, h, 10.0, self.page_height_mm);
                layer.use_text(*header, 10.0, mm(text_x), text_y, font);

                current_x += width;
            }
        }
    }

    /// タイムカードデータ行を描画
    fn render_timecard_data(&self, timecard: &MonthlyTimecard, x: f64, start_y: f64, row_h: f64,
        col_day: f64, col_weekday: f64, col_time: f64, col_overtime: f64, col_remarks: f64, col_kosoku: f64) {

        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            let widths = [col_day, col_weekday, col_time, col_time, col_time, col_time, col_overtime, col_remarks, col_kosoku];

            for (row_idx, day) in timecard.days.iter().enumerate() {
                let y = start_y + row_idx as f64 * row_h;

                // データ配列を作成
                let in1 = day.clock_in.get(0).map(|s| s.as_str()).unwrap_or("");
                let out1 = day.clock_out.get(0).map(|s| s.as_str()).unwrap_or("");
                let in2 = day.clock_in.get(1).map(|s| s.as_str()).unwrap_or("");
                let out2 = day.clock_out.get(1).map(|s| s.as_str()).unwrap_or("");

                // 備考（PHPでは畜/引マークを備考に出力していない）
                let remarks = day.remarks.clone();

                let values = [
                    day.day.to_string(),
                    day.weekday.clone(),
                    in1.to_string(),
                    out1.to_string(),
                    in2.to_string(),
                    out2.to_string(),
                    day.zangyo_str(),       // 残業（旅費から取得）
                    remarks,                // 備考
                    day.kosoku_str(),       // 拘束時間（別列）
                ];

                // 各セルを描画
                let mut current_x = x;
                for (col_idx, (value, width)) in values.iter().zip(widths.iter()).enumerate() {
                    // 曜日列（col_idx=1）で日曜日の場合はグレー背景
                    if col_idx == 1 && day.is_sunday {
                        self.draw_filled_rect_gray(current_x, y, *width, row_h);
                    }

                    // 拘束時間列（col_idx=8）で14時間（840分）超えの場合はグレー背景
                    if col_idx == 8 {
                        if let Some(minutes) = day.kosoku_minutes {
                            if minutes > 840 {
                                self.draw_filled_rect_gray(current_x, y, *width, row_h);
                            }
                        }
                    }

                    // 枠線
                    self.draw_rect(current_x, y, *width, row_h);

                    // テキスト描画 - 色を黒に設定してから描画
                    if !value.is_empty() {
                        layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                        // 拘束時間列（col_idx=8）は8pt、それ以外は10pt
                        let font_size = if col_idx == 8 { 8.0 } else { 10.0 };
                        let text_x = calc_text_x(current_x, *width, value, font_size, "C");
                        let text_y = y_convert_text(y, row_h, font_size, self.page_height_mm);
                        layer.use_text(value, font_size, mm(text_x), text_y, font);
                    }

                    current_x += width;
                }
            }
        }
    }

    /// 集計部分を描画
    fn render_timecard_summary(&self, timecard: &MonthlyTimecard, x: f64, y: f64, row_h: f64, width: f64) {
        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));

            let summary = &timecard.summary;

            // 1行目: 社員番号、氏名、拘束時間合計
            let kyuyo_id = timecard.driver.kyuyo_shain_id
                .map(|id| id.to_string())
                .unwrap_or_default();

            // 社員番号
            layer.use_text(&kyuyo_id, 10.0, mm(x + 2.0), y_convert_text(y, row_h, 10.0, self.page_height_mm), font);

            // 氏名
            layer.use_text(&timecard.driver.name, 10.0, mm(x + 15.0), y_convert_text(y, row_h, 10.0, self.page_height_mm), font);

            // 拘束時間合計（右端）
            let kosoku_str = summary.total_kosoku_str();
            layer.use_text(&kosoku_str, 10.0, mm(x + width - 18.0), y_convert_text(y, row_h, 10.0, self.page_height_mm), font);

            // 2行目: ヘッダー（出、休、有、欠、遅、早、特）
            let y2 = y + row_h;
            let col_w = 10.0;
            let headers = ["出", "休", "有", "欠", "遅", "早", "特"];
            for (i, header) in headers.iter().enumerate() {
                let cell_x = x + i as f64 * col_w;
                self.draw_rect(cell_x, y2, col_w, row_h);
                let text_x = calc_text_x(cell_x, col_w, header, 10.0, "C");
                layer.use_text(*header, 10.0, mm(text_x), y_convert_text(y2, row_h, 10.0, self.page_height_mm), font);
            }

            // 3行目: 値（出勤、休日、有休、欠勤、遅刻、早退、特休）
            let y3 = y2 + row_h;
            let values = [
                summary.shukkin.to_string(),
                summary.kyuka.to_string(),
                summary.yukyu.to_string(),
                summary.kekkin.to_string(),
                summary.chikoku.to_string(),
                summary.soutai.to_string(),
                summary.tokukyu.to_string(),
            ];
            for (i, value) in values.iter().enumerate() {
                let cell_x = x + i as f64 * col_w;
                self.draw_rect(cell_x, y3, col_w, row_h);
                let text_x = calc_text_x(cell_x, col_w, value, 10.0, "C");
                layer.use_text(value, 10.0, mm(text_x), y_convert_text(y3, row_h, 10.0, self.page_height_mm), font);
            }

            // 4行目: ヘッダー（残業、休出、引、畜、追）
            let y4 = y3 + row_h;
            let headers2 = ["残業", "休出", "引", "畜", "追"];
            let widths2 = [14.0, 10.0, 10.0, 10.0, 10.0];
            let mut cx = x;
            for (header, w) in headers2.iter().zip(widths2.iter()) {
                self.draw_rect(cx, y4, *w, row_h);
                let text_x = calc_text_x(cx, *w, header, 10.0, "C");
                layer.use_text(*header, 10.0, mm(text_x), y_convert_text(y4, row_h, 10.0, self.page_height_mm), font);
                cx += w;
            }

            // 5行目: 値（残業合計、休出、トレーラー、家畜、追加）
            let y5 = y4 + row_h;
            let zangyo_str = if summary.total_zangyo != 0.0 {
                if summary.total_zangyo.fract() == 0.0 {
                    format!("{}", summary.total_zangyo as i32)
                } else {
                    format!("{:.1}", summary.total_zangyo)
                }
            } else {
                "0".to_string()
            };
            let values2 = [
                zangyo_str,
                summary.kyushutsu.to_string(),
                summary.trailer.to_string(),
                summary.kachiku.to_string(),
                summary.tsuika.to_string(),
            ];
            let mut cx = x;
            for (value, w) in values2.iter().zip(widths2.iter()) {
                self.draw_rect(cx, y5, *w, row_h);
                let text_x = calc_text_x(cx, *w, value, 10.0, "C");
                layer.use_text(value, 10.0, mm(text_x), y_convert_text(y5, row_h, 10.0, self.page_height_mm), font);
                cx += w;
            }
        }
    }

    /// 集計モード: タイムカードデータからPDFを生成
    /// 1人1ページ、日付を横並びで表示
    pub fn render_timecards_shukei(&mut self, timecards: &[MonthlyTimecard]) {
        // 埋め込みフォントを使用
        let cursor = Cursor::new(MSMINCHO_FONT.to_vec());
        self.font = Some(
            self.doc
                .add_external_font(cursor)
                .expect("Failed to add font"),
        );

        for timecard in timecards {
            // ページ追加
            self.page_count += 1;
            if self.page_count == 1 {
                self.current_layer = self.first_page_layer.take();
            } else {
                let (page, layer) = self.doc.add_page(
                    mm(self.page_width_mm),
                    mm(self.page_height_mm),
                    "Layer 1",
                );
                self.current_layer = Some(self.doc.get_page(page).get_layer(layer));
            }

            let days_in_month = timecard.days.len();
            let cell_w = 8.0;  // 日付セルの幅（8mm × 31日 = 248mm）
            let ind_x = 5.0;   // 左マージン
            let mut y = 5.0;   // 開始Y座標

            // ===== ヘッダー: 年月（左上）、氏名 =====
            if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));

                // 年月（左上）
                let year_month_display = format!("{}年{}月", timecard.year, timecard.month);
                layer.use_text(&year_month_display, 12.0, mm(ind_x), y_convert_text(y, 6.0, 12.0, self.page_height_mm), font);

                // 氏名（年月の右側に配置）
                layer.use_text(&timecard.driver.name, 14.0, mm(ind_x + 30.0), y_convert_text(y, 6.0, 14.0, self.page_height_mm), font);
            }

            // 氏名にリンクを追加
            let year_month_link = format!("{}-{:02}", timecard.year, timecard.month);
            if let Some(layer) = &self.current_layer {
                let name_x = ind_x + 30.0;
                let name_w = 40.0;
                let name_h = 6.0;
                layer.add_link_annotation(printpdf::LinkAnnotation::new(
                    printpdf::Rect::new(
                        mm(name_x),
                        mm(self.page_height_mm - y - name_h),
                        mm(name_x + name_w),
                        mm(self.page_height_mm - y),
                    ),
                    None,
                    None,
                    printpdf::Actions::uri(format!("/time-card?driver_id={}&month={}", timecard.driver.id, year_month_link)),
                    None,
                ));
            }

            // ===== リンクボタン: TC, 集計, 出勤簿, DrV（名前の右側） =====
            let link_w = 30.0;
            let link_h = 5.0;
            let link_x = ind_x + 75.0;  // 名前の右側に配置
            let link_y = y;
            let link_labels = ["TC", "集計", "出勤簿", "DrV"];
            let year_month_str = format!("{}-{:02}-01", timecard.year, timecard.month);

            for (i, label) in link_labels.iter().enumerate() {
                let x = link_x + i as f64 * link_w;
                // 背景（グレー）
                self.draw_filled_rect_gray(x, link_y, link_w, link_h);
                // 枠線
                self.draw_rect(x, link_y, link_w, link_h);
                // テキスト
                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let text_x = calc_text_x(x, link_w, *label, 10.0, "C");
                    layer.use_text(*label, 10.0, mm(text_x), y_convert_text(link_y, link_h, 10.0, self.page_height_mm), font);
                }
            }

            // リンクを追加（printpdf のAnnotation使用）
            if let Some(layer) = &self.current_layer {
                let driver_id = timecard.driver.id;
                let links = [
                    format!("/time-card?driver_id={}&month={}", driver_id, year_month_str),
                    format!("/time-card/create-pdf/{}/{}/集計/計算", driver_id, year_month_str),
                    format!("/time-card/create-shukkinbo/{}/{}", year_month_str, driver_id),
                    format!("/drivers/view/{}", driver_id),
                ];
                for (i, url) in links.iter().enumerate() {
                    let x = link_x + i as f64 * link_w;
                    layer.add_link_annotation(printpdf::LinkAnnotation::new(
                        printpdf::Rect::new(
                            mm(x),
                            mm(self.page_height_mm - link_y - link_h),
                            mm(x + link_w),
                            mm(self.page_height_mm - link_y),
                        ),
                        None, // border
                        None, // color
                        printpdf::Actions::uri(url.clone()),
                        None, // highlighting mode
                    ));
                }
            }

            y += 8.0;

            // ===== 曜日行 =====
            for (i, day) in timecard.days.iter().enumerate() {
                let x = ind_x + i as f64 * cell_w;
                let is_sunday = day.weekday == "日";

                // 日曜日は背景をグレーに
                if is_sunday {
                    self.draw_filled_rect_gray(x, y, cell_w, 4.0);
                }

                // 枠線
                self.draw_rect(x, y, cell_w, 4.0);

                // 曜日テキスト
                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let text_x = calc_text_x(x, cell_w, &day.weekday, 10.0, "C");
                    layer.use_text(&day.weekday, 10.0, mm(text_x), y_convert_text(y, 4.0, 10.0, self.page_height_mm), font);
                }
            }
            y += 4.0;

            // ===== 日付行 =====
            for (i, day) in timecard.days.iter().enumerate() {
                let x = ind_x + i as f64 * cell_w;

                // 枠線
                self.draw_rect(x, y, cell_w, 4.0);

                // 日付テキスト
                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let day_str = day.day.to_string();
                    let text_x = calc_text_x(x, cell_w, &day_str, 10.0, "C");
                    layer.use_text(&day_str, 10.0, mm(text_x), y_convert_text(y, 4.0, 10.0, self.page_height_mm), font);
                }
            }
            y += 4.0;

            // ===== 勤務状態行 =====
            // PHP: dispShukkinStH相当
            // <出勤 >退勤 [デジタコ出勤 ]デジタコ退勤 X休暇 =デジタコのみ -TC_DCのみ
            for (i, day) in timecard.days.iter().enumerate() {
                let x = ind_x + i as f64 * cell_w;

                // 休暇の場合は背景をグレー
                let is_kyuka = matches!(day.remarks.as_str(), "公休" | "泊休" | "有休" | "特休" | "欠勤");
                if is_kyuka {
                    self.draw_filled_rect_gray(x, y, cell_w, 4.0);
                }

                // 枠線
                self.draw_rect(x, y, cell_w, 4.0);

                // 勤務状態テキスト
                // PHPロジック: デジタコがある日は[/]、ない日は</>
                // [ = デジタコデータがある日の出勤
                // ] = デジタコデータがある日の退勤
                // < = デジタコデータがない日の出勤
                // > = デジタコデータがない日の退勤
                // = = 出退勤なしでデジタコのみ
                // - = 出退勤なしで拘束時間（TC_DC）のみ
                // X = 休暇
                let mut st = String::new();

                // デジタコデータまたは備考が「仮乗」の場合は[/]を使用
                let drive_st = day.has_digitacho || day.remarks == "仮乗";
                let (arrow_left, arrow_right) = if drive_st { ('[', ']') } else { ('<', '>') };

                // 出勤/退勤マーク（最大2回分）
                if !day.clock_in.is_empty() {
                    st.push(arrow_left);
                }
                if !day.clock_out.is_empty() {
                    st.push(arrow_right);
                }
                if day.clock_in.len() > 1 {
                    st.push(arrow_left);
                }
                if day.clock_out.len() > 1 {
                    st.push(arrow_right);
                }

                // 休暇マーク
                if is_kyuka {
                    st.push('X');
                }

                // 出退勤がない場合
                if st.is_empty() {
                    if drive_st {
                        // デジタコデータのみ（出退勤なし）
                        st.push('=');
                    } else if day.kosoku_minutes.is_some() {
                        // 拘束時間（TC_DC）のみ
                        st.push('-');
                    }
                }

                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let text_x = calc_text_x(x, cell_w, &st, 9.0, "C");
                    layer.use_text(&st, 9.0, mm(text_x), y_convert_text(y, 4.0, 9.0, self.page_height_mm), font);
                }
            }
            y += 4.0;

            // ===== 手当行（T=トレーラー, K=家畜） =====
            for (i, day) in timecard.days.iter().enumerate() {
                let x = ind_x + i as f64 * cell_w;

                // 枠線
                self.draw_rect(x, y, cell_w, 4.0);

                // 手当マーク
                let mut teate = String::new();
                if day.is_trailer {
                    teate.push('T');
                }
                if day.is_kachiku {
                    teate.push('K');
                }

                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let text_x = calc_text_x(x, cell_w, &teate, 9.0, "C");
                    layer.use_text(&teate, 9.0, mm(text_x), y_convert_text(y, 4.0, 9.0, self.page_height_mm), font);
                }
            }
            // ===== 左下: 日別タイムカード（カレンダーの下、Y=30.0から開始） =====
            // render_timecardsと同じ関数を使用
            let daily_list_y = 30.0;
            let row_h = 5.0;
            let col_day = 8.0;
            let col_weekday = 6.0;
            let col_time = 11.0;
            let col_overtime = 11.0;
            let col_remarks = 11.0;
            let col_kosoku = 13.0;

            // カラムヘッダー描画
            self.render_column_headers(ind_x, daily_list_y, row_h,
                col_day, col_weekday, col_time, col_overtime, col_remarks, col_kosoku);

            // データ行描画
            let data_start_y = daily_list_y + row_h;
            self.render_timecard_data(timecard, ind_x, data_start_y, row_h,
                col_day, col_weekday, col_time, col_overtime, col_remarks, col_kosoku);

            // ===== 集計欄: タイムカードリストの右側 =====
            // タイムカードリストの幅: 8+6+11*4+11+11+13 = 93mm
            let summary_x = ind_x + 100.0;  // タイムカードリストの右側
            let summary_y = daily_list_y;   // カレンダーの下と同じ高さ

            // 社員番号・氏名
            if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                let kyuyo_id = timecard.driver.kyuyo_shain_id
                    .map(|id| id.to_string())
                    .unwrap_or_default();
                layer.use_text(&kyuyo_id, 10.0, mm(summary_x), y_convert_text(summary_y, 5.0, 10.0, self.page_height_mm), font);
                layer.use_text(&timecard.driver.name, 10.0, mm(summary_x + 15.0), y_convert_text(summary_y, 5.0, 10.0, self.page_height_mm), font);
            }

            // 集計表（出/休/有/欠/遅/早/特）- 社員番号・氏名の下
            self.render_shukei_summary_right(timecard, summary_x, summary_y + 5.0);

            // ===== カウント欄（運転/作業/休暇/不明）: 集計表の右隣 =====
            // 集計表の幅: 10*7 = 70mm
            let count_x = summary_x + 75.0;  // 集計表の右隣
            let count_w = 10.0;
            let count_y = summary_y;  // 社員番号と同じ高さから開始

            // カウントヘッダー
            let count_headers = ["運転", "作業", "休暇", "不明"];
            for (i, header) in count_headers.iter().enumerate() {
                let x = count_x + i as f64 * count_w;
                self.draw_rect(x, count_y, count_w, 5.0);
                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let text_x = calc_text_x(x, count_w, header, 9.0, "C");
                    layer.use_text(*header, 9.0, mm(text_x), y_convert_text(count_y, 5.0, 9.0, self.page_height_mm), font);
                }
            }

            // カウント値を計算
            let mut unten = 0;  // 運転（拘束時間あり）
            let mut sagyo = 0;  // 作業（出退勤ありだが拘束時間なし）
            let mut kyuka = 0;  // 休暇

            for day in &timecard.days {
                let is_kyuka = matches!(day.remarks.as_str(), "公休" | "泊休" | "有休" | "特休" | "欠勤" | "入社前" | "退職後");
                if is_kyuka {
                    kyuka += 1;
                } else if day.kosoku_minutes.is_some() {
                    unten += 1;
                } else if !day.clock_in.is_empty() || !day.clock_out.is_empty() {
                    sagyo += 1;
                }
            }
            let fumei = days_in_month as i32 - unten - sagyo - kyuka;

            // カウント値
            let count_values = [
                unten.to_string(),
                sagyo.to_string(),
                kyuka.to_string(),
                fumei.to_string(),
            ];
            for (i, value) in count_values.iter().enumerate() {
                let x = count_x + i as f64 * count_w;
                let vy = count_y + 5.0;

                // 不明がある場合はハイライト
                if i == 3 && fumei > 0 {
                    self.draw_filled_rect_gray(x, vy, count_w, 5.0);
                }

                self.draw_rect(x, vy, count_w, 5.0);
                if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
                    layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
                    let text_x = calc_text_x(x, count_w, value, 9.0, "C");
                    layer.use_text(value, 9.0, mm(text_x), y_convert_text(vy, 5.0, 9.0, self.page_height_mm), font);
                }
            }

            println!("Page {} rendered: {}", self.page_count, timecard.driver.name);
        }
    }

    /// 集計モード: 右側に集計部分を描画（参考レイアウト準拠）
    fn render_shukei_summary_right(&self, timecard: &MonthlyTimecard, x: f64, y: f64) {
        let summary = &timecard.summary;
        let col_w = 10.0;
        let row_h = 5.0;

        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            layer.set_fill_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));

            // 1行目ヘッダー: 出/休/有/欠/遅/早/特
            let y1 = y;
            let headers1 = ["出", "休", "有", "欠", "遅", "早", "特"];
            for (i, header) in headers1.iter().enumerate() {
                let cx = x + i as f64 * col_w;
                self.draw_rect(cx, y1, col_w, row_h);
                let text_x = calc_text_x(cx, col_w, header, 10.0, "C");
                layer.use_text(*header, 10.0, mm(text_x), y_convert_text(y1, row_h, 10.0, self.page_height_mm), font);
            }

            // 1行目値
            let y2 = y1 + row_h;
            let format_f64 = |v: f64| -> String {
                if v.fract() == 0.0 {
                    format!("{}", v as i32)
                } else {
                    format!("{:.1}", v)
                }
            };
            let values1 = [
                format_f64(summary.shukkin),
                summary.kyuka.to_string(),
                format_f64(summary.yukyu),
                summary.kekkin.to_string(),
                summary.chikoku.to_string(),
                summary.soutai.to_string(),
                summary.tokukyu.to_string(),
            ];
            for (i, value) in values1.iter().enumerate() {
                let cx = x + i as f64 * col_w;
                self.draw_rect(cx, y2, col_w, row_h);
                let text_x = calc_text_x(cx, col_w, value, 10.0, "C");
                layer.use_text(value, 10.0, mm(text_x), y_convert_text(y2, row_h, 10.0, self.page_height_mm), font);
            }

            // 2行目ヘッダー: 残業/休出/引/畜/追
            let y3 = y2 + row_h;
            let headers2 = ["残業", "休出", "引", "畜", "追"];
            let widths2 = [14.0, 10.0, 10.0, 10.0, 10.0];
            let mut cx = x;
            for (header, w) in headers2.iter().zip(widths2.iter()) {
                self.draw_rect(cx, y3, *w, row_h);
                let text_x = calc_text_x(cx, *w, header, 10.0, "C");
                layer.use_text(*header, 10.0, mm(text_x), y_convert_text(y3, row_h, 10.0, self.page_height_mm), font);
                cx += w;
            }

            // 2行目値
            let y4 = y3 + row_h;
            let zangyo_str = format_f64(summary.total_zangyo);
            let values2 = [
                zangyo_str,
                format_f64(summary.kyushutsu),
                summary.trailer.to_string(),
                summary.kachiku.to_string(),
                summary.tsuika.to_string(),
            ];
            let mut cx = x;
            for (value, w) in values2.iter().zip(widths2.iter()) {
                self.draw_rect(cx, y4, *w, row_h);
                let text_x = calc_text_x(cx, *w, value, 10.0, "C");
                layer.use_text(value, 10.0, mm(text_x), y_convert_text(y4, row_h, 10.0, self.page_height_mm), font);
                cx += w;
            }
        }
    }

    /// 縦線を描画（ページ分割用）
    fn draw_vertical_line(&self, x: f64, y1: f64, y2: f64) {
        if let Some(layer) = &self.current_layer {
            layer.set_outline_thickness(0.2);
            let points = vec![
                (Point::new(mm(x), y_convert(y1, self.page_height_mm)), false),
                (Point::new(mm(x), y_convert(y2, self.page_height_mm)), false),
            ];
            let line = Line {
                points,
                is_closed: false,
            };
            layer.add_line(line);
        }
    }

    /// グレーの塗りつぶし矩形を描画（日曜日用）
    fn draw_filled_rect_gray(&self, x: f64, y: f64, w: f64, h: f64) {
        if let Some(layer) = &self.current_layer {
            let points = vec![
                (Point::new(mm(x), y_convert(y, self.page_height_mm)), false),
                (Point::new(mm(x + w), y_convert(y, self.page_height_mm)), false),
                (Point::new(mm(x + w), y_convert(y + h, self.page_height_mm)), false),
                (Point::new(mm(x), y_convert(y + h, self.page_height_mm)), false),
            ];
            let polygon = Polygon {
                rings: vec![points],
                mode: PaintMode::Fill,
                winding_order: WindingOrder::NonZero,
            };
            // グレー (200/255 ≈ 0.78)
            layer.set_fill_color(Color::Rgb(Rgb::new(0.78, 0.78, 0.78, None)));
            layer.add_polygon(polygon);
        }
    }

    /// PDFをメモリ上で生成してバイト配列を返す（HTTPレスポンス用）
    pub fn save_to_bytes(self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // まずprintpdfでPDFをメモリ上に生成
        let mut buffer = Vec::new();
        {
            self.doc.save(&mut BufWriter::new(&mut buffer))?;
        }

        // リンクがない場合はそのまま返す
        if self.links.is_empty() {
            return Ok(buffer);
        }

        // lopdfでPDFを読み込んでリンクを追加
        let mut doc = Document::load_mem(&buffer)?;

        let page_height_pt = mm_to_pt(self.page_height_mm);

        for link in &self.links {
            let page_idx = (link.page - 1) as usize;

            let x1_pt = mm_to_pt(link.x_mm);
            let y1_pt = page_height_pt - mm_to_pt(link.y_mm + link.h_mm);
            let x2_pt = mm_to_pt(link.x_mm + link.w_mm);
            let y2_pt = page_height_pt - mm_to_pt(link.y_mm);

            let action_dict = Dictionary::from_iter(vec![
                ("S", Object::Name(b"URI".to_vec())),
                ("URI", Object::String(link.url.as_bytes().to_vec(), StringFormat::Literal)),
            ]);

            let annot_dict = Dictionary::from_iter(vec![
                ("Type", Object::Name(b"Annot".to_vec())),
                ("Subtype", Object::Name(b"Link".to_vec())),
                ("Rect", Object::Array(vec![
                    Object::Real(x1_pt as f32),
                    Object::Real(y1_pt as f32),
                    Object::Real(x2_pt as f32),
                    Object::Real(y2_pt as f32),
                ])),
                ("Border", Object::Array(vec![
                    Object::Integer(0),
                    Object::Integer(0),
                    Object::Integer(0),
                ])),
                ("A", Object::Dictionary(action_dict)),
            ]);

            let annot_id = doc.add_object(Object::Dictionary(annot_dict));

            // ページIDを先に取得
            let page_id = doc.page_iter().nth(page_idx);

            if let Some(page_id) = page_id {
                if let Ok(page_obj) = doc.get_object_mut(page_id) {
                    if let Object::Dictionary(ref mut page_dict) = page_obj {
                        let annots = if let Ok(existing) = page_dict.get(b"Annots") {
                            if let Object::Array(arr) = existing.clone() {
                                let mut new_arr = arr;
                                new_arr.push(Object::Reference(annot_id));
                                new_arr
                            } else {
                                vec![Object::Reference(annot_id)]
                            }
                        } else {
                            vec![Object::Reference(annot_id)]
                        };
                        page_dict.set("Annots", Object::Array(annots));
                    }
                }
            }
        }

        // PDFをメモリ上に保存
        let mut output = Vec::new();
        doc.save_to(&mut output)?;

        Ok(output)
    }

    pub fn save(self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        // まずprintpdfでPDFを保存
        let temp_path = format!("{}.tmp", path);
        {
            let file = File::create(&temp_path)?;
            self.doc.save(&mut BufWriter::new(file))?;
        }

        // lopdfでPDFを開いてリンクを追加
        let mut doc = Document::load(&temp_path)?;

        let page_height_pt = mm_to_pt(self.page_height_mm);

        for link in &self.links {
            // ページインデックス（0-indexed）
            let page_idx = (link.page - 1) as usize;

            // TCPDF座標（左上原点）→ PDF座標（左下原点）に変換
            let x1_pt = mm_to_pt(link.x_mm);
            let y1_pt = page_height_pt - mm_to_pt(link.y_mm + link.h_mm);  // 下端
            let x2_pt = mm_to_pt(link.x_mm + link.w_mm);
            let y2_pt = page_height_pt - mm_to_pt(link.y_mm);  // 上端

            // URIアクション辞書
            let action_dict = Dictionary::from_iter(vec![
                ("S", Object::Name(b"URI".to_vec())),
                ("URI", Object::String(link.url.as_bytes().to_vec(), StringFormat::Literal)),
            ]);

            // リンクアノテーション辞書
            let annot_dict = Dictionary::from_iter(vec![
                ("Type", Object::Name(b"Annot".to_vec())),
                ("Subtype", Object::Name(b"Link".to_vec())),
                ("Rect", Object::Array(vec![
                    Object::Real(x1_pt as f32),
                    Object::Real(y1_pt as f32),
                    Object::Real(x2_pt as f32),
                    Object::Real(y2_pt as f32),
                ])),
                ("Border", Object::Array(vec![
                    Object::Integer(0),
                    Object::Integer(0),
                    Object::Integer(0),
                ])),
                ("A", Object::Dictionary(action_dict)),
            ]);

            // アノテーションオブジェクトを追加
            let annot_id = doc.add_object(Object::Dictionary(annot_dict));

            // ページIDを先に取得
            let page_id = doc.page_iter().nth(page_idx);

            // ページにアノテーションを追加
            if let Some(page_id) = page_id {
                if let Ok(page_obj) = doc.get_object_mut(page_id) {
                    if let Object::Dictionary(ref mut page_dict) = page_obj {
                        // 既存のAnnotsを取得または新規作成
                        let annots = if let Ok(existing) = page_dict.get(b"Annots") {
                            if let Object::Array(arr) = existing.clone() {
                                let mut new_arr = arr;
                                new_arr.push(Object::Reference(annot_id));
                                new_arr
                            } else {
                                vec![Object::Reference(annot_id)]
                            }
                        } else {
                            vec![Object::Reference(annot_id)]
                        };
                        page_dict.set("Annots", Object::Array(annots));
                    }
                }
            }
        }

        // 最終PDFを保存
        doc.save(path)?;

        // 一時ファイルを削除
        std::fs::remove_file(&temp_path)?;

        println!("Added {} links to PDF", self.links.len());

        Ok(())
    }
}
