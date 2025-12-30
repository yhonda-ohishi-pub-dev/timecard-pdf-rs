use printpdf::*;
use lopdf::{Document, Object, Dictionary, StringFormat};
use std::fs::File;
use std::io::{BufWriter, Cursor};

use crate::coordinate_data::*;

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
        // フォントを読み込む
        let font_data = std::fs::read("fonts/msmincho01.ttf")
            .expect("Failed to read font file");
        let cursor = Cursor::new(font_data);
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

        // Y座標を調整（TCPDFのsetFontSizeによる隙間を補正）
        // 整数座標に丸める（例: 10.93 → 11 → 実質10として扱う）
        let y_adjusted = p.y.floor();

        if let (Some(layer), Some(font)) = (&self.current_layer, &self.font) {
            // テキスト描画
            if let Some(text) = get_text_from_value(&p.text) {
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
