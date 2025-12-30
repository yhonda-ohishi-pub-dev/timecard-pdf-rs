use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub struct CoordinateData {
    pub page_width_mm: f64,
    pub page_height_mm: f64,
    pub orientation: String,
    pub unit: String,
    pub total_pages: u32,
    pub elements: Vec<Element>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Element {
    pub seq: u32,
    #[serde(rename = "type")]
    pub element_type: String,
    pub page: u32,
    pub params: Value,
}

// MultiCell パラメータ
#[derive(Debug, Deserialize)]
pub struct MultiCellParams {
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
    pub text: Value,  // String, Number, or null
    pub border: Value,
    pub align: String,
    pub fill: bool,
    pub ln: i32,
}

// Cell パラメータ
#[derive(Debug, Deserialize)]
pub struct CellParams {
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
    pub text: Value,  // String, Number, or null
    pub border: Value,
    pub align: String,
    pub fill: bool,
    pub ln: i32,
    pub link: String,
}

// Line パラメータ
#[derive(Debug, Deserialize)]
pub struct LineParams {
    pub x1: f64,
    pub y1: f64,
    pub x2: f64,
    pub y2: f64,
}

// Link パラメータ
#[derive(Debug, Deserialize)]
pub struct LinkParams {
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
    pub link: String,
}

// SetFont パラメータ
#[derive(Debug, Deserialize)]
pub struct SetFontParams {
    pub family: String,
    pub style: String,
    pub size: Option<f64>,
}

// setFontSize パラメータ
#[derive(Debug, Deserialize)]
pub struct SetFontSizeParams {
    pub size: f64,
}

// setFillColor パラメータ
#[derive(Debug, Deserialize)]
pub struct SetFillColorParams {
    pub col1: i32,
    pub col2: i32,
    pub col3: i32,
    pub col4: i32,
}

// AddPage パラメータ
#[derive(Debug, Deserialize)]
pub struct AddPageParams {
    pub orientation: String,
    pub format: Vec<f64>,
}

// setAbsX パラメータ
#[derive(Debug, Deserialize)]
pub struct SetAbsXParams {
    pub x: f64,
}

// setAbsY パラメータ
#[derive(Debug, Deserialize)]
pub struct SetAbsYParams {
    pub y: f64,
}

// Ln パラメータ
#[derive(Debug, Deserialize)]
pub struct LnParams {
    pub h: Value,
    pub y_before: f64,
}
