mod tcpdf_compat;
mod coordinate_data;

use std::fs;
use coordinate_data::CoordinateData;
use tcpdf_compat::TcpdfCompat;

fn main() {
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
