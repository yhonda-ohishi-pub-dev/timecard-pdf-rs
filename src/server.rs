use axum::{
    routing::{get, post},
    Router, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
    extract::State,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::{CorsLayer, Any};

use crate::db::{DbConfig, TimecardDb};
use crate::tcpdf_compat::TcpdfCompat;

/// アプリケーション状態（DBの設定情報を共有）
#[derive(Clone)]
pub struct AppState {
    pub db_config: DbConfig,
}

/// PDF生成リクエスト
#[derive(Deserialize)]
pub struct PdfRequest {
    pub year: i32,
    pub month: u32,
    pub driver_id: Option<i32>,
}

/// エラーレスポンス
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// HTTPサーバーを起動
pub async fn run(port: u16) {
    let state = AppState {
        db_config: DbConfig::production(),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/pdf", post(generate_pdf))
        .route("/api/pdf-shukei", post(generate_pdf_shukei))
        .layer(cors)
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("Failed to bind to port");

    println!("Server listening on port {}", port);
    axum::serve(listener, app).await.expect("Server failed");
}

/// ヘルスチェック
async fn health_check() -> &'static str {
    "OK"
}

/// PDF生成（3人/ページ）
async fn generate_pdf(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PdfRequest>,
) -> Response {
    // DBに接続
    let db = match TimecardDb::connect(&state.db_config) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("DB connection failed: {}", e) }),
            ).into_response();
        }
    };

    // タイムカードを取得
    let mut timecards = match db.get_all_monthly_timecards_with_kiso(req.year, req.month) {
        Ok(tc) => tc,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("Failed to get timecards: {}", e) }),
            ).into_response();
        }
    };

    // 特定ドライバーのみにフィルタリング
    if let Some(driver_id) = req.driver_id {
        timecards.retain(|tc| tc.driver.id == driver_id);
    }

    if timecards.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: "No timecards found".to_string() }),
        ).into_response();
    }

    // PDF生成
    let mut pdf = TcpdfCompat::new(297.0, 210.0, "L");
    pdf.render_timecards(&timecards);

    // PDFをメモリ上で生成
    match pdf.save_to_bytes() {
        Ok(bytes) => {
            (
                StatusCode::OK,
                [
                    ("content-type", "application/pdf"),
                    ("content-disposition", "attachment; filename=\"timecard.pdf\""),
                ],
                bytes,
            ).into_response()
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("PDF generation failed: {}", e) }),
            ).into_response()
        }
    }
}

/// PDF生成（集計モード: 1人/ページ）
async fn generate_pdf_shukei(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PdfRequest>,
) -> Response {
    // DBに接続
    let db = match TimecardDb::connect(&state.db_config) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("DB connection failed: {}", e) }),
            ).into_response();
        }
    };

    // タイムカードを取得
    let all_timecards = match db.get_all_monthly_timecards_with_kiso(req.year, req.month) {
        Ok(tc) => tc,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("Failed to get timecards: {}", e) }),
            ).into_response();
        }
    };

    // 特定ドライバーのみにフィルタリング
    let timecards: Vec<_> = if let Some(driver_id) = req.driver_id {
        all_timecards.into_iter().filter(|tc| tc.driver.id == driver_id).collect()
    } else {
        all_timecards
    };

    if timecards.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: "No timecards found".to_string() }),
        ).into_response();
    }

    // PDF生成（集計モード）
    let mut pdf = TcpdfCompat::new(297.0, 210.0, "L");
    pdf.render_timecards_shukei(&timecards);

    // PDFをメモリ上で生成
    match pdf.save_to_bytes() {
        Ok(bytes) => {
            (
                StatusCode::OK,
                [
                    ("content-type", "application/pdf"),
                    ("content-disposition", "attachment; filename=\"timecard_shukei.pdf\""),
                ],
                bytes,
            ).into_response()
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("PDF generation failed: {}", e) }),
            ).into_response()
        }
    }
}
