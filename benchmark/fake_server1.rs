#!/usr/bin/env rust-script
//! Install `rust-script` with `cargo install rust-script` and run with:
//!
//! rust-script ./fake_server.rs
//!
//! ```cargo
//! [dependencies]
//! axum = "0.7.7"
//! tokio = { version = "1.41.0", features = ["full"] }
//! ```

use axum::body::Body;
use axum::extract::Request;
use axum::{Router, response::IntoResponse, routing::get, routing::post};
#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/http/echo", post(echo_handler))
        .route("/http/echo", get(echo_handler))
        .route("/health", get(health_handler));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn echo_handler(_req: Request<Body>) -> impl IntoResponse {
    println!("Echo request");
    "ok"
}

async fn health_handler() -> impl IntoResponse {
    println!("Health check");
    "ok"
}
