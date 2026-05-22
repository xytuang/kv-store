use crate::server::storage::KVStore;
use crate::utils::{DeleteResponse, GetResponse, PutRequest, PutResponse};
use ::std::sync::{Arc, Mutex};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
};

pub fn build_app() -> Router {
    let shared_state = Arc::new(Mutex::new(KVStore::new(
        "/Users/txy/Desktop/kv-store/data".to_string(),
    )));
    Router::new()
        .route("/get/{key}", get(get_key))
        .route("/put/{key}", put(put_key))
        .route("/delete/{key}", delete(delete_key))
        .with_state(shared_state)
}

async fn get_key(
    State(state): State<Arc<Mutex<KVStore>>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match state.lock().unwrap().get(&key) {
        Ok(value) => (
            StatusCode::OK,
            Json(GetResponse {
                value: value.to_string(),
                error: "".to_string(),
            }),
        ),
        Err(err) => (
            StatusCode::NOT_FOUND,
            Json(GetResponse {
                value: "".to_string(),
                error: err.to_string(),
            }),
        ),
    }
}

async fn put_key(
    State(state): State<Arc<Mutex<KVStore>>>,
    Path(key): Path<String>,
    Json(payload): Json<PutRequest>,
) -> impl IntoResponse {
    state.lock().unwrap().put(key, payload.value);
    (
        StatusCode::OK,
        Json(PutResponse {
            message: "Key set".to_string(),
        }),
    )
}

async fn delete_key(
    State(state): State<Arc<Mutex<KVStore>>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    state.lock().unwrap().delete(&key);
    (
        StatusCode::OK,
        Json(DeleteResponse {
            message: "Key deleted".to_string(),
        }),
    )
}
