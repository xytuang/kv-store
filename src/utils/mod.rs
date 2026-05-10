use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct PutRequest {
    pub value: String,
}

#[derive(Deserialize, Serialize)]
pub struct GetResponse {
    pub value: String,
    pub error: String,
}

#[derive(Deserialize, Serialize)]
pub struct PutResponse {
    pub message: String,
}
