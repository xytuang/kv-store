#[cfg(test)]
mod tests {
    use crate::client;
    use crate::utils::GetResponse;
    use axum::http::StatusCode;
    use reqwest::Client;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tokio::net::TcpListener;

    async fn start_test_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, crate::server::build_app())
                .await
                .unwrap();
        });
        format!("http://{}", addr)
    }

    async fn run_test_file(base_url: &str, contents: &str) {
        let http = Client::new();
        let mut expected: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        for line in contents.lines().filter(|l| !l.trim().is_empty()) {
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            match parts[0].to_uppercase().as_str() {
                "PUT" => {
                    let (key, value) = (parts[1], parts[2]);
                    let resp = client::put(&http, base_url, key, value).await.unwrap();
                    assert_eq!(resp.message, "Key set", "PUT {key} failed");
                    expected.insert(key.to_string(), value.to_string());
                }
                "GET" => {
                    let key = parts[1];
                    if let Some(want) = expected.get(key) {
                        let resp = client::get(&http, base_url, key).await.unwrap();
                        assert_eq!(&resp.value, want, "GET {key} returned wrong value");
                    } else {
                        // Key was never PUT — expect a 404 from the raw response
                        let url = format!("{base_url}/get/{key}");
                        let resp = http.get(&url).send().await.unwrap();
                        assert_eq!(
                            resp.status(),
                            StatusCode::NOT_FOUND,
                            "GET {key} expected 404 for unknown key"
                        );
                    }
                }
                _ => panic!("Unknown operation: {line}"),
            }
        }
    }

    #[tokio::test]
    async fn test_generated_file() {
        let base_url = start_test_server().await;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "PUT name Alice").unwrap();
        writeln!(file, "PUT age 30").unwrap();
        writeln!(file, "GET name").unwrap();
        writeln!(file, "GET age").unwrap();
        writeln!(file, "GET missing_key").unwrap();

        let contents = std::fs::read_to_string(file.path()).unwrap();
        run_test_file(&base_url, &contents).await;
    }

    #[tokio::test]
    async fn test_from_file() {
        let base_url = start_test_server().await;

        let path = std::path::Path::new("tests/fixtures/basic.txt");
        if !path.exists() {
            return;
        }
        let contents = std::fs::read_to_string(path).unwrap();
        run_test_file(&base_url, &contents).await;
    }

    #[tokio::test]
    async fn test_overwrite_key() {
        let base_url = start_test_server().await;

        let script = "\
PUT color red
GET color
PUT color blue
GET color
";
        run_test_file(&base_url, script).await;
    }
}
