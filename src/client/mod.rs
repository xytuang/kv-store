use crate::utils::{GetResponse, PutResponse, DeleteResponse};
use reqwest::Client;

pub async fn get(
    client: &Client,
    base_url: &str,
    key: &str,
) -> Result<GetResponse, reqwest::Error> {
    let url = format!("{base_url}/get/{key}");
    client.get(&url).send().await?.json::<GetResponse>().await
}

pub async fn put(
    client: &Client,
    base_url: &str,
    key: &str,
    value: &str,
) -> Result<PutResponse, reqwest::Error> {
    let url = format!("{base_url}/put/{key}");
    let body = serde_json::json!({ "value": value });
    client
        .put(&url)
        .json(&body)
        .send()
        .await?
        .json::<PutResponse>()
        .await
}

pub async fn delete(
    client: &Client,
    base_url: &str,
    key: &str
) -> Result<DeleteResponse, reqwest::Error> {
    let url = format!("{base_url}/delete/{key}");
    client.delete(&url).send().await?.json::<DeleteResponse>().await
}

pub async fn start(base_url: &str) -> Result<(), reqwest::Error> {
    let client = Client::new();

    // Run a basic smoke-check script against the server
    let script = "\
PUT foo hello
PUT bar world
GET foo
GET bar
GET missing_key
";
    run_script(&client, base_url, script).await?;
    Ok(())
}

pub async fn run_script(
    client: &Client,
    base_url: &str,
    script: &str,
) -> Result<(), reqwest::Error> {
    for line in script.lines().filter(|l| !l.trim().is_empty()) {
        let parts: Vec<&str> = line.splitn(3, ' ').collect();
        match parts[0].to_uppercase().as_str() {
            "PUT" => {
                let (key, value) = (parts[1], parts[2]);
                let resp = put(client, base_url, key, value).await?;
                println!("PUT {key} = {value} → {}", resp.message);
            }
            "GET" => {
                let key = parts[1];
                let resp = get(client, base_url, key).await?;
                if resp.error.is_empty() {
                    println!("GET {key} → {}", resp.value);
                } else {
                    println!("GET {key} → error: {}", resp.error);
                }
            }
            _ => eprintln!("Unknown operation: {line}"),
        }
    }
    Ok(())
}
