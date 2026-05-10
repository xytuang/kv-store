mod client;
mod server;
mod utils;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, server::build_app()).await.unwrap();
    });

    client::start("http://localhost:3000").await?;

    server_task.abort();
    Ok(())
}
