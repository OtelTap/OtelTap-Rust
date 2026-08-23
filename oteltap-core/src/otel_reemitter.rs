
use std::time::Duration;
use hyper::body::Bytes;
use reqwest::{Client, Url};

// Forwards OTLP data to another endpoint, e.g., a collector or backend.
pub(crate) struct OtelReemitter {
    client: Client,
    endpoint: Url,
}

impl OtelReemitter {

    pub(crate) fn new(endpoint: &str) -> std::io::Result<Self> {

        let endpoint = Url::parse(endpoint)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let client = Client::builder()
            .timeout(Duration::from_secs(10)) // TODO: Make this configurable
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self { client, endpoint })
    }

    // Re-emits the given OTLP data to the configured endpoint.
    pub(crate) async fn reemit(&self, data: Bytes) -> Result<(), reqwest::Error> {

        self.client
            .post(self.endpoint.clone())
            .header("Content-Type", "application/x-protobuf")
            .body(data)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }   
}