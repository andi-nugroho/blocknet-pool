use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{BlockSubmitResponse, Job, NodeApi};

#[derive(Debug, Clone, Deserialize)]
pub struct BlockTemplate {
    pub block: Value,
    pub target: String,
    pub header_base: String,
    #[serde(default)]
    pub reward_address_used: String,
    #[serde(default)]
    pub template_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub peer_id: String,
    pub peers: i64,
    pub chain_height: u64,
    pub best_hash: String,
    pub total_work: u64,
    pub mempool_size: i64,
    pub mempool_bytes: i64,
    pub syncing: bool,
    pub identity_age: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeBlock {
    pub height: u64,
    pub hash: String,
    pub reward: u64,
    pub difficulty: u64,
    pub tx_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletSendResponse {
    pub txid: String,
    pub fee: u64,
    pub change: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletLoadResponse {
    pub loaded: bool,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletUnlockResponse {
    pub locked: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletAddressResponse {
    pub address: String,
    pub view_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletBalance {
    pub spendable: u64,
    pub pending: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitBlockRawResponse {
    pub accepted: bool,
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub height: u64,
}

#[derive(Debug)]
pub struct HttpError {
    pub path: String,
    pub status_code: u16,
    pub body: String,
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}: {}", self.path, self.status_code, self.body)
    }
}

impl std::error::Error for HttpError {}

pub fn is_http_status(err: &anyhow::Error, status: u16) -> bool {
    err.downcast_ref::<HttpError>()
        .is_some_and(|http_err| http_err.status_code == status)
}

#[derive(Debug)]
pub struct NodeClient {
    base_url: String,
    client: Client,
    chain_height: AtomicU64,
    syncing: AtomicBool,
}

impl NodeClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if !token.trim().is_empty() {
            let value = format!("Bearer {}", token.trim());
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&value).context("invalid auth header")?,
            );
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("build node http client")?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            chain_height: AtomicU64::new(0),
            syncing: AtomicBool::new(false),
        })
    }

    pub fn get_status(&self) -> Result<NodeStatus> {
        let status: NodeStatus = self.get_json("/api/status")?;
        self.chain_height
            .store(status.chain_height, Ordering::Relaxed);
        self.syncing.store(status.syncing, Ordering::Relaxed);
        Ok(status)
    }

    pub fn get_block_template(&self, reward_address: Option<&str>) -> Result<BlockTemplate> {
        let mut path = "/api/mining/blocktemplate".to_string();
        if let Some(address) = reward_address.filter(|v| !v.trim().is_empty()) {
            path.push_str("?address=");
            path.push_str(&urlencoding::encode(address));
        }
        self.get_json(&path)
    }

    pub fn get_block(&self, id: &str) -> Result<NodeBlock> {
        self.get_json(&format!("/api/block/{id}"))
    }

    pub fn wallet_send(
        &self,
        address: &str,
        amount: u64,
        idempotency_key: &str,
    ) -> Result<WalletSendResponse> {
        let mut extra = HashMap::<String, String>::new();
        if !idempotency_key.is_empty() {
            extra.insert("Idempotency-Key".to_string(), idempotency_key.to_string());
        }

        let payload = serde_json::json!({
            "address": address,
            "amount": amount,
        });
        self.post_json_with_headers("/api/wallet/send", &payload, &extra)
    }

    pub fn wallet_load(&self, password: &str) -> Result<WalletLoadResponse> {
        let payload = serde_json::json!({ "password": password });
        self.post_json("/api/wallet/load", &payload)
    }

    pub fn wallet_unlock(&self, password: &str) -> Result<WalletUnlockResponse> {
        let payload = serde_json::json!({ "password": password });
        self.post_json("/api/wallet/unlock", &payload)
    }

    pub fn get_wallet_address(&self) -> Result<WalletAddressResponse> {
        self.get_json("/api/wallet/address")
    }

    pub fn get_wallet_balance(&self) -> Result<WalletBalance> {
        self.get_json("/api/wallet/balance")
    }

    pub fn chain_height(&self) -> u64 {
        self.chain_height.load(Ordering::Relaxed)
    }

    pub fn syncing(&self) -> bool {
        self.syncing.load(Ordering::Relaxed)
    }

    fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .with_context(|| format!("GET {path}"))?;
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(HttpError {
                path: path.to_string(),
                status_code: status.as_u16(),
                body,
            }));
        }
        serde_json::from_str(&body).with_context(|| format!("decode JSON response for GET {path}"))
    }

    fn post_json<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<R> {
        self.post_json_with_headers(path, payload, &HashMap::new())
    }

    fn post_json_with_headers<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        payload: &T,
        headers: &HashMap<String, String>,
    ) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url).json(payload);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().with_context(|| format!("POST {path}"))?;
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(HttpError {
                path: path.to_string(),
                status_code: status.as_u16(),
                body,
            }));
        }
        serde_json::from_str(&body).with_context(|| format!("decode JSON response for POST {path}"))
    }
}

impl NodeApi for NodeClient {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        if let Some(template_id) = job.template_id.as_ref().filter(|v| !v.trim().is_empty()) {
            let payload = serde_json::json!({
                "template_id": template_id,
                "nonce": nonce,
            });
            if let Ok(resp) =
                self.post_json::<_, SubmitBlockRawResponse>("/api/mining/submitblock", &payload)
            {
                return Ok(BlockSubmitResponse {
                    accepted: resp.accepted,
                    hash: if resp.hash.is_empty() {
                        None
                    } else {
                        Some(resp.hash)
                    },
                    height: if resp.height == 0 {
                        None
                    } else {
                        Some(resp.height)
                    },
                });
            }
        }

        let full_block = job
            .full_block
            .as_ref()
            .ok_or_else(|| anyhow!("missing full block payload for submit fallback"))?;

        let mut block = full_block.clone();
        if let Some(header) = block.get_mut("header").and_then(|v| v.as_object_mut()) {
            if header.contains_key("Nonce") {
                header.insert("Nonce".to_string(), serde_json::json!(nonce));
            } else {
                header.insert("nonce".to_string(), serde_json::json!(nonce));
            }
        }

        let resp: SubmitBlockRawResponse = self.post_json("/api/mining/submitblock", &block)?;
        Ok(BlockSubmitResponse {
            accepted: resp.accepted,
            hash: if resp.hash.is_empty() {
                None
            } else {
                Some(resp.hash)
            },
            height: if resp.height == 0 {
                None
            } else {
                Some(resp.height)
            },
        })
    }
}