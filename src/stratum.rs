use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;

use crate::engine::PoolEngine;
use crate::jobs::JobManager;
use crate::protocol::{
    LoginParams, StratumNotify, StratumRequest, StratumResponse, SubmitParams, METHOD_LOGIN,
    METHOD_SUBMIT,
};
use crate::stats::PoolStats;

const MAX_CONNS_PER_IP: usize = 16;
const MAX_CONNS_TOTAL: usize = 4096;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct ConnState {
    counts: HashMap<String, usize>,
    total: usize,
}

pub struct StratumServer {
    listen_addr: SocketAddr,
    engine: Arc<PoolEngine>,
    jobs: Arc<JobManager>,
    stats: Arc<PoolStats>,
    conn_state: Arc<Mutex<ConnState>>,
}

impl StratumServer {
    pub fn new(
        listen_addr: SocketAddr,
        engine: Arc<PoolEngine>,
        jobs: Arc<JobManager>,
        stats: Arc<PoolStats>,
    ) -> Arc<Self> {
        Arc::new(Self {
            listen_addr,
            engine,
            jobs,
            stats,
            conn_state: Arc::new(Mutex::new(ConnState {
                counts: HashMap::new(),
                total: 0,
            })),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        tracing::info!(addr = %self.listen_addr, "stratum listening");

        loop {
            let (stream, peer) = listener.accept().await?;
            let ip = peer.ip().to_string();

            if !self.try_track_conn(&ip) {
                tracing::warn!(ip = %ip, "rejecting stratum connection due to limits");
                continue;
            }

            let this = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(err) = this.handle_conn(stream, peer).await {
                    tracing::warn!(peer = %peer, error = %err, "stratum connection ended with error");
                }
                this.untrack_conn(&ip);
            });
        }
    }

    fn try_track_conn(&self, ip: &str) -> bool {
        let mut state = self.conn_state.lock();
        if state.total >= MAX_CONNS_TOTAL {
            return false;
        }
        let count = state.counts.entry(ip.to_string()).or_default();
        if *count >= MAX_CONNS_PER_IP {
            return false;
        }
        *count += 1;
        state.total += 1;
        true
    }

    fn untrack_conn(&self, ip: &str) {
        let mut state = self.conn_state.lock();
        if let Some(count) = state.counts.get_mut(ip) {
            if *count > 1 {
                *count -= 1;
            } else {
                state.counts.remove(ip);
            }
        }
        if state.total > 0 {
            state.total -= 1;
        }
    }

    async fn handle_conn(&self, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let conn_id = peer.to_string();
        let (reader_half, writer_half) = stream.into_split();
        let writer = Arc::new(AsyncMutex::new(writer_half));

        let mut logged_in: Option<(String, String, u64)> = None; // address, worker, difficulty
        let mut lines = BufReader::new(reader_half).lines();
        let mut rx_jobs = self.jobs.subscribe();

        let login_deadline = tokio::time::sleep(LOGIN_TIMEOUT);
        tokio::pin!(login_deadline);

        loop {
            tokio::select! {
                _ = &mut login_deadline, if logged_in.is_none() => {
                    tracing::warn!(peer = %peer, "stratum login timeout");
                    return Ok(());
                }
                maybe_job = rx_jobs.recv(), if logged_in.is_some() => {
                    if let Ok(job) = maybe_job {
                        if let Some((_, _, difficulty)) = logged_in.as_ref() {
                            if let Some(miner_job) = self.jobs.build_miner_job(*difficulty) {
                                let notify = StratumNotify {
                                    method: "job".to_string(),
                                    params: serde_json::to_value(miner_job)?,
                                };
                                send_json(&writer, &notify).await?;
                            }
                        }
                        let _ = job;
                    }
                }
                line = lines.next_line() => {
                    let Some(line) = line? else {
                        break;
                    };
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let req: StratumRequest = match serde_json::from_str(trimmed) {
                        Ok(v) => v,
                        Err(_) => {
                            send_error(&writer, 0, "invalid JSON").await?;
                            continue;
                        }
                    };

                    match req.method.as_str() {
                        METHOD_LOGIN => {
                            let params: LoginParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    send_error(&writer, req.id, "invalid login params").await?;
                                    continue;
                                }
                            };

                            match self.engine.login(
                                &conn_id,
                                params.address.clone(),
                                Some(params.worker.clone()),
                                params.protocol_version,
                                params.capabilities.clone(),
                            ) {
                                Ok(login_result) => {
                                    let response = StratumResponse {
                                        id: req.id,
                                        status: Some("ok".to_string()),
                                        error: None,
                                        result: Some(serde_json::to_value(login_result)?),
                                    };
                                    send_json(&writer, &response).await?;

                                    let worker = if params.worker.trim().is_empty() {
                                        "default".to_string()
                                    } else {
                                        params.worker.trim().chars().take(64).collect()
                                    };

                                    logged_in = Some((
                                        params.address.clone(),
                                        worker.clone(),
                                        self.engine
                                            .session_difficulty(&conn_id)
                                            .unwrap_or(1),
                                    ));
                                    self.stats.add_miner(&conn_id, &params.address, &worker);

                                    if let Some(miner_job) = self.jobs.build_miner_job(
                                        self.engine.session_difficulty(&conn_id).unwrap_or(1),
                                    ) {
                                        let notify = StratumNotify {
                                            method: "job".to_string(),
                                            params: serde_json::to_value(miner_job)?,
                                        };
                                        send_json(&writer, &notify).await?;
                                    }
                                }
                                Err(err) => {
                                    send_error(&writer, req.id, &err.to_string()).await?;
                                    return Ok(());
                                }
                            }
                        }
                        METHOD_SUBMIT => {
                            let params: SubmitParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    send_error(&writer, req.id, "invalid submit params").await?;
                                    continue;
                                }
                            };

                            let engine = Arc::clone(&self.engine);
                            let submit_conn_id = conn_id.clone();
                            let submit = tokio::task::spawn_blocking(move || {
                                engine.submit(&submit_conn_id, params.job_id, params.nonce, params.claimed_hash)
                            })
                            .await;

                            match submit {
                                Ok(Ok(ack)) => {
                                    let response = StratumResponse {
                                        id: req.id,
                                        status: Some("ok".to_string()),
                                        error: None,
                                        result: Some(serde_json::json!({
                                            "accepted": ack.accepted,
                                            "verified": ack.verified,
                                            "status": ack.status,
                                        })),
                                    };
                                    if let Some((address, _, difficulty)) = logged_in.as_ref() {
                                        self.stats.record_accepted_share(address, *difficulty);
                                    }
                                    send_json(&writer, &response).await?;
                                }
                                Ok(Err(err)) => {
                                    if let Some((address, _, _)) = logged_in.as_ref() {
                                        self.stats.record_rejected_share(address);
                                    }
                                    send_error(&writer, req.id, &err.to_string()).await?;
                                }
                                Err(err) => {
                                    if let Some((address, _, _)) = logged_in.as_ref() {
                                        self.stats.record_rejected_share(address);
                                    }
                                    send_error(
                                        &writer,
                                        req.id,
                                        &format!("submit worker failure: {err}"),
                                    )
                                    .await?;
                                }
                            }
                        }
                        _ => {
                            send_error(&writer, req.id, "unknown method").await?;
                        }
                    }
                }
            }
        }

        if let Some((address, _, _)) = logged_in {
            self.engine.disconnect(&conn_id);
            self.stats.remove_miner(&conn_id);
            tracing::debug!(peer = %peer, address = %address, "stratum miner disconnected");
        }

        Ok(())
    }
}

async fn send_error(
    writer: &Arc<AsyncMutex<tokio::net::tcp::OwnedWriteHalf>>,
    id: u64,
    msg: &str,
) -> Result<()> {
    let response = StratumResponse {
        id,
        status: None,
        error: Some(msg.to_string()),
        result: None,
    };
    send_json(writer, &response).await
}

async fn send_json<T: serde::Serialize>(
    writer: &Arc<AsyncMutex<tokio::net::tcp::OwnedWriteHalf>>,
    value: &T,
) -> Result<()> {
    let mut data = serde_json::to_vec(value)?;
    data.push(b'\n');

    let mut guard = writer.lock().await;
    guard.write_all(&data).await?;
    Ok(())
}