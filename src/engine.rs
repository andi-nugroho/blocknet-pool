use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;

use crate::config::Config;
use crate::pow::{check_target, difficulty_to_target};
use crate::protocol::{
    build_login_result, normalize_capabilities, normalize_protocol_version, parse_hash_hex,
    should_inline_validation_on_queue_full, CAP_SUBMIT_CLAIMED_HASH,
    STRATUM_PROTOCOL_VERSION_CURRENT,
};
use crate::validation::{
    ValidationEngine, ValidationResult, ValidationTask, SHARE_STATUS_PROVISIONAL,
    SHARE_STATUS_VERIFIED,
};

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub height: u64,
    pub header_base: Vec<u8>,
    pub network_target: [u8; 32],
    pub template_id: Option<String>,
    pub full_block: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ShareRecord {
    pub job_id: String,
    pub miner: String,
    pub worker: String,
    pub difficulty: u64,
    pub nonce: u64,
    pub status: &'static str,
    pub was_sampled: bool,
    pub block_hash: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Default)]
pub struct BlockSubmitResponse {
    pub accepted: bool,
    pub hash: Option<String>,
    pub height: Option<u64>,
}

pub trait NodeApi: Send + Sync + 'static {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse>;
}

pub trait JobRepository: Send + Sync + 'static {
    fn get_job(&self, job_id: &str) -> Option<Job>;
    fn current_job(&self) -> Option<Job>;
}

pub trait ShareStore: Send + Sync + 'static {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool>;
    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()>;
    fn add_share(&self, share: ShareRecord) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct SubmitAck {
    pub accepted: bool,
    pub verified: bool,
    pub status: &'static str,
    pub block_accepted: bool,
}

#[derive(Debug, Clone)]
struct MinerSession {
    address: String,
    worker: String,
    difficulty: u64,
    protocol_version: u32,
    capabilities: BTreeSet<String>,
}

pub struct PoolEngine {
    cfg: Config,
    validation: Arc<ValidationEngine>,
    jobs: Arc<dyn JobRepository>,
    store: Arc<dyn ShareStore>,
    node: Arc<dyn NodeApi>,

    sessions: Mutex<HashMap<String, MinerSession>>,
    seen_in_memory: Mutex<HashSet<(String, u64)>>,
}

impl PoolEngine {
    pub fn new(
        cfg: Config,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        Self {
            cfg,
            validation,
            jobs,
            store,
            node,
            sessions: Mutex::new(HashMap::new()),
            seen_in_memory: Mutex::new(HashSet::new()),
        }
    }

    pub fn login(
        &self,
        conn_id: &str,
        address: String,
        worker: Option<String>,
        protocol_version: u32,
        capabilities: Vec<String>,
    ) -> Result<crate::protocol::LoginResult> {
        let address = address.trim();
        if address.is_empty() || address.len() > 128 {
            return Err(anyhow!("invalid address"));
        }

        let worker = worker
            .unwrap_or_else(|| "default".to_string())
            .chars()
            .take(64)
            .collect::<String>();
        let protocol_version = normalize_protocol_version(protocol_version);
        let caps = normalize_capabilities(&capabilities);
        let cap_set = caps.iter().cloned().collect::<BTreeSet<_>>();

        if self.cfg.stratum_submit_v2_required
            && protocol_version < STRATUM_PROTOCOL_VERSION_CURRENT
        {
            return Err(anyhow!("pool requires protocol_version >= 2"));
        }
        if self.cfg.stratum_submit_v2_required
            && !caps.is_empty()
            && !cap_set.contains(CAP_SUBMIT_CLAIMED_HASH)
        {
            return Err(anyhow!("pool requires submit_claimed_hash capability"));
        }

        let mut sessions = self.sessions.lock();
        sessions.insert(
            conn_id.to_string(),
            MinerSession {
                address: address.to_string(),
                worker,
                difficulty: self.cfg.initial_share_difficulty,
                protocol_version,
                capabilities: cap_set,
            },
        );

        Ok(build_login_result(
            protocol_version,
            self.cfg.stratum_submit_v2_required,
        ))
    }

    pub fn disconnect(&self, conn_id: &str) {
        self.sessions.lock().remove(conn_id);
    }

    pub fn submit(
        &self,
        conn_id: &str,
        job_id: String,
        nonce: u64,
        claimed_hash_hex: Option<String>,
    ) -> Result<SubmitAck> {
        let session = {
            let sessions = self.sessions.lock();
            sessions
                .get(conn_id)
                .cloned()
                .ok_or_else(|| anyhow!("not logged in"))?
        };

        let job = self
            .jobs
            .get_job(&job_id)
            .ok_or_else(|| anyhow!("stale job"))?;

        let seen_key = (job_id.clone(), nonce);
        {
            let mut seen = self.seen_in_memory.lock();
            if !seen.insert(seen_key.clone()) {
                return Err(anyhow!("duplicate share"));
            }
        }

        let result = (|| -> Result<SubmitAck> {
            if self.store.is_share_seen(&job_id, nonce)? {
                return Err(anyhow!("duplicate share"));
            }

            let claimed_hash = match claimed_hash_hex {
                Some(ref value) if !value.trim().is_empty() => {
                    Some(parse_hash_hex(value).map_err(|err| anyhow!(err))?)
                }
                _ => None,
            };

            if self.cfg.stratum_submit_v2_required && claimed_hash.is_none() {
                return Err(anyhow!("claimed hash required"));
            }

            let share_target = difficulty_to_target(session.difficulty.max(1));

            let candidate_hint = claimed_hash
                .map(|hash| check_target(hash, job.network_target))
                .unwrap_or(false);

            let task = ValidationTask {
                address: session.address.clone(),
                nonce,
                header_base: job.header_base.clone(),
                share_target,
                network_target: job.network_target,
                claimed_hash,
                force_full_verify: candidate_hint || claimed_hash.is_none(),
            };

            let validation = self.validate_task(task, claimed_hash.is_some(), candidate_hint)?;

            if !validation.accepted {
                return Err(anyhow!(
                    "{}",
                    validation.reject_reason.unwrap_or("invalid share")
                ));
            }

            self.store.mark_share_seen(&job_id, nonce)?;

            let status = if validation.verified {
                SHARE_STATUS_VERIFIED
            } else {
                SHARE_STATUS_PROVISIONAL
            };

            let mut block_hash = None;
            let mut block_accepted = false;
            if validation.is_block_candidate {
                block_hash = Some(hex_string(validation.hash));
                let submit = self.node.submit_block(&job, nonce)?;
                block_accepted = submit.accepted;
            }

            self.store.add_share(ShareRecord {
                job_id,
                miner: session.address,
                worker: session.worker,
                difficulty: session.difficulty,
                nonce,
                status,
                was_sampled: validation.verified,
                block_hash,
                created_at: SystemTime::now(),
            })?;

            Ok(SubmitAck {
                accepted: true,
                verified: validation.verified,
                status,
                block_accepted,
            })
        })();

        self.seen_in_memory.lock().remove(&seen_key);
        result
    }

    fn validate_task(
        &self,
        task: ValidationTask,
        has_claimed_hash: bool,
        candidate_hint: bool,
    ) -> Result<ValidationResult> {
        if let Some(rx) = self.validation.submit(task.clone(), candidate_hint) {
            let mut timeout = self.cfg.job_timeout_duration();
            timeout = timeout.clamp(Duration::from_secs(5), Duration::from_secs(60));
            return rx
                .recv_timeout(timeout)
                .map_err(|_| anyhow!("validation timeout"));
        }

        if should_inline_validation_on_queue_full(has_claimed_hash, candidate_hint) {
            Ok(self.validation.process_inline(task))
        } else {
            Err(anyhow!("server busy, retry"))
        }
    }

    pub fn session_protocol_version(&self, conn_id: &str) -> Option<u32> {
        self.sessions
            .lock()
            .get(conn_id)
            .map(|session| session.protocol_version)
    }

    pub fn session_difficulty(&self, conn_id: &str) -> Option<u64> {
        self.sessions
            .lock()
            .get(conn_id)
            .map(|session| session.difficulty)
    }

    pub fn session_capabilities(&self, conn_id: &str) -> Option<Vec<String>> {
        self.sessions.lock().get(conn_id).map(|session| {
            session
                .capabilities
                .iter()
                .cloned()
                .collect::<Vec<String>>()
        })
    }
}

fn hex_string(hash: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

#[derive(Default)]
pub struct InMemoryStore {
    seen: Mutex<HashSet<(String, u64)>>,
    shares: Mutex<Vec<ShareRecord>>,
}

impl InMemoryStore {
    pub fn shares(&self) -> Vec<ShareRecord> {
        self.shares.lock().clone()
    }
}

impl ShareStore for InMemoryStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        Ok(self.seen.lock().contains(&(job_id.to_string(), nonce)))
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.seen.lock().insert((job_id.to_string(), nonce));
        Ok(())
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        self.shares.lock().push(share);
        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryJobs {
    current: Mutex<Option<String>>,
    jobs: Mutex<HashMap<String, Job>>,
}

impl InMemoryJobs {
    pub fn insert(&self, job: Job) {
        let id = job.id.clone();
        self.jobs.lock().insert(id.clone(), job);
        *self.current.lock() = Some(id);
    }
}

impl JobRepository for InMemoryJobs {
    fn get_job(&self, job_id: &str) -> Option<Job> {
        self.jobs.lock().get(job_id).cloned()
    }

    fn current_job(&self) -> Option<Job> {
        let current = self.current.lock().clone()?;
        self.jobs.lock().get(&current).cloned()
    }
}

#[derive(Default)]
pub struct InMemoryNode {
    pub submits: Mutex<Vec<(String, u64)>>,
    pub accepted: bool,
}

impl NodeApi for InMemoryNode {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        self.submits.lock().push((job.id.clone(), nonce));
        Ok(BlockSubmitResponse {
            accepted: self.accepted,
            hash: Some(format!("{:x}", nonce)),
            height: Some(job.height),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pow::{difficulty_to_target, DeterministicTestHasher, PowHasher};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn cfg() -> Config {
        Config {
            validation_mode: "probabilistic".to_string(),
            sample_rate: 0.0,
            warmup_shares: 0,
            min_sample_every: 0,
            max_verifiers: 1,
            max_validation_queue: 8,
            job_timeout: "10s".to_string(),
            initial_share_difficulty: 1,
            ..Config::default()
        }
    }

    fn job() -> Job {
        Job {
            id: "job1".to_string(),
            height: 100,
            header_base: vec![1, 2, 3],
            network_target: [0x0f; 32],
            template_id: Some("tmpl1".to_string()),
            full_block: None,
        }
    }

    #[test]
    fn login_negotiates_and_records_session() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(cfg, validation, jobs, store, node);

        let result = engine
            .login(
                "conn1",
                "BTestAddr".to_string(),
                Some("rig01".to_string()),
                2,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect("login");

        assert_eq!(result.protocol_version, 2);
        assert_eq!(engine.session_protocol_version("conn1"), Some(2));
        let caps = engine.session_capabilities("conn1").expect("caps");
        assert!(caps.iter().any(|v| v == "submit_claimed_hash"));
    }

    #[test]
    fn login_rejects_old_protocol_when_required() {
        let mut cfg = cfg();
        cfg.stratum_submit_v2_required = true;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let err = engine
            .login(
                "conn1",
                "BTestAddr".to_string(),
                None,
                1,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect_err("should reject protocol 1 when v2 required");
        assert!(err.to_string().contains("protocol_version"));
    }

    #[test]
    fn submit_rejects_when_not_logged_in() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let err = engine
            .submit("missing", "job1".to_string(), 1, Some("00".repeat(32)))
            .expect_err("must fail");
        assert!(err.to_string().contains("not logged"));
    }

    #[test]
    fn submit_accepts_valid_share_and_records_it() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::clone(&jobs) as Arc<dyn JobRepository>,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );

        engine
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        // In probabilistic mode with sample_rate=0 and no forced full verify, claimed hash drives acceptance.
        let ack = engine
            .submit("conn1", "job1".to_string(), 42, Some("ff".repeat(32)))
            .expect("share should be accepted");

        assert!(ack.accepted);
        assert_eq!(ack.status, SHARE_STATUS_PROVISIONAL);
        assert!(!ack.verified);
        assert!(!ack.block_accepted);

        let shares = store.shares();
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].nonce, 42);
    }

    #[test]
    fn submit_rejects_duplicate_share() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        engine
            .submit("conn1", "job1".to_string(), 7, Some("ff".repeat(32)))
            .expect("first submit");

        let err = engine
            .submit("conn1", "job1".to_string(), 7, Some("ff".repeat(32)))
            .expect_err("duplicate must fail");
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn candidate_share_triggers_block_submission() {
        struct CandidateHasher;
        impl PowHasher for CandidateHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> [u8; 32] {
                [0x00; 32]
            }
        }

        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(CandidateHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            network_target: [0x01; 32],
            ..job()
        });
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });

        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );

        engine
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        let ack = engine
            .submit("conn1", "job1".to_string(), 9, Some("00".repeat(32)))
            .expect("submit candidate");

        assert!(ack.block_accepted);
        assert_eq!(node.submits.lock().len(), 1);

        let shares = store.shares();
        assert_eq!(shares.len(), 1);
        assert!(shares[0].block_hash.is_some());
    }

    #[test]
    fn queue_full_regular_submit_returns_busy_but_legacy_inline_works() {
        struct SlowHasher {
            hit: AtomicBool,
        }
        impl PowHasher for SlowHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> [u8; 32] {
                self.hit.store(true, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(120));
                [0x01; 32]
            }
        }

        let mut cfg = cfg();
        cfg.max_validation_queue = 1;
        cfg.validation_mode = "full".to_string();

        let hasher = Arc::new(SlowHasher {
            hit: AtomicBool::new(false),
        });
        let validation = ValidationEngine::new(cfg.clone(), hasher);
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            network_target: difficulty_to_target(1),
            ..job()
        });

        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        // Fill queue with one candidate so next regular v2 submit sees queue pressure.
        let _ = engine.submit("conn1", "job1".to_string(), 1, Some("00".repeat(32)));

        // v2 regular share should return busy when queue is full.
        let err = engine
            .submit("conn1", "job1".to_string(), 2, Some("ff".repeat(32)))
            .expect_err("regular share should be rejected when queue is full");
        assert!(!err.to_string().is_empty());

        // Legacy path (no claimed hash) should still inline and not return busy.
        engine
            .submit("conn1", "job1".to_string(), 3, None)
            .expect("legacy share should not be dropped");
    }

    #[test]
    fn seen_cache_entry_is_released_after_rejected_submit() {
        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());

        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        let err = engine
            .submit("conn1", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("expected invalid share proof");
        assert!(!err.to_string().is_empty());
        assert!(engine.seen_in_memory.lock().is_empty());
    }

    #[test]
    fn seen_cache_entry_is_released_after_accepted_submit() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");
        engine
            .submit("conn1", "job1".to_string(), 89, Some("ff".repeat(32)))
            .expect("share should be accepted");

        assert!(engine.seen_in_memory.lock().is_empty());
    }
}