use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::config::Config;
use crate::pow::{check_target, PowHasher};

pub const SHARE_STATUS_VERIFIED: &str = "verified";
pub const SHARE_STATUS_PROVISIONAL: &str = "provisional";

#[derive(Debug, Clone)]
pub struct ValidationTask {
    pub address: String,
    pub nonce: u64,
    pub header_base: Vec<u8>,
    pub share_target: [u8; 32],
    pub network_target: [u8; 32],
    pub claimed_hash: Option<[u8; 32]>,
    pub force_full_verify: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ValidationResult {
    pub nonce: u64,
    pub accepted: bool,
    pub reject_reason: Option<&'static str>,
    pub hash: [u8; 32],
    pub verified: bool,
    pub is_block_candidate: bool,
    pub suspected_fraud: bool,
    pub escalate_risk: bool,
}

#[derive(Debug, Clone, Default)]
struct ValidationAddressState {
    total_shares: u64,
    sampled_shares: u64,
    invalid_samples: u64,
    forced_until: Option<Instant>,
    provisional_at: VecDeque<Instant>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ValidationSnapshot {
    pub in_flight: i64,
    pub candidate_queue_depth: usize,
    pub regular_queue_depth: usize,
    pub tracked_addresses: usize,
    pub forced_verify_addresses: usize,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub pending_provisional: u64,
    pub fraud_detections: u64,
}

struct QueuedTask {
    task: ValidationTask,
    result_tx: flume::Sender<ValidationResult>,
}

struct ValidationInner {
    config: Config,
    hasher: Arc<dyn PowHasher>,
    state: Mutex<HashMap<String, ValidationAddressState>>,
    rng: Mutex<StdRng>,
    in_flight: AtomicI64,
    fraud: AtomicU64,
}

pub struct ValidationEngine {
    inner: Arc<ValidationInner>,
    candidate_tx: flume::Sender<QueuedTask>,
    regular_tx: flume::Sender<QueuedTask>,
    shutdown: Arc<AtomicBool>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl ValidationEngine {
    pub fn new(config: Config, hasher: Arc<dyn PowHasher>) -> Self {
        let worker_count = if config.max_verifiers <= 0 {
            std::thread::available_parallelism()
                .map(|n| n.get().max(1) / 2)
                .unwrap_or(1)
                .max(1)
        } else {
            config.max_verifiers as usize
        };
        let queue_size = config.max_validation_queue.max(1) as usize;

        let (candidate_tx, candidate_rx) = flume::bounded::<QueuedTask>(queue_size);
        let (regular_tx, regular_rx) = flume::bounded::<QueuedTask>(queue_size);
        let shutdown = Arc::new(AtomicBool::new(false));

        let inner = Arc::new(ValidationInner {
            config,
            hasher,
            state: Mutex::new(HashMap::new()),
            rng: Mutex::new(StdRng::from_entropy()),
            in_flight: AtomicI64::new(0),
            fraud: AtomicU64::new(0),
        });

        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let inner_cloned = Arc::clone(&inner);
            let candidate_rx = candidate_rx.clone();
            let regular_rx = regular_rx.clone();
            let shutdown = Arc::clone(&shutdown);
            workers.push(thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    if let Ok(queued) = candidate_rx.try_recv() {
                        inner_cloned.process(queued);
                        continue;
                    }

                    match regular_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(queued) => inner_cloned.process(queued),
                        Err(flume::RecvTimeoutError::Timeout) => continue,
                        Err(flume::RecvTimeoutError::Disconnected) => {
                            if candidate_rx.is_disconnected() {
                                break;
                            }
                        }
                    }
                }
            }));
        }

        Self {
            inner,
            candidate_tx,
            regular_tx,
            shutdown,
            workers,
        }
    }

    pub fn submit(
        &self,
        task: ValidationTask,
        candidate: bool,
    ) -> Option<flume::Receiver<ValidationResult>> {
        let (tx, rx) = flume::bounded(1);
        let queued = QueuedTask {
            task,
            result_tx: tx,
        };

        let result = if candidate {
            self.candidate_tx.try_send(queued)
        } else {
            self.regular_tx.try_send(queued)
        };

        match result {
            Ok(()) => Some(rx),
            Err(flume::TrySendError::Full(_)) | Err(flume::TrySendError::Disconnected(_)) => None,
        }
    }

    pub fn process_inline(&self, task: ValidationTask) -> ValidationResult {
        let (tx, rx) = flume::bounded(1);
        self.inner.process(QueuedTask {
            task,
            result_tx: tx,
        });
        rx.recv().expect("inline validation should return result")
    }

    pub fn queue_depths(&self) -> (usize, usize) {
        (self.candidate_tx.len(), self.regular_tx.len())
    }

    pub fn snapshot(&self) -> ValidationSnapshot {
        self.inner
            .snapshot(self.candidate_tx.len(), self.regular_tx.len())
    }
}

impl Drop for ValidationEngine {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        while let Some(handle) = self.workers.pop() {
            let _ = handle.join();
        }
    }
}

impl ValidationInner {
    fn process(&self, queued: QueuedTask) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let result = self.process_task(&queued.task);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let _ = queued.result_tx.send(result);
    }

    fn process_task(&self, task: &ValidationTask) -> ValidationResult {
        let full_verify = task.force_full_verify || self.should_fully_verify(&task.address);

        let mut result = ValidationResult {
            nonce: task.nonce,
            accepted: false,
            reject_reason: None,
            hash: [0u8; 32],
            verified: full_verify,
            is_block_candidate: false,
            suspected_fraud: false,
            escalate_risk: false,
        };

        if full_verify {
            let hash = self.hasher.hash(&task.header_base, task.nonce);
            result.hash = hash;

            if let Some(claimed_hash) = task.claimed_hash {
                if claimed_hash != hash {
                    result.reject_reason = Some("invalid share proof");
                    result.suspected_fraud = true;
                    self.fraud.fetch_add(1, Ordering::Relaxed);
                } else if !check_target(hash, task.share_target) {
                    result.reject_reason = Some("low difficulty share");
                } else {
                    result.accepted = true;
                    result.is_block_candidate = check_target(hash, task.network_target);
                }
            } else if !check_target(hash, task.share_target) {
                result.reject_reason = Some("low difficulty share");
            } else {
                result.accepted = true;
                result.is_block_candidate = check_target(hash, task.network_target);
            }
        } else {
            match task.claimed_hash {
                None => {
                    result.reject_reason = Some("claimed hash required");
                }
                Some(claimed_hash) if !check_target(claimed_hash, task.share_target) => {
                    result.reject_reason = Some("low difficulty share");
                }
                Some(claimed_hash) => {
                    result.accepted = true;
                    result.hash = claimed_hash;
                    result.is_block_candidate = check_target(claimed_hash, task.network_target);
                }
            }
        }

        let invalid_sample = full_verify && !result.accepted;
        let provisional_accepted = result.accepted && !full_verify;
        if self.update_address_state(
            &task.address,
            full_verify,
            invalid_sample,
            result.suspected_fraud,
            provisional_accepted,
        ) {
            result.escalate_risk = true;
        }

        result
    }

    fn should_fully_verify(&self, address: &str) -> bool {
        if self.config.validation_mode.eq_ignore_ascii_case("full") {
            return true;
        }

        let now = Instant::now();

        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);
        self.prune_provisional_locked(st, now);

        if self.config.max_provisional_shares > 0
            && st.provisional_at.len() >= self.config.max_provisional_shares as usize
        {
            st.forced_until = Some(now + self.config.provisional_share_delay_duration());
        }

        if st.forced_until.is_some_and(|deadline| now < deadline) {
            return true;
        }

        let next_share_idx = st.total_shares + 1;
        if self.config.warmup_shares > 0 && next_share_idx <= self.config.warmup_shares as u64 {
            return true;
        }
        if self.config.min_sample_every > 0
            && next_share_idx.is_multiple_of(self.config.min_sample_every as u64)
        {
            return true;
        }

        let rate = self.config.sample_rate;
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }

        self.rng.lock().gen::<f64>() < rate
    }

    fn update_address_state(
        &self,
        address: &str,
        sampled: bool,
        invalid_sample: bool,
        suspected_fraud: bool,
        provisional_accepted: bool,
    ) -> bool {
        let now = Instant::now();
        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);

        st.total_shares = st.total_shares.saturating_add(1);
        self.prune_provisional_locked(st, now);
        if provisional_accepted {
            st.provisional_at.push_back(now);
        }
        if sampled {
            st.sampled_shares = st.sampled_shares.saturating_add(1);
            if invalid_sample {
                st.invalid_samples = st.invalid_samples.saturating_add(1);
            }
        }

        if suspected_fraud {
            st.forced_until = Some(now + self.config.forced_verify_duration());
            return true;
        }

        let min_samples = self.config.invalid_sample_min.max(1) as u64;
        if st.sampled_shares < min_samples {
            return false;
        }

        let invalid_ratio = if st.sampled_shares == 0 {
            0.0
        } else {
            st.invalid_samples as f64 / st.sampled_shares as f64
        };

        if invalid_ratio > self.config.invalid_sample_threshold {
            st.forced_until = Some(now + self.config.forced_verify_duration());
            return true;
        }

        false
    }

    fn prune_provisional_locked(&self, st: &mut ValidationAddressState, now: Instant) {
        if st.provisional_at.is_empty() {
            return;
        }

        let delay = self.config.provisional_share_delay_duration();
        if delay.is_zero() {
            st.provisional_at.clear();
            return;
        }

        let cutoff = now.checked_sub(delay).unwrap_or(now);
        while st
            .provisional_at
            .front()
            .is_some_and(|timestamp| *timestamp <= cutoff)
        {
            st.provisional_at.pop_front();
        }
    }

    fn snapshot(&self, candidate_depth: usize, regular_depth: usize) -> ValidationSnapshot {
        let now = Instant::now();
        let mut snap = ValidationSnapshot {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            candidate_queue_depth: candidate_depth,
            regular_queue_depth: regular_depth,
            fraud_detections: self.fraud.load(Ordering::Relaxed),
            ..ValidationSnapshot::default()
        };

        let mut state = self.state.lock();
        snap.tracked_addresses = state.len();
        for st in state.values_mut() {
            self.prune_provisional_locked(st, now);
            snap.total_shares = snap.total_shares.saturating_add(st.total_shares);
            snap.sampled_shares = snap.sampled_shares.saturating_add(st.sampled_shares);
            snap.invalid_samples = snap.invalid_samples.saturating_add(st.invalid_samples);
            snap.pending_provisional = snap
                .pending_provisional
                .saturating_add(st.provisional_at.len() as u64);
            if st.forced_until.is_some_and(|deadline| now < deadline) {
                snap.forced_verify_addresses += 1;
            }
        }

        snap
    }
}

fn get_or_insert_state<'a>(
    state: &'a mut HashMap<String, ValidationAddressState>,
    address: &str,
) -> &'a mut ValidationAddressState {
    if !state.contains_key(address) {
        state.insert(address.to_string(), ValidationAddressState::default());
    }
    state
        .get_mut(address)
        .expect("address state must be present after insert")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pow::{difficulty_to_target, DeterministicTestHasher};

    fn test_cfg() -> Config {
        Config {
            validation_mode: "probabilistic".to_string(),
            sample_rate: 0.0,
            warmup_shares: 0,
            min_sample_every: 0,
            invalid_sample_min: 1,
            invalid_sample_threshold: 0.01,
            max_verifiers: 1,
            max_validation_queue: 16,
            max_provisional_shares: 2,
            provisional_share_delay: "10m".to_string(),
            forced_verify_duration: "1h".to_string(),
            ..Config::default()
        }
    }

    fn base_task() -> ValidationTask {
        ValidationTask {
            address: "addr1".to_string(),
            nonce: 1,
            header_base: vec![1, 2, 3],
            share_target: [0xFF; 32],
            network_target: [0x0F; 32],
            claimed_hash: Some([0x01; 32]),
            force_full_verify: false,
        }
    }

    #[test]
    fn invalid_sample_escalates_force_verify() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.force_full_verify = true;
        task.claimed_hash = Some([0xAA; 32]);
        let result = engine.process_inline(task.clone());
        assert!(!result.accepted);
        assert!(result.suspected_fraud);
        assert!(result.escalate_risk);

        task.force_full_verify = false;
        task.claimed_hash = Some([0x01; 32]);
        let result2 = engine.process_inline(task);
        assert!(
            result2.verified,
            "subsequent shares should be force-verified"
        );
    }

    #[test]
    fn provisional_cap_forces_full_verify() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = Some([0x01; 32]);

        let r1 = engine.process_inline(task.clone());
        assert!(r1.accepted);
        assert!(!r1.verified);

        let r2 = engine.process_inline(task.clone());
        assert!(r2.accepted);
        assert!(!r2.verified);

        let r3 = engine.process_inline(task);
        assert!(r3.verified, "provisional cap should force full verify");
    }

    #[test]
    fn full_mode_always_verifies() {
        let mut cfg = test_cfg();
        cfg.validation_mode = "full".to_string();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = None;
        task.share_target = difficulty_to_target(1);

        let result = engine.process_inline(task);
        assert!(result.verified);
    }

    #[test]
    fn missing_claimed_hash_rejected_when_not_full_verified() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = None;

        let result = engine.process_inline(task);
        assert!(!result.accepted);
        assert_eq!(result.reject_reason, Some("claimed hash required"));
    }

    #[test]
    fn queue_submit_returns_none_when_full() {
        let mut cfg = test_cfg();
        cfg.max_validation_queue = 1;

        struct SlowHasher;
        impl PowHasher for SlowHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> [u8; 32] {
                std::thread::sleep(Duration::from_millis(150));
                [0x01; 32]
            }
        }

        let engine = ValidationEngine::new(cfg, Arc::new(SlowHasher));
        let mut task = base_task();
        task.force_full_verify = true;
        task.claimed_hash = Some([0x01; 32]);

        let r1 = engine.submit(task.clone(), false);
        assert!(r1.is_some());
        let r2 = engine.submit(task.clone(), false);
        assert!(r2.is_none(), "second submit should fail when queue is full");
    }

    #[test]
    fn candidate_queue_is_prioritized_after_current_task() {
        let mut cfg = test_cfg();
        cfg.validation_mode = "full".to_string();

        struct SleepyHasher;
        impl PowHasher for SleepyHasher {
            fn hash(&self, _header_base: &[u8], nonce: u64) -> [u8; 32] {
                std::thread::sleep(Duration::from_millis(60));
                let mut out = [0u8; 32];
                out[31] = nonce as u8;
                out
            }
        }

        let engine = ValidationEngine::new(cfg, Arc::new(SleepyHasher));

        let mut regular1 = base_task();
        regular1.nonce = 1;
        regular1.claimed_hash = Some([0u8; 32]);
        regular1.force_full_verify = true;

        let mut regular2 = base_task();
        regular2.nonce = 2;
        regular2.claimed_hash = Some([0u8; 32]);
        regular2.force_full_verify = true;

        let mut candidate = base_task();
        candidate.nonce = 3;
        candidate.claimed_hash = Some([0u8; 32]);
        candidate.force_full_verify = true;

        let rx1 = engine.submit(regular1, false).expect("queue regular1");
        let rx2 = engine.submit(regular2, false).expect("queue regular2");
        std::thread::sleep(Duration::from_millis(5));
        let rxc = engine.submit(candidate, true).expect("queue candidate");

        let first = rx1
            .recv_timeout(Duration::from_secs(1))
            .expect("first result");
        assert_eq!(first.nonce, 1);

        let second_candidate = rxc
            .recv_timeout(Duration::from_secs(1))
            .expect("candidate should complete second");
        assert_eq!(second_candidate.nonce, 3);

        let third = rx2
            .recv_timeout(Duration::from_secs(1))
            .expect("third result");
        assert_eq!(third.nonce, 2);
    }

    #[test]
    fn snapshot_tracks_state() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = Some([0x01; 32]);
        let _ = engine.process_inline(task);

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.tracked_addresses, 1);
        assert_eq!(snapshot.total_shares, 1);
    }
}