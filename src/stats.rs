use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct MinerStats {
    pub address: String,
    pub workers: HashSet<String>,
    pub shares_accepted: u64,
    pub shares_rejected: u64,
    pub blocks_found: u64,
    pub last_share_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct ShareRecord {
    miner: String,
    difficulty: u64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct ConnectedWorker {
    address: String,
    worker: String,
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub total_shares_accepted: u64,
    pub total_shares_rejected: u64,
    pub total_blocks_found: u64,
    pub connected_miners: usize,
    pub connected_workers: usize,
    pub estimated_hashrate: f64,
}

#[derive(Debug, Default)]
pub struct PoolStats {
    total_shares_accepted: AtomicU64,
    total_shares_rejected: AtomicU64,
    total_blocks_found: AtomicU64,

    miner_stats: RwLock<HashMap<String, MinerStats>>,
    connected_miners: RwLock<HashMap<String, ConnectedWorker>>, // conn_id -> active miner/worker
    recent_shares: RwLock<VecDeque<ShareRecord>>,
}

impl PoolStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_miner(&self, conn_id: &str, address: &str, worker: &str) {
        self.connected_miners.write().insert(
            conn_id.to_string(),
            ConnectedWorker {
                address: address.to_string(),
                worker: worker.to_string(),
            },
        );

        let mut miners = self.miner_stats.write();
        let entry = miners
            .entry(address.to_string())
            .or_insert_with(|| MinerStats {
                address: address.to_string(),
                workers: HashSet::new(),
                shares_accepted: 0,
                shares_rejected: 0,
                blocks_found: 0,
                last_share_at: None,
            });
        entry.workers.insert(worker.to_string());
    }

    pub fn remove_miner(&self, conn_id: &str) {
        self.connected_miners.write().remove(conn_id);
    }

    pub fn record_accepted_share(&self, address: &str, difficulty: u64) {
        self.total_shares_accepted.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now();

        {
            let mut miners = self.miner_stats.write();
            let entry = miners
                .entry(address.to_string())
                .or_insert_with(|| MinerStats {
                    address: address.to_string(),
                    workers: HashSet::new(),
                    shares_accepted: 0,
                    shares_rejected: 0,
                    blocks_found: 0,
                    last_share_at: None,
                });
            entry.shares_accepted = entry.shares_accepted.saturating_add(1);
            entry.last_share_at = Some(now);
        }

        let mut recent = self.recent_shares.write();
        recent.push_back(ShareRecord {
            miner: address.to_string(),
            difficulty,
            timestamp: now,
        });

        let cutoff = now
            .checked_sub(Duration::from_secs(60 * 60))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        while recent
            .front()
            .is_some_and(|share| share.timestamp <= cutoff)
        {
            recent.pop_front();
        }
    }

    pub fn record_rejected_share(&self, address: &str) {
        self.total_shares_rejected.fetch_add(1, Ordering::Relaxed);
        if let Some(entry) = self.miner_stats.write().get_mut(address) {
            entry.shares_rejected = entry.shares_rejected.saturating_add(1);
        }
    }

    pub fn record_block_found(&self, address: &str) {
        self.total_blocks_found.fetch_add(1, Ordering::Relaxed);
        if let Some(entry) = self.miner_stats.write().get_mut(address) {
            entry.blocks_found = entry.blocks_found.saturating_add(1);
        }
    }

    pub fn estimate_hashrate(&self) -> f64 {
        let recent = self.recent_shares.read();
        if recent.len() < 2 {
            return 0.0;
        }

        let oldest = recent
            .front()
            .map(|r| r.timestamp)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let newest = recent
            .back()
            .map(|r| r.timestamp)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let Ok(window) = newest.duration_since(oldest) else {
            return 0.0;
        };
        if window.as_secs_f64() < 1.0 {
            return 0.0;
        }

        let total_diff = recent
            .iter()
            .fold(0u64, |acc, r| acc.saturating_add(r.difficulty));
        total_diff as f64 / window.as_secs_f64()
    }

    pub fn estimate_miner_hashrate(&self, address: &str) -> f64 {
        let recent = self.recent_shares.read();
        let mut first = None;
        let mut last = None;
        let mut total_diff = 0u64;
        let mut count = 0usize;

        for share in recent.iter().filter(|s| s.miner == address) {
            if first.is_none() {
                first = Some(share.timestamp);
            }
            last = Some(share.timestamp);
            total_diff = total_diff.saturating_add(share.difficulty);
            count += 1;
        }

        if count < 2 {
            return 0.0;
        }

        let oldest = first.unwrap_or(SystemTime::UNIX_EPOCH);
        let newest = last.unwrap_or(SystemTime::UNIX_EPOCH);
        let Ok(window) = newest.duration_since(oldest) else {
            return 0.0;
        };
        if window.as_secs_f64() < 1.0 {
            return 0.0;
        }

        total_diff as f64 / window.as_secs_f64()
    }

    pub fn connected_miner_count(&self) -> usize {
        self.connected_miners
            .read()
            .values()
            .map(|entry| entry.address.as_str())
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn connected_worker_count(&self) -> usize {
        self.connected_miners
            .read()
            .values()
            .map(|entry| (entry.address.as_str(), entry.worker.as_str()))
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn get_miner_stats(&self, address: &str) -> Option<MinerStats> {
        self.miner_stats.read().get(address).cloned()
    }

    pub fn all_miner_stats(&self) -> HashMap<String, MinerStats> {
        self.miner_stats.read().clone()
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            total_shares_accepted: self.total_shares_accepted.load(Ordering::Relaxed),
            total_shares_rejected: self.total_shares_rejected.load(Ordering::Relaxed),
            total_blocks_found: self.total_blocks_found.load(Ordering::Relaxed),
            connected_miners: self.connected_miner_count(),
            connected_workers: self.connected_worker_count(),
            estimated_hashrate: self.estimate_hashrate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PoolStats;

    #[test]
    fn records_shares_and_estimates_hashrate() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");
        stats.record_accepted_share("addr1", 10);
        stats.record_accepted_share("addr1", 20);

        assert_eq!(stats.connected_miner_count(), 1);
        assert_eq!(stats.connected_worker_count(), 1);
        assert!(stats.snapshot().total_shares_accepted >= 2);
    }

    #[test]
    fn connected_counts_track_active_connections() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");
        stats.add_miner("c2", "addr1", "rig1");
        stats.add_miner("c3", "addr2", "rig9");

        assert_eq!(stats.connected_miner_count(), 2);
        assert_eq!(stats.connected_worker_count(), 2);

        stats.remove_miner("c1");
        assert_eq!(stats.connected_miner_count(), 2);
        assert_eq!(stats.connected_worker_count(), 2);

        stats.remove_miner("c2");
        assert_eq!(stats.connected_miner_count(), 1);
        assert_eq!(stats.connected_worker_count(), 1);

        stats.remove_miner("c3");
        assert_eq!(stats.connected_miner_count(), 0);
        assert_eq!(stats.connected_worker_count(), 0);
    }
}