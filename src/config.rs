use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub pool_name: String,
    pub pool_url: String,

    pub stratum_port: u16,
    pub api_port: u16,
    pub api_host: String,

    pub daemon_binary: String,
    pub daemon_data_dir: String,
    pub daemon_api: String,
    pub daemon_token: String,
    pub pool_wallet_address: String,

    pub initial_share_difficulty: u64,
    pub block_poll_interval: String,
    pub job_timeout: String,
    pub validation_mode: String,
    pub max_verifiers: i32,
    pub max_validation_queue: i32,
    pub sample_rate: f64,
    pub warmup_shares: i32,
    pub min_sample_every: i32,
    pub invalid_sample_threshold: f64,
    pub invalid_sample_min: i32,
    pub forced_verify_duration: String,
    pub quarantine_duration: String,
    pub max_quarantine_duration: String,
    pub provisional_share_delay: String,
    pub max_provisional_shares: i32,
    pub stratum_submit_v2_required: bool,

    pub pool_fee_flat: f64,
    pub pool_fee_pct: f64,

    pub payout_scheme: String,
    pub pplns_window: i32,
    pub pplns_window_duration: String,
    pub blocks_before_payout: i32,
    pub min_payout_amount: f64,
    pub block_finder_bonus: bool,
    pub block_finder_bonus_pct: f64,
    pub payout_interval: String,

    pub database_path: String,
    #[serde(default)]
    pub database_url: String,
    pub api_key: String,

    #[serde(skip)]
    pub log_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pool_name: "blocknet pool".to_string(),
            pool_url: "http://localhost:24783".to_string(),
            stratum_port: 3333,
            api_port: 24783,
            api_host: "0.0.0.0".to_string(),
            daemon_binary: "./blocknet".to_string(),
            daemon_data_dir: "data".to_string(),
            daemon_api: "http://127.0.0.1:8332".to_string(),
            daemon_token: String::new(),
            pool_wallet_address: String::new(),
            initial_share_difficulty: 1,
            block_poll_interval: "2s".to_string(),
            job_timeout: "5m".to_string(),
            validation_mode: "probabilistic".to_string(),
            max_verifiers: 2,
            max_validation_queue: 2048,
            sample_rate: 0.05,
            warmup_shares: 50,
            min_sample_every: 20,
            invalid_sample_threshold: 0.01,
            invalid_sample_min: 50,
            forced_verify_duration: "24h".to_string(),
            quarantine_duration: "1h".to_string(),
            max_quarantine_duration: "168h".to_string(),
            provisional_share_delay: "15m".to_string(),
            max_provisional_shares: 200,
            stratum_submit_v2_required: false,
            pool_fee_flat: 0.0,
            pool_fee_pct: 0.0,
            payout_scheme: "pplns".to_string(),
            pplns_window: 1000,
            pplns_window_duration: "24h".to_string(),
            blocks_before_payout: 120,
            min_payout_amount: 0.1,
            block_finder_bonus: false,
            block_finder_bonus_pct: 5.0,
            payout_interval: "1h".to_string(),
            database_path: "pool.db".to_string(),
            database_url: String::new(),
            api_key: String::new(),
            log_path: String::new(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("read config {}", path.display()))?;
        let mut cfg: Config = serde_json::from_slice(&data)
            .with_context(|| format!("parse config {}", path.display()))?;
        cfg.normalize();
        Ok(cfg)
    }

    pub fn write_default(path: &Path) -> Result<()> {
        let cfg = Config::default();
        let data = serde_json::to_vec_pretty(&cfg).context("serialize default config")?;
        fs::write(path, data).with_context(|| format!("write config {}", path.display()))
    }

    pub fn normalize(&mut self) {
        let mode = self.validation_mode.trim().to_ascii_lowercase();
        self.validation_mode = match mode.as_str() {
            "full" | "probabilistic" => mode,
            _ => "probabilistic".to_string(),
        };

        if self.max_verifiers < 0 {
            self.max_verifiers = 0;
        }
        if self.max_validation_queue < 1 {
            self.max_validation_queue = 2048;
        }
        self.sample_rate = self.sample_rate.clamp(0.0, 1.0);
        if self.warmup_shares < 0 {
            self.warmup_shares = 0;
        }
        if self.min_sample_every < 0 {
            self.min_sample_every = 0;
        }
        if self.invalid_sample_min < 1 {
            self.invalid_sample_min = 1;
        }
        if !(0.0 < self.invalid_sample_threshold && self.invalid_sample_threshold <= 1.0) {
            self.invalid_sample_threshold = 0.01;
        }
        if self.max_provisional_shares < 0 {
            self.max_provisional_shares = 0;
        }
        if self.initial_share_difficulty < 1 {
            self.initial_share_difficulty = 1;
        }
    }

    pub fn block_poll_duration(&self) -> Duration {
        parse_duration_or(&self.block_poll_interval, Duration::from_secs(2))
    }

    pub fn job_timeout_duration(&self) -> Duration {
        parse_duration_or(&self.job_timeout, Duration::from_secs(5 * 60))
    }

    pub fn forced_verify_duration(&self) -> Duration {
        parse_duration_or(
            &self.forced_verify_duration,
            Duration::from_secs(24 * 60 * 60),
        )
    }

    pub fn provisional_share_delay_duration(&self) -> Duration {
        parse_duration_or(&self.provisional_share_delay, Duration::from_secs(15 * 60))
    }

    pub fn quarantine_duration_duration(&self) -> Duration {
        parse_duration_or(&self.quarantine_duration, Duration::from_secs(60 * 60))
    }

    pub fn max_quarantine_duration_duration(&self) -> Duration {
        parse_duration_or(
            &self.max_quarantine_duration,
            Duration::from_secs(168 * 60 * 60),
        )
    }

    pub fn payout_interval_duration(&self) -> Duration {
        parse_duration_or(&self.payout_interval, Duration::from_secs(60 * 60))
    }

    pub fn pplns_window_duration_duration(&self) -> Duration {
        if self.pplns_window_duration.trim().is_empty() {
            return Duration::from_secs(0);
        }
        parse_duration_or(
            &self.pplns_window_duration,
            Duration::from_secs(24 * 60 * 60),
        )
    }

    pub fn pool_fee(&self, reward: u64) -> u64 {
        let mut fee = 0u64;

        if self.pool_fee_flat > 0.0 {
            let flat = (self.pool_fee_flat * 100_000_000.0) as u64;
            if flat > reward {
                return reward;
            }
            fee = fee.saturating_add(flat);
        }

        if self.pool_fee_pct > 0.0 {
            let remainder = reward.saturating_sub(fee);
            let pct = ((remainder as f64) * self.pool_fee_pct / 100.0) as u64;
            fee = fee.saturating_add(pct);
        }

        fee.min(reward)
    }
}

fn parse_duration_or(value: &str, fallback: Duration) -> Duration {
    humantime::parse_duration(value).unwrap_or(fallback)
}

pub fn generate_default_env(path: &Path) -> Result<bool> {
    if path.exists() {
        return Ok(false);
    }
    let template = "# Blocknet pool runtime secrets\n# REQUIRED for wallet unlock/load operations\nBLOCKNET_WALLET_PASSWORD=\n";
    fs::write(path, template).with_context(|| format!("write {}", path.display()))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_clamps_values() {
        let mut cfg = Config {
            validation_mode: "invalid".to_string(),
            max_verifiers: -1,
            max_validation_queue: 0,
            sample_rate: 2.0,
            warmup_shares: -5,
            min_sample_every: -1,
            invalid_sample_min: 0,
            invalid_sample_threshold: 2.0,
            max_provisional_shares: -1,
            initial_share_difficulty: 0,
            ..Config::default()
        };
        cfg.normalize();

        assert_eq!(cfg.validation_mode, "probabilistic");
        assert_eq!(cfg.max_verifiers, 0);
        assert_eq!(cfg.max_validation_queue, 2048);
        assert_eq!(cfg.sample_rate, 1.0);
        assert_eq!(cfg.warmup_shares, 0);
        assert_eq!(cfg.min_sample_every, 0);
        assert_eq!(cfg.invalid_sample_min, 1);
        assert_eq!(cfg.invalid_sample_threshold, 0.01);
        assert_eq!(cfg.max_provisional_shares, 0);
        assert_eq!(cfg.initial_share_difficulty, 1);
    }

    #[test]
    fn fee_applies_flat_then_pct() {
        let cfg = Config {
            pool_fee_flat: 1.0,
            pool_fee_pct: 10.0,
            ..Config::default()
        };
        let reward = 10_000_000_000u64;
        let fee = cfg.pool_fee(reward);
        assert!(fee > 100_000_000);
        assert!(fee < reward);
    }

    #[test]
    fn writes_env_template_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env_path = dir.path().join(".env");

        assert!(generate_default_env(&env_path).expect("create env"));
        assert!(!generate_default_env(&env_path).expect("second create should be false"));
    }
}