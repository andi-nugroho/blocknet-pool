use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::db::{DbBlock, DbShare, PendingPayout};
use crate::node::{is_http_status, NodeClient};
use crate::store::PoolStore;
use crate::validation::{SHARE_STATUS_PROVISIONAL, SHARE_STATUS_VERIFIED};

#[derive(Debug)]
pub struct PayoutProcessor {
    cfg: Config,
    store: Arc<PoolStore>,
    node: Arc<NodeClient>,
}

impl PayoutProcessor {
    pub fn new(cfg: Config, store: Arc<PoolStore>, node: Arc<NodeClient>) -> Arc<Self> {
        Arc::new(Self { cfg, store, node })
    }

    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            {
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.recover_pending_payouts();
                    this.tick();
                })
                .await;
            }

            let mut ticker = tokio::time::interval(this.cfg.payout_interval_duration());
            loop {
                ticker.tick().await;
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.tick();
                })
                .await;
            }
        });
    }

    fn tick(&self) {
        if let Err(err) = self.node.get_status() {
            tracing::warn!(error = %err, "cannot reach node for payouts");
            return;
        }

        self.confirm_blocks();
        self.distribute_rewards();
        self.send_payouts();
    }

    fn confirm_blocks(&self) {
        let blocks = match self.store.get_unconfirmed_blocks() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read unconfirmed blocks");
                return;
            }
        };

        let current_height = self.node.chain_height();

        for mut block in blocks {
            if current_height < block.height
                || current_height - block.height < self.cfg.blocks_before_payout.max(0) as u64
            {
                continue;
            }

            let node_block = match self.node.get_block(&block.height.to_string()) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(height = block.height, error = %err, "failed to verify block");
                    continue;
                }
            };

            if node_block.hash != block.hash {
                let grace = 6u64;
                if current_height - block.height
                    < self.cfg.blocks_before_payout.max(0) as u64 + grace
                {
                    continue;
                }

                block.orphaned = true;
                if let Err(err) = self.store.update_block(&block) {
                    tracing::warn!(height = block.height, error = %err, "failed to mark orphan block");
                }
                continue;
            }

            block.confirmed = true;
            block.reward = node_block.reward;
            if let Err(err) = self.store.update_block(&block) {
                tracing::warn!(height = block.height, error = %err, "failed to update confirmed block");
            }
        }
    }

    fn distribute_rewards(&self) {
        let blocks = match self.store.get_unpaid_blocks() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read unpaid blocks");
                return;
            }
        };

        for mut block in blocks {
            if block.reward == 0 {
                continue;
            }

            let fee = self.cfg.pool_fee(block.reward);
            let distributable = block.reward.saturating_sub(fee);
            if fee > 0 {
                if let Some(fee_address) = self.resolve_pool_fee_address(&block) {
                    if let Err(err) =
                        self.store
                            .record_pool_fee(block.height, fee, &fee_address, block.timestamp)
                    {
                        tracing::warn!(
                            height = block.height,
                            fee,
                            fee_address = %fee_address,
                            error = %err,
                            "failed to record pool fee"
                        );
                    }
                } else {
                    tracing::warn!(
                        height = block.height,
                        fee,
                        "pool fee applied without an explicit fee destination"
                    );
                }
            }

            let result = if self.cfg.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
                self.distribute_pplns(&block, distributable)
            } else {
                self.distribute_proportional(&block, distributable)
            };

            if let Err(err) = result {
                tracing::warn!(height = block.height, error = %err, "failed reward distribution");
                continue;
            }

            block.paid_out = true;
            if let Err(err) = self.store.update_block(&block) {
                tracing::warn!(height = block.height, error = %err, "failed marking block paid");
            }
        }
    }

    fn distribute_pplns(&self, block: &DbBlock, reward: u64) -> anyhow::Result<()> {
        let shares = if self.cfg.pplns_window_duration_duration().is_zero() {
            self.store
                .get_last_n_shares(self.cfg.pplns_window.max(1) as i64)?
        } else {
            let since = SystemTime::now() - self.cfg.pplns_window_duration_duration();
            self.store.get_shares_since(since)?
        };

        if shares.is_empty() {
            self.store.credit_balance(&block.finder, reward)?;
            return Ok(());
        }

        let (weights, total_weight) = weight_shares(&shares);
        if total_weight == 0 {
            self.store.credit_balance(&block.finder, reward)?;
            return Ok(());
        }

        let mut distributable = reward;
        if self.cfg.block_finder_bonus && self.cfg.block_finder_bonus_pct > 0.0 {
            let bonus = (reward as f64 * self.cfg.block_finder_bonus_pct / 100.0) as u64;
            self.store.credit_balance(&block.finder, bonus)?;
            distributable = distributable.saturating_sub(bonus);
        }

        self.credit_miners(weights, total_weight, distributable)?;
        Ok(())
    }

    fn distribute_proportional(&self, block: &DbBlock, reward: u64) -> anyhow::Result<()> {
        let shares = self
            .store
            .get_shares_since(block.timestamp - Duration::from_secs(60 * 60))?;
        if shares.is_empty() {
            self.store.credit_balance(&block.finder, reward)?;
            return Ok(());
        }

        let (weights, total_weight) = weight_shares(&shares);
        if total_weight == 0 {
            self.store.credit_balance(&block.finder, reward)?;
            return Ok(());
        }

        let mut distributable = reward;
        if self.cfg.block_finder_bonus && self.cfg.block_finder_bonus_pct > 0.0 {
            let bonus = (reward as f64 * self.cfg.block_finder_bonus_pct / 100.0) as u64;
            self.store.credit_balance(&block.finder, bonus)?;
            distributable = distributable.saturating_sub(bonus);
        }

        self.credit_miners(weights, total_weight, distributable)?;
        Ok(())
    }

    fn credit_miners(
        &self,
        weights: HashMap<String, u64>,
        total_weight: u64,
        amount: u64,
    ) -> anyhow::Result<u64> {
        let mut credited = 0u64;
        for (address, weight) in weights {
            let share = ((amount as f64) * (weight as f64) / (total_weight as f64)) as u64;
            if share == 0 {
                continue;
            }
            self.store.credit_balance(&address, share)?;
            credited = credited.saturating_add(share);
        }
        Ok(credited)
    }

    fn recover_pending_payouts(&self) {
        let pending = match self.store.get_pending_payouts() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read pending payouts");
                return;
            }
        };

        for entry in pending {
            let age = SystemTime::now()
                .duration_since(entry.initiated_at)
                .unwrap_or_default();
            if age > Duration::from_secs(60 * 60) {
                let _ = self.store.cancel_pending_payout(&entry.address);
            }
        }
    }

    fn send_payouts(&self) {
        if !self.ensure_wallet_ready() {
            return;
        }

        let min_amount = (self.cfg.min_payout_amount * 100_000_000.0) as u64;
        let balances = match self.store.get_all_balances() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to list balances");
                return;
            }
        };

        for bal in balances {
            if bal.pending < min_amount {
                continue;
            }

            let wallet_balance = match self.node.get_wallet_balance() {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(error = %err, "failed wallet balance check");
                    return;
                }
            };
            if wallet_balance.spendable < bal.pending {
                continue;
            }

            let pending = match self.store.get_pending_payout(&bal.address) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(address = %bal.address, error = %err, "failed pending payout query");
                    continue;
                }
            };

            let pending = match pending {
                Some(v) => v,
                None => {
                    if let Err(err) = self.store.create_pending_payout(&bal.address, bal.pending) {
                        tracing::warn!(address = %bal.address, error = %err, "failed create pending payout");
                        continue;
                    }
                    match self.store.get_pending_payout(&bal.address) {
                        Ok(Some(v)) => v,
                        _ => continue,
                    }
                }
            };

            if pending.amount != bal.pending {
                continue;
            }

            let idempotency_key = payout_idempotency_key(&pending);
            let send = self
                .node
                .wallet_send(&bal.address, bal.pending, &idempotency_key);
            let sent = match send {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(address = %bal.address, error = %err, "wallet send failed");
                    continue;
                }
            };

            if let Err(err) =
                self.store
                    .complete_pending_payout(&bal.address, bal.pending, &sent.txid)
            {
                tracing::error!(address = %bal.address, tx = %sent.txid, error = %err, "critical payout reconciliation failure");
                continue;
            }
        }
    }

    fn ensure_wallet_ready(&self) -> bool {
        if self.node.get_wallet_balance().is_ok() {
            return self.validate_daemon_wallet_address();
        }

        let password = std::env::var("BLOCKNET_WALLET_PASSWORD")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let Some(password) = password else {
            return false;
        };

        let probe = match self.node.get_wallet_balance() {
            Ok(_) => return self.validate_daemon_wallet_address(),
            Err(e) => e,
        };

        if is_http_status(&probe, 403) {
            if self.node.wallet_unlock(&password).is_err() {
                return false;
            }
        } else if is_http_status(&probe, 503) {
            if self.node.wallet_load(&password).is_err() {
                return false;
            }
            if self.node.wallet_unlock(&password).is_err() {
                return false;
            }
        } else {
            return false;
        }

        self.node.get_wallet_balance().is_ok() && self.validate_daemon_wallet_address()
    }

    fn validate_daemon_wallet_address(&self) -> bool {
        match self.node.get_wallet_address() {
            Ok(addr) => !addr.address.trim().is_empty() && !addr.view_only,
            Err(_) => false,
        }
    }

    fn resolve_pool_fee_address(&self, block: &DbBlock) -> Option<String> {
        let configured = self.cfg.pool_wallet_address.trim();
        if !configured.is_empty() {
            return Some(configured.to_string());
        }

        let finder = block.finder.trim();
        if !finder.is_empty() {
            return Some(finder.to_string());
        }
        None
    }
}

pub fn weight_shares(shares: &[DbShare]) -> (HashMap<String, u64>, u64) {
    let mut weights = HashMap::<String, u64>::new();
    let mut total = 0u64;

    let now = SystemTime::now();
    for share in shares {
        if !is_share_payout_eligible(share, now) {
            continue;
        }
        let weight = share.difficulty;
        let entry = weights.entry(share.miner.clone()).or_default();
        *entry = entry.saturating_add(weight);
        total = total.saturating_add(weight);
    }

    (weights, total)
}

pub fn is_share_payout_eligible(share: &DbShare, now: SystemTime) -> bool {
    match share.status.as_str() {
        "" | SHARE_STATUS_VERIFIED => true,
        SHARE_STATUS_PROVISIONAL => {
            // Current DB schema doesn't store eligible_at separately yet; provisional shares
            // become eligible only if they survived delay and were retained by validation logic.
            let _ = now;
            false
        }
        _ => false,
    }
}

fn payout_idempotency_key(p: &PendingPayout) -> String {
    use sha2::{Digest, Sha256};

    let ts = p
        .initiated_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let input = format!("blocknet-pool:payout:{}:{}:{}", p.address, p.amount, ts);
    let digest = Sha256::digest(input.as_bytes());
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbShare;

    #[test]
    fn payout_weighting_ignores_provisional() {
        let shares = vec![
            DbShare {
                id: 1,
                job_id: "j1".into(),
                miner: "a1".into(),
                worker: "w1".into(),
                difficulty: 10,
                nonce: 1,
                status: SHARE_STATUS_VERIFIED.into(),
                was_sampled: true,
                block_hash: None,
                created_at: SystemTime::now(),
            },
            DbShare {
                id: 2,
                job_id: "j2".into(),
                miner: "a2".into(),
                worker: "w2".into(),
                difficulty: 100,
                nonce: 2,
                status: SHARE_STATUS_PROVISIONAL.into(),
                was_sampled: false,
                block_hash: None,
                created_at: SystemTime::now(),
            },
        ];

        let (weights, total) = weight_shares(&shares);
        assert_eq!(total, 10);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert!(!weights.contains_key("a2"));
    }
}