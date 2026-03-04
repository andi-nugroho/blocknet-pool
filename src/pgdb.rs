use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use postgres::{Client, NoTls};

use crate::db::{AddressRiskState, Balance, DbBlock, DbShare, Payout, PendingPayout, PoolFeeEvent};
use crate::engine::{ShareRecord, ShareStore};

const SEEN_SHARE_EXPIRY_SECS: i64 = 24 * 60 * 60;

pub struct PostgresStore {
    conn: Mutex<ManuallyDrop<Client>>,
}

impl PostgresStore {
    pub fn connect(url: &str) -> Result<Arc<Self>> {
        let mut conn =
            Client::connect(url, NoTls).with_context(|| format!("connect postgres {url}"))?;
        conn.batch_execute(
            r#"
CREATE TABLE IF NOT EXISTS shares (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    miner TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    nonce BIGINT NOT NULL,
    status TEXT NOT NULL,
    was_sampled BOOLEAN NOT NULL,
    block_hash TEXT,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_created ON shares(miner, created_at DESC);

CREATE TABLE IF NOT EXISTS blocks (
    height BIGINT PRIMARY KEY,
    hash TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    finder TEXT NOT NULL,
    finder_worker TEXT NOT NULL,
    reward BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    confirmed BOOLEAN NOT NULL,
    orphaned BOOLEAN NOT NULL,
    paid_out BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC);

CREATE TABLE IF NOT EXISTS balances (
    address TEXT PRIMARY KEY,
    pending BIGINT NOT NULL,
    paid BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS payouts (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payouts_timestamp ON payouts(timestamp DESC);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS seen_shares (
    job_id TEXT NOT NULL,
    nonce BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    PRIMARY KEY (job_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_seen_shares_expiry ON seen_shares(expires_at);

CREATE TABLE IF NOT EXISTS pending_payouts (
    address TEXT PRIMARY KEY,
    amount BIGINT NOT NULL,
    initiated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_fee_events (
    id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL UNIQUE,
    amount BIGINT NOT NULL,
    fee_address TEXT NOT NULL,
    timestamp BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_timestamp ON pool_fee_events(timestamp DESC);

CREATE TABLE IF NOT EXISTS address_risk (
    address TEXT PRIMARY KEY,
    strikes BIGINT NOT NULL,
    last_reason TEXT,
    last_event_at BIGINT,
    quarantined_until BIGINT,
    force_verify_until BIGINT
);
"#,
        )
        .context("init postgres schema")?;

        Ok(Arc::new(Self {
            conn: Mutex::new(ManuallyDrop::new(conn)),
        }))
    }

    pub fn add_share(&self, share: ShareRecord) -> Result<()> {
        let created = to_unix(share.created_at);
        self.conn.lock().execute(
            "INSERT INTO shares (job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[
                &share.job_id,
                &share.miner,
                &share.worker,
                &(share.difficulty as i64),
                &(share.nonce as i64),
                &share.status,
                &share.was_sampled,
                &share.block_hash,
                &created,
            ],
        )?;
        Ok(())
    }

    pub fn get_recent_shares(&self, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn.lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares ORDER BY id DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn.lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE miner = $1 ORDER BY id DESC LIMIT $2",
            &[&address, &limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_shares_since(&self, since: SystemTime) -> Result<Vec<DbShare>> {
        let ts = to_unix(since);
        let rows = self.conn.lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at >= $1 ORDER BY id DESC",
            &[&ts],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_last_n_shares(&self, n: i64) -> Result<Vec<DbShare>> {
        self.get_recent_shares(n)
    }

    pub fn get_total_share_count(&self) -> Result<u64> {
        let row = self
            .conn
            .lock()
            .query_one("SELECT COUNT(*) FROM shares", &[])?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn add_block(&self, block: &DbBlock) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT(height) DO UPDATE SET
                 hash=EXCLUDED.hash,
                 difficulty=EXCLUDED.difficulty,
                 finder=EXCLUDED.finder,
                 finder_worker=EXCLUDED.finder_worker,
                 reward=EXCLUDED.reward,
                 timestamp=EXCLUDED.timestamp,
                 confirmed=EXCLUDED.confirmed,
                 orphaned=EXCLUDED.orphaned,
                 paid_out=EXCLUDED.paid_out",
            &[
                &(block.height as i64),
                &block.hash,
                &(block.difficulty as i64),
                &block.finder,
                &block.finder_worker,
                &(block.reward as i64),
                &to_unix(block.timestamp),
                &block.confirmed,
                &block.orphaned,
                &block.paid_out,
            ],
        )?;
        Ok(())
    }

    pub fn get_block(&self, height: u64) -> Result<Option<DbBlock>> {
        let row = self.conn.lock().query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE height = $1",
            &[&(height as i64)],
        )?;
        Ok(row.map(|v| row_to_block(&v)))
    }

    pub fn update_block(&self, block: &DbBlock) -> Result<()> {
        self.add_block(block)
    }

    pub fn get_recent_blocks(&self, limit: i64) -> Result<Vec<DbBlock>> {
        let rows = self.conn.lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks ORDER BY height DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_unconfirmed_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn.lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE confirmed = FALSE AND orphaned = FALSE ORDER BY height ASC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_unpaid_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn.lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE confirmed = TRUE AND orphaned = FALSE AND paid_out = FALSE ORDER BY height ASC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_block_count(&self) -> Result<u64> {
        let row = self
            .conn
            .lock()
            .query_one("SELECT COUNT(*) FROM blocks", &[])?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn get_balance(&self, address: &str) -> Result<Balance> {
        let row = self.conn.lock().query_opt(
            "SELECT address, pending, paid FROM balances WHERE address = $1",
            &[&address],
        )?;
        if let Some(row) = row {
            return Ok(Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            });
        }
        Ok(Balance {
            address: address.to_string(),
            pending: 0,
            paid: 0,
        })
    }

    pub fn update_balance(&self, bal: &Balance) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO balances (address, pending, paid) VALUES ($1, $2, $3)
             ON CONFLICT(address) DO UPDATE SET pending = EXCLUDED.pending, paid = EXCLUDED.paid",
            &[&bal.address, &(bal.pending as i64), &(bal.paid as i64)],
        )?;
        Ok(())
    }

    pub fn credit_balance(&self, address: &str, amount: u64) -> Result<()> {
        if amount == 0 {
            return Ok(());
        }

        let mut bal = self.get_balance(address)?;
        bal.pending = bal
            .pending
            .checked_add(amount)
            .ok_or_else(|| anyhow!("balance overflow"))?;
        self.update_balance(&bal)
    }

    pub fn debit_balance(&self, address: &str, amount: u64) -> Result<()> {
        if amount == 0 {
            return Ok(());
        }

        let mut bal = self.get_balance(address)?;
        if bal.pending < amount {
            return Err(anyhow!(
                "insufficient balance: have={}, need={}",
                bal.pending,
                amount
            ));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid balance overflow"))?;
        self.update_balance(&bal)
    }

    pub fn get_all_balances(&self) -> Result<Vec<Balance>> {
        let rows = self.conn.lock().query(
            "SELECT address, pending, paid FROM balances ORDER BY pending DESC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            })
            .collect())
    }

    pub fn add_payout(&self, address: &str, amount: u64, tx_hash: &str) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO payouts (address, amount, tx_hash, timestamp) VALUES ($1, $2, $3, $4)",
            &[&address, &(amount as i64), &tx_hash, &now_unix()],
        )?;
        Ok(())
    }

    pub fn get_recent_payouts(&self, limit: i64) -> Result<Vec<Payout>> {
        let rows = self.conn.lock().query(
            "SELECT id, address, amount, tx_hash, timestamp FROM payouts ORDER BY id DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| Payout {
                id: row.get::<_, i64>(0),
                address: row.get::<_, String>(1),
                amount: row.get::<_, i64>(2).max(0) as u64,
                tx_hash: row.get::<_, String>(3),
                timestamp: from_unix(row.get::<_, i64>(4)),
            })
            .collect())
    }

    pub fn record_pool_fee(
        &self,
        block_height: u64,
        amount: u64,
        fee_address: &str,
        timestamp: SystemTime,
    ) -> Result<bool> {
        if amount == 0 {
            return Ok(false);
        }

        let destination = fee_address.trim();
        if destination.is_empty() {
            return Err(anyhow!("fee address is required"));
        }

        let inserted = self.conn.lock().execute(
            "INSERT INTO pool_fee_events (block_height, amount, fee_address, timestamp)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(block_height) DO NOTHING",
            &[
                &(block_height as i64),
                &(amount as i64),
                &destination,
                &to_unix(timestamp),
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn get_total_pool_fees(&self) -> Result<u64> {
        let row = self
            .conn
            .lock()
            .query_one("SELECT COALESCE(SUM(amount), 0) FROM pool_fee_events", &[])?;
        let total: i64 = row.get(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_recent_pool_fees(&self, limit: i64) -> Result<Vec<PoolFeeEvent>> {
        let rows = self.conn.lock().query(
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events ORDER BY id DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| PoolFeeEvent {
                id: row.get::<_, i64>(0),
                block_height: row.get::<_, i64>(1).max(0) as u64,
                amount: row.get::<_, i64>(2).max(0) as u64,
                fee_address: row.get::<_, String>(3),
                timestamp: from_unix(row.get::<_, i64>(4)),
            })
            .collect())
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO meta (key, value) VALUES ($1, $2)
             ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
            &[&key, &value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row = self
            .conn
            .lock()
            .query_opt("SELECT value FROM meta WHERE key = $1", &[&key])?;
        Ok(row.map(|v| v.get::<_, Vec<u8>>(0)))
    }

    pub fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        let expires_at = now_unix() + SEEN_SHARE_EXPIRY_SECS;
        self.conn.lock().execute(
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = EXCLUDED.expires_at",
            &[&job_id, &(nonce as i64), &expires_at],
        )?;
        Ok(())
    }

    pub fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        let row = self.conn.lock().query_opt(
            "SELECT expires_at FROM seen_shares WHERE job_id = $1 AND nonce = $2",
            &[&job_id, &(nonce as i64)],
        )?;
        if let Some(row) = row {
            let expiry: i64 = row.get(0);
            return Ok(expiry > now_unix());
        }
        Ok(false)
    }

    pub fn clean_expired_seen_shares(&self) -> Result<u64> {
        let affected = self.conn.lock().execute(
            "DELETE FROM seen_shares WHERE expires_at <= $1",
            &[&now_unix()],
        )?;
        Ok(affected as u64)
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        let row = self.conn.lock().query_opt(
            "SELECT address, strikes, last_reason, last_event_at, quarantined_until, force_verify_until
             FROM address_risk WHERE address = $1",
            &[&address],
        )?;
        Ok(row.map(|row| AddressRiskState {
            address: row.get::<_, String>(0),
            strikes: row.get::<_, i64>(1).max(0) as u64,
            last_reason: row.get::<_, Option<String>>(2),
            last_event_at: row.get::<_, Option<i64>>(3).map(from_unix),
            quarantined_until: row.get::<_, Option<i64>>(4).map(from_unix),
            force_verify_until: row.get::<_, Option<i64>>(5).map(from_unix),
        }))
    }

    pub fn is_address_quarantined(
        &self,
        address: &str,
    ) -> Result<(bool, Option<AddressRiskState>)> {
        let state = self.get_address_risk(address)?;
        let now = SystemTime::now();
        let quarantined = state
            .as_ref()
            .and_then(|s| s.quarantined_until)
            .is_some_and(|until| until > now);
        Ok((quarantined, state))
    }

    pub fn should_force_verify_address(
        &self,
        address: &str,
    ) -> Result<(bool, Option<AddressRiskState>)> {
        let state = self.get_address_risk(address)?;
        let now = SystemTime::now();
        let force = state.as_ref().is_some_and(|s| {
            s.force_verify_until.is_some_and(|until| until > now)
                || s.quarantined_until.is_some_and(|until| until > now)
        });
        Ok((force, state))
    }

    pub fn escalate_address_risk(
        &self,
        address: &str,
        reason: &str,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
        apply_quarantine: bool,
    ) -> Result<AddressRiskState> {
        if address.trim().is_empty() {
            return Err(anyhow!("address is required"));
        }

        let mut state = self
            .get_address_risk(address)?
            .unwrap_or_else(|| AddressRiskState {
                address: address.to_string(),
                ..AddressRiskState::default()
            });

        let now = SystemTime::now();
        state.address = address.to_string();
        state.strikes = state.strikes.saturating_add(1);
        state.last_reason = Some(reason.to_string());
        state.last_event_at = Some(now);

        if !force_verify_duration.is_zero() {
            let force_until = now + force_verify_duration;
            if state
                .force_verify_until
                .is_none_or(|existing| force_until > existing)
            {
                state.force_verify_until = Some(force_until);
            }
        }

        if apply_quarantine && !quarantine_base.is_zero() {
            let mut duration = quarantine_base;
            if !quarantine_max.is_zero() && duration > quarantine_max {
                duration = quarantine_max;
            }
            for _ in 1..state.strikes {
                if quarantine_max.is_zero() {
                    duration = duration.saturating_mul(2);
                    continue;
                }
                duration = duration.saturating_mul(2);
                if duration > quarantine_max {
                    duration = quarantine_max;
                    break;
                }
            }
            let until = now + duration;
            if state
                .quarantined_until
                .is_none_or(|existing| until > existing)
            {
                state.quarantined_until = Some(until);
            }
        }

        self.conn.lock().execute(
            "INSERT INTO address_risk (address, strikes, last_reason, last_event_at, quarantined_until, force_verify_until)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT(address) DO UPDATE SET
                strikes = EXCLUDED.strikes,
                last_reason = EXCLUDED.last_reason,
                last_event_at = EXCLUDED.last_event_at,
                quarantined_until = EXCLUDED.quarantined_until,
                force_verify_until = EXCLUDED.force_verify_until",
            &[
                &state.address,
                &(state.strikes as i64),
                &state.last_reason,
                &state.last_event_at.map(to_unix),
                &state.quarantined_until.map(to_unix),
                &state.force_verify_until.map(to_unix),
            ],
        )?;

        Ok(state)
    }

    pub fn get_risk_summary(&self) -> Result<(u64, u64)> {
        let now = now_unix();
        let mut conn = self.conn.lock();

        let q_row = conn.query_one(
            "SELECT COUNT(*) FROM address_risk WHERE quarantined_until IS NOT NULL AND quarantined_until > $1",
            &[&now],
        )?;
        let quarantined: i64 = q_row.get(0);

        let f_row = conn.query_one(
            "SELECT COUNT(*) FROM address_risk
             WHERE (force_verify_until IS NOT NULL AND force_verify_until > $1)
                OR (quarantined_until IS NOT NULL AND quarantined_until > $1)",
            &[&now],
        )?;
        let forced: i64 = f_row.get(0);

        Ok((quarantined.max(0) as u64, forced.max(0) as u64))
    }

    pub fn create_pending_payout(&self, address: &str, amount: u64) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO pending_payouts (address, amount, initiated_at) VALUES ($1, $2, $3)
             ON CONFLICT(address) DO UPDATE SET amount = EXCLUDED.amount, initiated_at = EXCLUDED.initiated_at",
            &[&address, &(amount as i64), &now_unix()],
        )?;
        Ok(())
    }

    pub fn complete_pending_payout(&self, address: &str, amount: u64, tx_hash: &str) -> Result<()> {
        let mut conn = self.conn.lock();
        let mut tx = conn.transaction()?;

        let pending = tx.query_opt(
            "SELECT amount, initiated_at FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        if pending.is_none() {
            return Err(anyhow!("no pending payout for {address}"));
        }

        let row = tx.query_opt(
            "SELECT address, pending, paid FROM balances WHERE address = $1",
            &[&address],
        )?;
        let mut bal = if let Some(row) = row {
            Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            }
        } else {
            Balance {
                address: address.to_string(),
                pending: 0,
                paid: 0,
            }
        };
        if bal.pending < amount {
            return Err(anyhow!("insufficient balance"));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid overflow"))?;

        tx.execute(
            "INSERT INTO balances (address, pending, paid) VALUES ($1, $2, $3)
             ON CONFLICT(address) DO UPDATE SET pending = EXCLUDED.pending, paid = EXCLUDED.paid",
            &[&bal.address, &(bal.pending as i64), &(bal.paid as i64)],
        )?;

        tx.execute(
            "INSERT INTO payouts (address, amount, tx_hash, timestamp) VALUES ($1, $2, $3, $4)",
            &[&address, &(amount as i64), &tx_hash, &now_unix()],
        )?;

        tx.execute(
            "DELETE FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn cancel_pending_payout(&self, address: &str) -> Result<()> {
        self.conn.lock().execute(
            "DELETE FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        Ok(())
    }

    pub fn get_pending_payouts(&self) -> Result<Vec<PendingPayout>> {
        let rows = self.conn.lock().query(
            "SELECT address, amount, initiated_at FROM pending_payouts ORDER BY initiated_at ASC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| PendingPayout {
                address: row.get::<_, String>(0),
                amount: row.get::<_, i64>(1).max(0) as u64,
                initiated_at: from_unix(row.get::<_, i64>(2)),
            })
            .collect())
    }

    pub fn get_pending_payout(&self, address: &str) -> Result<Option<PendingPayout>> {
        let row = self.conn.lock().query_opt(
            "SELECT address, amount, initiated_at FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        Ok(row.map(|row| PendingPayout {
            address: row.get::<_, String>(0),
            amount: row.get::<_, i64>(1).max(0) as u64,
            initiated_at: from_unix(row.get::<_, i64>(2)),
        }))
    }
}

impl ShareStore for PostgresStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        PostgresStore::is_share_seen(self, job_id, nonce)
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        PostgresStore::mark_share_seen(self, job_id, nonce)
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        PostgresStore::add_share(self, share)
    }
}

fn row_to_share(row: postgres::Row) -> DbShare {
    DbShare {
        id: row.get::<_, i64>(0),
        job_id: row.get::<_, String>(1),
        miner: row.get::<_, String>(2),
        worker: row.get::<_, String>(3),
        difficulty: row.get::<_, i64>(4).max(0) as u64,
        nonce: row.get::<_, i64>(5).max(0) as u64,
        status: row.get::<_, String>(6),
        was_sampled: row.get::<_, bool>(7),
        block_hash: row.get::<_, Option<String>>(8),
        created_at: from_unix(row.get::<_, i64>(9)),
    }
}

fn row_to_block(row: &postgres::Row) -> DbBlock {
    DbBlock {
        height: row.get::<_, i64>(0).max(0) as u64,
        hash: row.get::<_, String>(1),
        difficulty: row.get::<_, i64>(2).max(0) as u64,
        finder: row.get::<_, String>(3),
        finder_worker: row.get::<_, String>(4),
        reward: row.get::<_, i64>(5).max(0) as u64,
        timestamp: from_unix(row.get::<_, i64>(6)),
        confirmed: row.get::<_, bool>(7),
        orphaned: row.get::<_, bool>(8),
        paid_out: row.get::<_, bool>(9),
    }
}

fn now_unix() -> i64 {
    to_unix(SystemTime::now())
}

fn to_unix(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn from_unix(ts: i64) -> SystemTime {
    if ts <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_secs(ts as u64)
}