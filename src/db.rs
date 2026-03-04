use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::engine::{ShareRecord, ShareStore};

const SEEN_SHARE_EXPIRY_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, Serialize)]
pub struct DbShare {
    pub id: i64,
    pub job_id: String,
    pub miner: String,
    pub worker: String,
    pub difficulty: u64,
    pub nonce: u64,
    pub status: String,
    pub was_sampled: bool,
    pub block_hash: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbBlock {
    pub height: u64,
    pub hash: String,
    pub difficulty: u64,
    pub finder: String,
    pub finder_worker: String,
    pub reward: u64,
    pub timestamp: SystemTime,
    pub confirmed: bool,
    pub orphaned: bool,
    pub paid_out: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Balance {
    pub address: String,
    pub pending: u64,
    pub paid: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Payout {
    pub id: i64,
    pub address: String,
    pub amount: u64,
    pub tx_hash: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingPayout {
    pub address: String,
    pub amount: u64,
    pub initiated_at: SystemTime,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolFeeEvent {
    pub id: i64,
    pub block_height: u64,
    pub amount: u64,
    pub fee_address: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AddressRiskState {
    pub address: String,
    pub strikes: u64,
    pub last_reason: Option<String>,
    pub last_event_at: Option<SystemTime>,
    pub quarantined_until: Option<SystemTime>,
    pub force_verify_until: Option<SystemTime>,
}

#[derive(Debug)]
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: &str) -> Result<Arc<Self>> {
        ensure_sqlite_compatible_path(path)?;
        let conn = Connection::open(path).with_context(|| format!("open sqlite {path}"))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("set WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("set synchronous")?;

        let store = Arc::new(Self {
            conn: Mutex::new(conn),
        });
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<()> {
        let sql = r#"
CREATE TABLE IF NOT EXISTS shares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    miner TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty INTEGER NOT NULL,
    nonce INTEGER NOT NULL,
    status TEXT NOT NULL,
    was_sampled INTEGER NOT NULL,
    block_hash TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_created ON shares(miner, created_at DESC);

CREATE TABLE IF NOT EXISTS blocks (
    height INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    difficulty INTEGER NOT NULL,
    finder TEXT NOT NULL,
    finder_worker TEXT NOT NULL,
    reward INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    confirmed INTEGER NOT NULL,
    orphaned INTEGER NOT NULL,
    paid_out INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC);

CREATE TABLE IF NOT EXISTS balances (
    address TEXT PRIMARY KEY,
    pending INTEGER NOT NULL,
    paid INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    amount INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payouts_timestamp ON payouts(timestamp DESC);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS seen_shares (
    job_id TEXT NOT NULL,
    nonce INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_seen_shares_expiry ON seen_shares(expires_at);

CREATE TABLE IF NOT EXISTS pending_payouts (
    address TEXT PRIMARY KEY,
    amount INTEGER NOT NULL,
    initiated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_fee_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    block_height INTEGER NOT NULL UNIQUE,
    amount INTEGER NOT NULL,
    fee_address TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_timestamp ON pool_fee_events(timestamp DESC);

CREATE TABLE IF NOT EXISTS address_risk (
    address TEXT PRIMARY KEY,
    strikes INTEGER NOT NULL,
    last_reason TEXT,
    last_event_at INTEGER,
    quarantined_until INTEGER,
    force_verify_until INTEGER
);
"#;

        self.conn.lock().execute_batch(sql).context("init schema")?;
        Ok(())
    }

    pub fn add_share_immediate(&self, share: ShareRecord) -> Result<()> {
        self.add_share(share)
    }

    pub fn add_share(&self, share: ShareRecord) -> Result<()> {
        let created = to_unix(share.created_at);
        self.conn.lock().execute(
            "INSERT INTO shares (job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                share.job_id,
                share.miner,
                share.worker,
                share.difficulty as i64,
                share.nonce as i64,
                share.status,
                if share.was_sampled { 1i64 } else { 0i64 },
                share.block_hash,
                created,
            ],
        )?;
        Ok(())
    }

    pub fn get_recent_shares(&self, limit: i64) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE miner = ?1 ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![address, limit], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_shares_since(&self, since: SystemTime) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at >= ?1 ORDER BY id DESC",
        )?;
        let rows = stmt.query_map(params![to_unix(since)], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_last_n_shares(&self, n: i64) -> Result<Vec<DbShare>> {
        self.get_recent_shares(n)
    }

    pub fn get_total_share_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM shares", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }

    pub fn add_block(&self, block: &DbBlock) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(height) DO UPDATE SET
                 hash=excluded.hash,
                 difficulty=excluded.difficulty,
                 finder=excluded.finder,
                 finder_worker=excluded.finder_worker,
                 reward=excluded.reward,
                 timestamp=excluded.timestamp,
                 confirmed=excluded.confirmed,
                 orphaned=excluded.orphaned,
                 paid_out=excluded.paid_out",
            params![
                block.height as i64,
                block.hash,
                block.difficulty as i64,
                block.finder,
                block.finder_worker,
                block.reward as i64,
                to_unix(block.timestamp),
                if block.confirmed { 1i64 } else { 0i64 },
                if block.orphaned { 1i64 } else { 0i64 },
                if block.paid_out { 1i64 } else { 0i64 },
            ],
        )?;
        Ok(())
    }

    pub fn get_block(&self, height: u64) -> Result<Option<DbBlock>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE height = ?1",
            params![height as i64],
            row_to_block,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn update_block(&self, block: &DbBlock) -> Result<()> {
        self.add_block(block)
    }

    pub fn get_recent_blocks(&self, limit: i64) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks ORDER BY height DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_block)?;
        collect_rows(rows)
    }

    pub fn get_unconfirmed_blocks(&self) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE confirmed = 0 AND orphaned = 0 ORDER BY height ASC",
        )?;
        let rows = stmt.query_map([], row_to_block)?;
        collect_rows(rows)
    }

    pub fn get_unpaid_blocks(&self) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE confirmed = 1 AND orphaned = 0 AND paid_out = 0 ORDER BY height ASC",
        )?;
        let rows = stmt.query_map([], row_to_block)?;
        collect_rows(rows)
    }

    pub fn get_block_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }

    pub fn get_balance(&self, address: &str) -> Result<Balance> {
        let conn = self.conn.lock();
        if let Some(bal) = conn
            .query_row(
                "SELECT address, pending, paid FROM balances WHERE address = ?1",
                params![address],
                |row| {
                    Ok(Balance {
                        address: row.get::<_, String>(0)?,
                        pending: row.get::<_, i64>(1)?.max(0) as u64,
                        paid: row.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?
        {
            return Ok(bal);
        }
        Ok(Balance {
            address: address.to_string(),
            pending: 0,
            paid: 0,
        })
    }

    pub fn update_balance(&self, bal: &Balance) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO balances (address, pending, paid) VALUES (?1, ?2, ?3)
             ON CONFLICT(address) DO UPDATE SET pending = excluded.pending, paid = excluded.paid",
            params![bal.address, bal.pending as i64, bal.paid as i64],
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
        let conn = self.conn.lock();
        let mut stmt =
            conn.prepare("SELECT address, pending, paid FROM balances ORDER BY pending DESC")?;
        let rows = stmt.query_map([], |row| {
            Ok(Balance {
                address: row.get::<_, String>(0)?,
                pending: row.get::<_, i64>(1)?.max(0) as u64,
                paid: row.get::<_, i64>(2)?.max(0) as u64,
            })
        })?;
        collect_rows(rows)
    }

    pub fn add_payout(&self, address: &str, amount: u64, tx_hash: &str) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO payouts (address, amount, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4)",
            params![address, amount as i64, tx_hash, now_unix()],
        )?;
        Ok(())
    }

    pub fn get_recent_payouts(&self, limit: i64) -> Result<Vec<Payout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, address, amount, tx_hash, timestamp FROM payouts ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(Payout {
                id: row.get(0)?,
                address: row.get(1)?,
                amount: row.get::<_, i64>(2)?.max(0) as u64,
                tx_hash: row.get(3)?,
                timestamp: from_unix(row.get::<_, i64>(4)?),
            })
        })?;
        collect_rows(rows)
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
            "INSERT OR IGNORE INTO pool_fee_events (block_height, amount, fee_address, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                block_height as i64,
                amount as i64,
                destination,
                to_unix(timestamp)
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn get_total_pool_fees(&self) -> Result<u64> {
        let total: Option<i64> =
            self.conn
                .lock()
                .query_row("SELECT SUM(amount) FROM pool_fee_events", [], |row| {
                    row.get(0)
                })?;
        Ok(total.unwrap_or(0).max(0) as u64)
    }

    pub fn get_recent_pool_fees(&self, limit: i64) -> Result<Vec<PoolFeeEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_pool_fee_event)?;
        collect_rows(rows)
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        let value = conn
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value)
    }

    pub fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        let expires_at = now_unix() + SEEN_SHARE_EXPIRY_SECS;
        self.conn.lock().execute(
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = excluded.expires_at",
            params![job_id, nonce as i64, expires_at],
        )?;
        Ok(())
    }

    pub fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        let now = now_unix();
        let expiry: Option<i64> = self
            .conn
            .lock()
            .query_row(
                "SELECT expires_at FROM seen_shares WHERE job_id = ?1 AND nonce = ?2",
                params![job_id, nonce as i64],
                |row| row.get(0),
            )
            .optional()?;

        Ok(expiry.is_some_and(|ts| ts > now))
    }

    pub fn clean_expired_seen_shares(&self) -> Result<u64> {
        let now = now_unix();
        let affected = self.conn.lock().execute(
            "DELETE FROM seen_shares WHERE expires_at <= ?1",
            params![now],
        )?;
        Ok(affected as u64)
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT address, strikes, last_reason, last_event_at, quarantined_until, force_verify_until
             FROM address_risk WHERE address = ?1",
            params![address],
            |row| {
                Ok(AddressRiskState {
                    address: row.get(0)?,
                    strikes: row.get::<_, i64>(1)?.max(0) as u64,
                    last_reason: row.get(2)?,
                    last_event_at: row.get::<_, Option<i64>>(3)?.map(from_unix),
                    quarantined_until: row.get::<_, Option<i64>>(4)?.map(from_unix),
                    force_verify_until: row.get::<_, Option<i64>>(5)?.map(from_unix),
                })
            },
        )
        .optional()
        .map_err(Into::into)
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
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(address) DO UPDATE SET
                strikes = excluded.strikes,
                last_reason = excluded.last_reason,
                last_event_at = excluded.last_event_at,
                quarantined_until = excluded.quarantined_until,
                force_verify_until = excluded.force_verify_until",
            params![
                state.address,
                state.strikes as i64,
                state.last_reason,
                state.last_event_at.map(to_unix),
                state.quarantined_until.map(to_unix),
                state.force_verify_until.map(to_unix),
            ],
        )?;

        Ok(state)
    }

    pub fn get_risk_summary(&self) -> Result<(u64, u64)> {
        let now = now_unix();
        let conn = self.conn.lock();

        let quarantined: i64 = conn.query_row(
            "SELECT COUNT(*) FROM address_risk WHERE quarantined_until IS NOT NULL AND quarantined_until > ?1",
            params![now],
            |row| row.get(0),
        )?;

        let forced: i64 = conn.query_row(
            "SELECT COUNT(*) FROM address_risk
             WHERE (force_verify_until IS NOT NULL AND force_verify_until > ?1)
                OR (quarantined_until IS NOT NULL AND quarantined_until > ?1)",
            params![now],
            |row| row.get(0),
        )?;

        Ok((quarantined.max(0) as u64, forced.max(0) as u64))
    }

    pub fn create_pending_payout(&self, address: &str, amount: u64) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO pending_payouts (address, amount, initiated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(address) DO UPDATE SET amount = excluded.amount, initiated_at = excluded.initiated_at",
            params![address, amount as i64, now_unix()],
        )?;
        Ok(())
    }

    pub fn complete_pending_payout(&self, address: &str, amount: u64, tx_hash: &str) -> Result<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;

        let pending: Option<(i64, i64)> = tx
            .query_row(
                "SELECT amount, initiated_at FROM pending_payouts WHERE address = ?1",
                params![address],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if pending.is_none() {
            return Err(anyhow!("no pending payout for {address}"));
        }

        let mut bal = tx
            .query_row(
                "SELECT address, pending, paid FROM balances WHERE address = ?1",
                params![address],
                |row| {
                    Ok(Balance {
                        address: row.get(0)?,
                        pending: row.get::<_, i64>(1)?.max(0) as u64,
                        paid: row.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?
            .unwrap_or_else(|| Balance {
                address: address.to_string(),
                pending: 0,
                paid: 0,
            });
        if bal.pending < amount {
            return Err(anyhow!("insufficient balance"));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid overflow"))?;

        tx.execute(
            "INSERT INTO balances (address, pending, paid) VALUES (?1, ?2, ?3)
             ON CONFLICT(address) DO UPDATE SET pending = excluded.pending, paid = excluded.paid",
            params![bal.address, bal.pending as i64, bal.paid as i64],
        )?;

        tx.execute(
            "INSERT INTO payouts (address, amount, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4)",
            params![address, amount as i64, tx_hash, now_unix()],
        )?;

        tx.execute(
            "DELETE FROM pending_payouts WHERE address = ?1",
            params![address],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn cancel_pending_payout(&self, address: &str) -> Result<()> {
        self.conn.lock().execute(
            "DELETE FROM pending_payouts WHERE address = ?1",
            params![address],
        )?;
        Ok(())
    }

    pub fn get_pending_payouts(&self) -> Result<Vec<PendingPayout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT address, amount, initiated_at FROM pending_payouts ORDER BY initiated_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PendingPayout {
                address: row.get(0)?,
                amount: row.get::<_, i64>(1)?.max(0) as u64,
                initiated_at: from_unix(row.get::<_, i64>(2)?),
            })
        })?;
        collect_rows(rows)
    }

    pub fn get_pending_payout(&self, address: &str) -> Result<Option<PendingPayout>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT address, amount, initiated_at FROM pending_payouts WHERE address = ?1",
            params![address],
            |row| {
                Ok(PendingPayout {
                    address: row.get(0)?,
                    amount: row.get::<_, i64>(1)?.max(0) as u64,
                    initiated_at: from_unix(row.get::<_, i64>(2)?),
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }
}

fn ensure_sqlite_compatible_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.exists() {
        return Ok(());
    }

    let meta = std::fs::metadata(p).with_context(|| format!("stat {}", p.display()))?;
    if meta.len() == 0 {
        return Ok(());
    }

    let mut file = File::open(p).with_context(|| format!("open {}", p.display()))?;
    let mut header = [0u8; 16];
    let bytes_read = file
        .read(&mut header)
        .with_context(|| format!("read {}", p.display()))?;

    const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
    if bytes_read >= 16 && &header == SQLITE_HEADER {
        return Ok(());
    }

    Err(anyhow!(
        "database path '{}' is not a SQLite file (likely legacy pool DB format). \
move or rename it and restart, e.g. `mv {} {}.legacy`",
        p.display(),
        p.display(),
        p.display()
    ))
}

impl ShareStore for SqliteStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        SqliteStore::is_share_seen(self, job_id, nonce)
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        SqliteStore::mark_share_seen(self, job_id, nonce)
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        SqliteStore::add_share(self, share)
    }
}

fn row_to_share(row: &rusqlite::Row<'_>) -> rusqlite::Result<DbShare> {
    Ok(DbShare {
        id: row.get(0)?,
        job_id: row.get(1)?,
        miner: row.get(2)?,
        worker: row.get(3)?,
        difficulty: row.get::<_, i64>(4)?.max(0) as u64,
        nonce: row.get::<_, i64>(5)?.max(0) as u64,
        status: row.get(6)?,
        was_sampled: row.get::<_, i64>(7)? != 0,
        block_hash: row.get(8)?,
        created_at: from_unix(row.get(9)?),
    })
}

fn row_to_block(row: &rusqlite::Row<'_>) -> rusqlite::Result<DbBlock> {
    Ok(DbBlock {
        height: row.get::<_, i64>(0)?.max(0) as u64,
        hash: row.get(1)?,
        difficulty: row.get::<_, i64>(2)?.max(0) as u64,
        finder: row.get(3)?,
        finder_worker: row.get(4)?,
        reward: row.get::<_, i64>(5)?.max(0) as u64,
        timestamp: from_unix(row.get(6)?),
        confirmed: row.get::<_, i64>(7)? != 0,
        orphaned: row.get::<_, i64>(8)? != 0,
        paid_out: row.get::<_, i64>(9)? != 0,
    })
}

fn row_to_pool_fee_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<PoolFeeEvent> {
    Ok(PoolFeeEvent {
        id: row.get(0)?,
        block_height: row.get::<_, i64>(1)?.max(0) as u64,
        amount: row.get::<_, i64>(2)?.max(0) as u64,
        fee_address: row.get(3)?,
        timestamp: from_unix(row.get::<_, i64>(4)?),
    })
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ShareRecord;
    use crate::validation::SHARE_STATUS_VERIFIED;

    fn test_store() -> Arc<SqliteStore> {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pool.sqlite");
        SqliteStore::open(path.to_str().expect("path")).expect("open store")
    }

    #[test]
    fn share_seen_roundtrip() {
        let store = test_store();
        assert!(!store.is_share_seen("job", 1).expect("seen"));
        store.mark_share_seen("job", 1).expect("mark");
        assert!(store.is_share_seen("job", 1).expect("seen2"));
    }

    #[test]
    fn add_and_query_shares() {
        let store = test_store();
        store
            .add_share(ShareRecord {
                job_id: "job1".into(),
                miner: "addr1".into(),
                worker: "rig1".into(),
                difficulty: 1,
                nonce: 7,
                status: SHARE_STATUS_VERIFIED,
                was_sampled: true,
                block_hash: None,
                created_at: SystemTime::now(),
            })
            .expect("add share");

        let shares = store.get_recent_shares(10).expect("recent");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].nonce, 7);
    }

    #[test]
    fn risk_escalation_and_queries() {
        let store = test_store();
        let state = store
            .escalate_address_risk(
                "addr1",
                "fraud",
                Duration::from_secs(60),
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                true,
            )
            .expect("escalate");
        assert_eq!(state.strikes, 1);

        let (force, loaded) = store
            .should_force_verify_address("addr1")
            .expect("force verify");
        assert!(force);
        assert!(loaded.is_some());

        let (quarantined, _) = store.is_address_quarantined("addr1").expect("quarantine");
        assert!(quarantined);
    }

    #[test]
    fn pending_payout_roundtrip() {
        let store = test_store();
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        let pending = store.get_pending_payout("addr1").expect("get pending");
        assert!(pending.is_some());
        assert_eq!(pending.expect("pending").amount, 100);

        store.cancel_pending_payout("addr1").expect("cancel");
        assert!(store
            .get_pending_payout("addr1")
            .expect("get after cancel")
            .is_none());
    }

    #[test]
    fn pool_fee_events_are_idempotent_per_block() {
        let store = test_store();

        assert!(store
            .record_pool_fee(100, 50, "pool-address", SystemTime::now())
            .expect("insert first fee"));
        assert!(!store
            .record_pool_fee(100, 50, "pool-address", SystemTime::now())
            .expect("duplicate fee should be ignored"));

        assert_eq!(store.get_total_pool_fees().expect("total"), 50);
        let events = store.get_recent_pool_fees(10).expect("recent fees");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_height, 100);
        assert_eq!(events[0].amount, 50);
    }
}