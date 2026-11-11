//! Candidate LTC-20 checkpoint producer. Never certifies an uncheckpointed legacy ledger.
use crate::indexer::Indexer;
use crate::ordinals::Block;
use crate::schema::status;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::panic::AssertUnwindSafe;

pub type CheckResult<T> = Result<T, String>;
pub const CHECKPOINT_KEY: &str = "token_trade_checkpoint";
// The reference migrations start immediately before LTC-20 activation.
pub const INITIAL_HEIGHT: i32 = 2_465_225;
pub const WRITER_LOCK: i64 = 0x4c54433230535750;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub version: u8,
    pub protocol: String,
    pub chain: String,
    pub ready: bool,
    pub height: i32,
    pub block_hash: String,
}

pub fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Checkpoint {
    pub fn new(height: i32, block_hash: String) -> CheckResult<Self> {
        let value = Self {
            version: 1,
            protocol: "ltc-20".into(),
            chain: "mainnet".into(),
            ready: true,
            height,
            block_hash,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> CheckResult<()> {
        if self.version != 1
            || self.protocol != "ltc-20"
            || self.chain != "mainnet"
            || !self.ready
            || self.height < INITIAL_HEIGHT
            || !valid_hash(&self.block_hash)
        {
            return Err("Invalid native ledger checkpoint".into());
        }
        Ok(())
    }

    pub fn successor(&self, block: &Block) -> CheckResult<Self> {
        self.validate()?;
        if self.height.checked_add(1) != Some(block.height)
            || block.previous_blockhash != self.block_hash
        {
            return Err(
                "Noncontiguous block or changed parent; recovery requires a separate audit".into(),
            );
        }
        Self::new(block.height, block.hash.clone())
    }
}

#[derive(QueryableByName)]
struct LockResult {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    acquired: bool,
}
#[derive(QueryableByName)]
struct EmptyResult {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    empty: bool,
}

impl Indexer {
    fn begin_checkpoint(&mut self) -> CheckResult<()> {
        self.database.connection.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ; SET LOCAL lock_timeout = '2s'; SET LOCAL statement_timeout = '30s';")
            .map_err(|_| "Cannot begin ledger transaction")?;
        let result = diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS acquired")
            .bind::<diesel::sql_types::BigInt, _>(WRITER_LOCK)
            .get_result::<LockResult>(&mut self.database.connection)
            .map_err(|_| "Cannot acquire ledger writer lock")?;
        if !result.acquired {
            return Err("Another checkpoint writer is active".into());
        }
        // Also excludes writers that do not implement the advisory lock during this block.
        self.database.connection.batch_execute("LOCK TABLE status, balances, tracker, inscriptions, history IN SHARE ROW EXCLUSIVE MODE NOWAIT")
            .map_err(|_| "Another ledger writer is active")?;
        Ok(())
    }

    fn finish_checkpoint<T>(&mut self, result: CheckResult<T>) -> CheckResult<T> {
        match result {
            Ok(value) => {
                self.database
                    .connection
                    .batch_execute("COMMIT")
                    .map_err(|_| "Ledger commit failed; stop and inspect before retrying")?;
                Ok(value)
            }
            Err(error) => {
                self.database
                    .connection
                    .batch_execute("ROLLBACK")
                    .map_err(|_| "Ledger rollback failed; connection must be discarded")?;
                Err(error)
            }
        }
    }

    fn read_height(&mut self) -> CheckResult<i32> {
        self.database
            .get_status("last_height".into())
            .map_err(|_| "Missing ledger progress")?
            .value
            .ok_or("Missing ledger progress")?
            .parse()
            .map_err(|_| "Invalid ledger progress".into())
    }

    pub(crate) fn read_checkpoint(&mut self) -> CheckResult<Checkpoint> {
        let value = self
            .database
            .get_status(CHECKPOINT_KEY.into())
            .map_err(|_| "Native checkpoint missing; legacy ledgers require audited replay")?
            .value
            .ok_or("Native checkpoint missing")?;
        let checkpoint: Checkpoint =
            serde_json::from_str(&value).map_err(|_| "Malformed native checkpoint")?;
        checkpoint.validate()?;
        if checkpoint.height != self.read_height()? {
            return Err("Ledger progress differs from checkpoint".into());
        }
        Ok(checkpoint)
    }

    fn write_checkpoint(&mut self, checkpoint: &Checkpoint) -> CheckResult<()> {
        checkpoint.validate()?;
        let value = serde_json::to_string(checkpoint).map_err(|_| "Cannot encode checkpoint")?;
        // This must succeed after every ledger mutation. In particular, COMMIT alone is
        // insufficient: PostgreSQL COMMIT on an aborted transaction performs ROLLBACK.
        let changed = diesel::insert_into(status::table)
            .values((status::key.eq(CHECKPOINT_KEY), status::value.eq(&value)))
            .on_conflict(status::key)
            .do_update()
            .set(status::value.eq(&value))
            .execute(&mut self.database.connection)
            .map_err(|_| "Ledger or checkpoint write failed")?;
        if changed != 1 {
            return Err("Checkpoint write did not affect exactly one row".into());
        }
        Ok(())
    }

    async fn canonical(&mut self, checkpoint: &Checkpoint) -> CheckResult<()> {
        self.ordinals.verify_chain(checkpoint.height).await?;
        if self.ordinals.get_block_hash(checkpoint.height).await? != checkpoint.block_hash {
            return Err("Canonical block changed; stop for audited recovery".into());
        }
        Ok(())
    }

    pub(crate) async fn initialize_checkpoint(&mut self) -> CheckResult<Checkpoint> {
        if self.meta_protocol != "ltc-20" {
            return Err("This candidate checkpoint producer supports only LTC-20 mainnet".into());
        }
        let result = async {
            self.begin_checkpoint()?;
            let existing = self.database.get_status(CHECKPOINT_KEY.into()).optional().map_err(|_| "Cannot read checkpoint")?;
            let checkpoint = if existing.is_some() {
                self.read_checkpoint()?
            } else {
                let empty = diesel::sql_query("SELECT NOT (EXISTS (SELECT 1 FROM inscriptions) OR EXISTS (SELECT 1 FROM balances) OR EXISTS (SELECT 1 FROM tracker) OR EXISTS (SELECT 1 FROM history)) AS empty")
                    .get_result::<EmptyResult>(&mut self.database.connection).map_err(|_| "Cannot verify empty initial ledger")?.empty;
                if !empty || self.read_height()? != INITIAL_HEIGHT {
                    return Err("Refusing to certify a populated or advanced legacy ledger; audited replay required".into());
                }
                self.ordinals.verify_chain(INITIAL_HEIGHT).await?;
                Checkpoint::new(INITIAL_HEIGHT, self.ordinals.get_block_hash(INITIAL_HEIGHT).await?)?
            };
            self.canonical(&checkpoint).await?;
            self.write_checkpoint(&checkpoint)?;
            Ok(checkpoint)
        }.await;
        self.finish_checkpoint(result)
    }

    pub(crate) async fn commit_block(&mut self, block: &Block) -> CheckResult<i32> {
        let result = AssertUnwindSafe(async {
            self.begin_checkpoint()?;
            let previous = self.read_checkpoint()?;
            let next = previous.successor(block)?;
            self.canonical(&previous).await?;
            self.canonical(&next).await?;
            let count = self.process_block(block).await?;
            self.canonical(&previous).await?;
            self.canonical(&next).await?;
            self.database
                .update_status("last_height".into(), next.height.to_string())
                .map_err(|_| "Cannot advance ledger progress")?;
            self.write_checkpoint(&next)?;
            Ok(count)
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err("Ledger processing panicked; block rolled back".into()));
        self.finish_checkpoint(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_contract_is_strict() {
        let good = Checkpoint::new(INITIAL_HEIGHT, "a".repeat(64)).unwrap();
        assert_eq!(
            serde_json::from_str::<Checkpoint>(&serde_json::to_string(&good).unwrap()).unwrap(),
            good
        );
        assert!(Checkpoint::new(INITIAL_HEIGHT - 1, "a".repeat(64)).is_err());
        for hash in ["A".repeat(64), "a".repeat(63), "g".repeat(64)] {
            assert!(!valid_hash(&hash));
        }
        let mut wrong = good.clone();
        wrong.ready = false;
        assert!(wrong.validate().is_err());
        wrong = good.clone();
        wrong.protocol = "brc-20".into();
        assert!(wrong.validate().is_err());
    }
}
