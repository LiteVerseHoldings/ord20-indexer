use crate::checkpoint::CheckResult;
use crate::database::Database;
use crate::models::{Inscriptions, Ord20};
use crate::ordinals::{Block, Inscription, Ordinals};
use crate::util::{bigdecimal_fractional_count, string_to_timestamp};
use crate::SHUTTING_DOWN;
use bigdecimal::{BigDecimal, Zero};
use hex::decode;
use std::convert::Into;
use std::string::ToString;
use std::sync::atomic::Ordering;
use std::time::Instant;

pub struct Indexer {
    pub ordinals: Ordinals,
    pub database: Database,
    pub meta_protocol: String,
}

impl Indexer {
    pub(crate) async fn get_blocks(&mut self, target_block: i32) -> CheckResult<()> {
        loop {
            let previous = self.read_checkpoint()?;
            if previous.height >= target_block || SHUTTING_DOWN.load(Ordering::Relaxed) {
                return Ok(());
            }
            let block = self
                .ordinals
                .get_block(previous.height + 1)
                .await
                .map_err(|_| "Required block unavailable")?;
            let started = Instant::now();
            let count = self.commit_block(&block).await?;
            log::info!(
                "Committed block {}, inscriptions {}, elapsed {:?}",
                block.height,
                count,
                started.elapsed()
            );
        }
    }

    pub(crate) async fn process_block(&mut self, block: &Block) -> CheckResult<i32> {
        let mut inscriptions_count = 0;
        let mut block_miner_address = "";
        for txs in &block.transactions {
            if block_miner_address.is_empty() {
                block_miner_address = txs
                    .output_addresses
                    .first()
                    .map(String::as_str)
                    .unwrap_or("");
            }
            let address = if !txs.output_addresses.is_empty() {
                txs.output_addresses.first().unwrap()
            } else {
                ""
            };
            if txs.outputs.is_empty() || txs.outputs.len() != txs.output_addresses.len() {
                return Err("Incomplete block transaction outputs".into());
            }
            if txs
                .outputs
                .iter()
                .try_fold(0u64, |sum, (_, value)| sum.checked_add(*value))
                .is_none_or(|sum| sum > 8_400_000_000_000_000)
            {
                return Err("Invalid transaction output values".into());
            }
            string_to_timestamp(block.timestamp.as_str()).ok_or("Invalid block timestamp")?;
            let mut fetched_up_to_index = -1;
            let mut input_offset = 0u64;
            for (index, (input, _input_value)) in txs.inputs.iter().enumerate() {
                // Check inputs for transfer inscription
                if let Ok(inscription) = self.database.get_inscription_by_output(input.to_string())
                {
                    if inscription.action == "transfer" && !inscription.spent.unwrap_or_default() {
                        if index > 0 {
                            for backfill_index in (fetched_up_to_index + 1) as usize..index {
                                let (backfill_input, _) = &txs.inputs[backfill_index];

                                let output = self.ordinals.get_output(backfill_input.to_string()).await
                                    .map_err(|_| "Required prior input value unavailable; cannot locate transfer")?;
                                if Some(output.transaction.as_str())
                                    != backfill_input.split(':').next()
                                    || output.value > 8_400_000_000_000_000
                                {
                                    return Err("Prior output identity or value is invalid".into());
                                }
                                input_offset = input_offset
                                    .checked_add(output.value)
                                    .ok_or("Input value overflow")?;
                                fetched_up_to_index = backfill_index as isize;
                            }
                        }

                        let vout =
                            Indexer::calculate_ordinal_position(index, input_offset, &txs.outputs);
                        let address_receiver = if vout > txs.outputs.len() - 1 {
                            // Preserve the organization's existing fee-recipient rule pending ledger audit.
                            block_miner_address
                        } else {
                            txs.output_addresses.get(vout).unwrap()
                        };
                        if address_receiver.is_empty() {
                            return Err("Transfer recipient unavailable".into());
                        }
                        let transfer_inscription = self
                            .database
                            .update_inscription_spent(
                                inscription.id,
                                inscription.genesis_address.clone(),
                                address_receiver.to_string(),
                                txs.transaction.clone(),
                                0,
                                block.height as i64,
                                string_to_timestamp(block.timestamp.as_str())
                                    .ok_or("Invalid block timestamp")?,
                            )
                            .map_err(|_| "Cannot record transfer spend")?;
                        if transfer_inscription.valid.unwrap_or_default() {
                            self.process_inscription_transfer(&transfer_inscription)?;
                        }
                    }
                }
            }
            if !txs.inscriptions.is_empty() && address.is_empty() {
                log::warn!("Empty address on tx {}", txs.transaction);
            }
            for tx_inscription in &txs.inscriptions {
                let inscription = Inscription {
                    number: tx_inscription.inscription_number,
                    offset: 0,
                    genesis_fee: 0,
                    genesis_height: block.height as i64,
                    genesis_transaction: txs.transaction.clone(),
                    inscription_id: tx_inscription.inscription_id.clone(),
                    output: txs.outputs[0].0.clone(),
                    location: "".to_string(),
                    address: "".to_string(),
                    genesis_address: address.to_string(),
                    content_type: tx_inscription.content_type.as_ref().map(|s| {
                        decode(s.as_bytes())
                            .map(|decoded| String::from_utf8_lossy(&decoded).into_owned())
                            .unwrap_or_else(|_| String::new())
                    }),
                    timestamp: block.timestamp.clone(),
                };
                if inscription.number > 0
                    && inscription.content_type.as_ref().is_some_and(|ct| {
                        ct.contains("text/plain") || ct.contains("application/json")
                    })
                {
                    let Some(content) = tx_inscription.content.as_ref() else {
                        continue;
                    };
                    let Ok(decoded) = decode(content) else {
                        continue;
                    };
                    if let Some(inscription) = self.add_inscription(
                        inscription,
                        String::from_utf8_lossy(&decoded).into_owned(),
                    ) {
                        log::debug!("Process Inscription: {:?}", inscription);
                        inscriptions_count += 1;
                        self.process_inscription(&inscription);
                    }
                }
            }
        }
        Ok(inscriptions_count)
    }

    pub(crate) fn calculate_ordinal_position(
        input_index: usize,
        ordinal_offset: u64,
        outputs: &Vec<(String, u64)>,
    ) -> usize {
        let mut output_index = 0;
        let mut output_count = 0;

        // Skip check if ordinal input is first input in transaction
        // Assumptions: ordinal inscribed at offset 0 of input is on output 0
        if input_index == 0 {
            return output_index;
        }

        for (_, output) in outputs {
            output_count += output;
            if output_count > ordinal_offset {
                break;
            }
            output_index += 1;
        }

        output_index
    }

    pub(crate) fn add_inscription(
        &mut self,
        inscription: Inscription,
        content: String,
    ) -> Option<Inscriptions> {
        if let Ok(ord20) = serde_json::from_str::<Ord20>(&content) {
            if ord20.p != self.meta_protocol {
                return None;
            }
            let inscription_id = if inscription.inscription_id.is_empty() {
                format!("{}i{}", inscription.genesis_transaction, inscription.output)
            } else {
                inscription.inscription_id
            };
            // Add to inscription database
            return self
                .database
                .create_inscription(
                    inscription.genesis_transaction,
                    inscription.genesis_address,
                    inscription_id,
                    inscription.number,
                    inscription.genesis_height,
                    inscription.output,
                    string_to_timestamp(inscription.timestamp.as_str()).unwrap(),
                    ord20.tick.to_lowercase(),
                    ord20.op,
                    ord20.max,
                    ord20.lim,
                    ord20.dec,
                    ord20.amt,
                    0,
                )
                .ok();
        }

        None
    }

    pub(crate) fn process_inscription(&mut self, inscription: &Inscriptions) {
        let ticker = self.database.get_tracker(inscription.ticker.clone());

        if &inscription.action == "deploy" {
            if ticker.is_err() {
                let decimal = inscription.decimal.unwrap_or(18);

                if !(0..=18).contains(&decimal) {
                    return;
                }

                #[allow(clippy::needless_as_bytes)]
                if inscription.ticker.as_bytes().len() != 4 {
                    return;
                }

                if inscription.supply.clone().unwrap_or_default() <= BigDecimal::zero()
                    || inscription
                        .supply
                        .clone()
                        .unwrap_or(BigDecimal::from(i64::MAX - 1))
                        > i64::MAX.into()
                    || inscription.limit_mint.clone().unwrap_or_default() < BigDecimal::zero()
                {
                    return;
                }

                self.database.create_tracker(
                    inscription.ticker.clone(),
                    inscription.inscription_id.clone(),
                    inscription.inscription_num,
                    inscription.supply.clone().unwrap_or_default(),
                    inscription.limit_mint.clone().unwrap_or_default(),
                    decimal,
                );
            }
        } else if &inscription.action == "mint" {
            if let Ok(ticker) = ticker {
                let limit = ticker.limit_mint;

                let amount = inscription.amount.clone().unwrap_or_default();

                if amount <= BigDecimal::zero()
                    || bigdecimal_fractional_count(amount.clone()) > ticker.decimals as u32
                {
                    return;
                }

                if (limit == BigDecimal::zero() || limit >= amount)
                    && ticker.supply != ticker.supply_minted.clone()
                {
                    if ticker.supply_minted.clone() == BigDecimal::zero() {
                        let _ = self
                            .database
                            .update_tracker_mint_start(ticker.id, inscription.inscription_num);
                    }
                    if ticker.supply_minted.clone() + amount.clone() >= ticker.supply {
                        let _ = self
                            .database
                            .update_tracker_mint_end(ticker.id, inscription.inscription_num);
                    }

                    let mut mint_balance = amount.clone();
                    let mut new_supply = ticker.supply_minted + mint_balance.clone();

                    if new_supply > ticker.supply {
                        mint_balance = amount.clone() - (new_supply - ticker.supply.clone());
                        new_supply = ticker.supply;
                    }

                    let _ = self.database.update_tracker_minted(ticker.id, new_supply);

                    let mut transfer_balance = BigDecimal::zero();

                    if let Ok(account) = self
                        .database
                        .get_balance(inscription.genesis_address.clone(), ticker.ticker.clone())
                    {
                        mint_balance = account.balance + mint_balance.clone();
                        transfer_balance = account.transfer_balance + transfer_balance;
                    } else {
                        let _ = self.database.create_balance(
                            inscription.genesis_address.clone(),
                            ticker.ticker.clone(),
                        );
                        let _ = self
                            .database
                            .update_tracker_holders(ticker.id, ticker.holders + 1);
                    }

                    let _ = self.database.update_balance(
                        inscription.genesis_address.clone(),
                        ticker.ticker.clone(),
                        mint_balance,
                        transfer_balance,
                    );

                    let _ = self.database.create_history(
                        "".to_string(),
                        inscription.genesis_address.clone(),
                        inscription.amount.clone().unwrap(),
                        ticker.ticker.clone(),
                        inscription.action.clone(),
                        false,
                        inscription.inscription_id.clone(),
                        inscription.inscription_num,
                        inscription.genesis_tx_id.clone(),
                        inscription.height,
                        inscription.timestamp,
                    );
                }
            }
        } else if &inscription.action == "transfer" && inscription.amount.is_some() {
            if let Ok(ticker) = ticker {
                if let Ok(account) = self
                    .database
                    .get_balance(inscription.genesis_address.clone(), ticker.ticker.clone())
                {
                    let amount = inscription.amount.clone().unwrap();

                    if amount <= BigDecimal::zero()
                        || bigdecimal_fractional_count(amount.clone()) > ticker.decimals as u32
                    {
                        return;
                    }

                    let mut invalid = false;
                    if account.balance >= amount {
                        let new_balance = account.balance - amount.clone();
                        let transfer_balance = account.transfer_balance + amount.clone();

                        let _ = self.database.update_balance(
                            inscription.genesis_address.clone(),
                            ticker.ticker.clone(),
                            new_balance,
                            transfer_balance,
                        );
                    } else {
                        invalid = true;
                    }

                    let _ = self.database.create_history(
                        inscription.genesis_address.clone(),
                        "".to_string(),
                        inscription.amount.clone().unwrap(),
                        ticker.ticker.clone(),
                        inscription.action.clone(),
                        invalid,
                        inscription.inscription_id.clone(),
                        inscription.inscription_num,
                        inscription.genesis_tx_id.clone(),
                        inscription.height,
                        inscription.timestamp,
                    );

                    let _ = self
                        .database
                        .update_inscription_valid(inscription.id, !invalid);
                }
            }
        }
    }

    pub(crate) fn process_inscription_transfer(
        &mut self,
        inscription: &Inscriptions,
    ) -> CheckResult<()> {
        use diesel::OptionalExtension;
        let amount = inscription
            .amount
            .clone()
            .ok_or("Missing transfer amount")?;
        if amount <= BigDecimal::zero() {
            return Err("Invalid transfer amount".into());
        }
        let sender = inscription
            .address_sender
            .clone()
            .ok_or("Missing transfer sender")?;
        let receiver = inscription
            .address_receiver
            .clone()
            .ok_or("Missing transfer recipient")?;
        let sender_balance = self
            .database
            .get_balance(sender.clone(), inscription.ticker.clone())
            .map_err(|_| "Transfer sender balance unavailable")?;
        let tracker = self
            .database
            .get_tracker(inscription.ticker.clone())
            .map_err(|_| "Transfer token unavailable")?;
        if sender_balance.transfer_balance < amount {
            return Err("Insufficient reserved transfer balance".into());
        }
        let prior_receiver = self
            .database
            .get_balance(receiver.clone(), inscription.ticker.clone())
            .optional()
            .map_err(|_| "Cannot read recipient balance")?;
        let mut holders_change = 0;
        let (receiver_balance, receiver_reserved) = if let Some(account) = prior_receiver {
            if account.balance == BigDecimal::zero()
                && account.transfer_balance == BigDecimal::zero()
            {
                holders_change += 1;
            }
            (account.balance, account.transfer_balance)
        } else {
            self.database
                .create_balance(receiver.clone(), inscription.ticker.clone())
                .map_err(|_| "Cannot create recipient balance")?;
            holders_change += 1;
            (BigDecimal::zero(), BigDecimal::zero())
        };
        let remaining = sender_balance.transfer_balance - amount.clone();
        if sender_balance.balance == BigDecimal::zero() && remaining == BigDecimal::zero() {
            holders_change -= 1;
        }
        if sender == receiver {
            // A self-send converts the reservation back into available balance.
            holders_change = 0;
            self.database
                .update_balance(
                    sender.clone(),
                    inscription.ticker.clone(),
                    receiver_balance + amount.clone(),
                    remaining,
                )
                .map_err(|_| "Cannot update self transfer")?;
        } else {
            self.database
                .update_balance(
                    sender.clone(),
                    inscription.ticker.clone(),
                    sender_balance.balance,
                    remaining,
                )
                .map_err(|_| "Cannot debit transfer reservation")?;
            self.database
                .update_balance(
                    receiver.clone(),
                    inscription.ticker.clone(),
                    receiver_balance + amount.clone(),
                    receiver_reserved,
                )
                .map_err(|_| "Cannot credit transfer recipient")?;
        }
        self.database
            .update_tracker_holders(tracker.id, tracker.holders + holders_change)
            .map_err(|_| "Cannot update holders")?;
        self.database
            .update_tracker_transactions(tracker.id, tracker.transactions + 1)
            .map_err(|_| "Cannot update transfer count")?;
        self.database
            .create_history(
                sender,
                receiver,
                amount,
                inscription.ticker.clone(),
                "send".into(),
                false,
                inscription.inscription_id.clone(),
                inscription.inscription_num,
                inscription
                    .spent_tx
                    .clone()
                    .ok_or("Missing spending transaction")?,
                inscription.spent_height.ok_or("Missing spending height")?,
                inscription
                    .spent_timestamp
                    .ok_or("Missing spending timestamp")?,
            )
            .map_err(|_| "Cannot record transfer history")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::indexer::Indexer;

    #[test]
    fn test_calculate_ordinal_position_01() {
        let ordinal_offset = 10;
        let outputs = vec![
            ("output1".to_string(), 10),
            ("output2".to_string(), 5),
            ("output3".to_string(), 20),
        ];
        assert_eq!(
            Indexer::calculate_ordinal_position(usize::MAX, ordinal_offset, &outputs),
            1
        );
    }

    #[test]
    fn test_calculate_ordinal_position_02() {
        let ordinal_offset = 0;
        let outputs = vec![
            ("output1".to_string(), 10),
            ("output2".to_string(), 5),
            ("output3".to_string(), 20),
        ];
        assert_eq!(
            Indexer::calculate_ordinal_position(usize::MAX, ordinal_offset, &outputs),
            0
        );
    }

    #[test]
    fn test_calculate_ordinal_position_03() {
        let ordinal_offset = 40;
        let outputs = vec![
            ("output1".to_string(), 10),
            ("output2".to_string(), 5),
            ("output3".to_string(), 20),
        ];
        assert_eq!(
            Indexer::calculate_ordinal_position(usize::MAX, ordinal_offset, &outputs),
            3
        );
    }
}
