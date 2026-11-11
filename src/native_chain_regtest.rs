//! Fake-coin native chain replay. This module cannot be compiled into the service binary.
use crate::{checkpoint::CHECKPOINT_KEY, checkpoint_integration::isolated_native_indexer};
use bigdecimal::BigDecimal;
use diesel::{connection::SimpleConnection, OptionalExtension};
use serde_json::Value;
use std::{fs, str::FromStr};

#[tokio::test]
async fn real_regtest_blocks_credit_exact_tokens_after_actual_wallet_signed_swap() {
    let url =
        std::env::var("NATIVE_ORD_REGTEST_URL").expect("An explicit local regtest Ord is required");
    assert_eq!(url, "http://127.0.0.1:18594/");
    let mut indexer = isolated_native_indexer(url);
    let status: Value =
        serde_json::from_str(&indexer.ordinals.get_status().await.unwrap()).unwrap();
    assert_eq!(status["chain"], "regtest");
    let height = indexer.ordinals.get_block_height().await.unwrap();
    assert!(
        (100..1000).contains(&height),
        "Only the tiny synthetic chain is permitted"
    );
    // A regtest ledger must never receive a marker asserting mainnet readiness.
    assert!(indexer.initialize_checkpoint().await.is_err());
    let fixtures: Vec<Value> = ["segwit-canonical", "taproot-canonical"]
        .iter()
        .map(|label| {
            serde_json::from_str(
                &fs::read_to_string(format!("/tmp/ltc20-native-regtest/{label}/quote.json"))
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    let mut checked_swaps = 0;
    for block_height in 0..=height {
        let block = indexer.ordinals.get_block(block_height).await.unwrap();
        assert_eq!(block.height, block_height);
        for transaction in &block.transactions {
            assert_eq!(
                transaction.outputs.len(),
                transaction.output_addresses.len(),
                "Non-address outputs shifted the feed's recipient positions"
            );
        }
        indexer.database.connection.batch_execute("BEGIN").unwrap();
        // Disposable addresses may already have other valid test payments. Verify
        // this swap's exact balance delta, rather than assuming a zero starting balance.
        let mut before = Vec::new();
        for fixture in &fixtures {
            if let Some(transaction) = block.transactions.iter().find(|transaction| {
                Some(transaction.transaction.as_str()) == fixture["quote"]["txid"].as_str()
            }) {
                let record = indexer
                    .database
                    .get_inscription_by_output(
                        fixture["input_plan"]["inputs"][1]["output"]
                            .as_str()
                            .unwrap()
                            .into(),
                    )
                    .unwrap();
                let receiver = transaction.output_addresses[1].clone();
                let seller_balance = indexer
                    .database
                    .get_balance(receiver.clone(), record.ticker.clone())
                    .optional()
                    .unwrap()
                    .map(|account| account.balance)
                    .unwrap_or_else(|| BigDecimal::from(0));
                let buyer = indexer
                    .database
                    .get_balance(record.genesis_address.clone(), record.ticker.clone())
                    .unwrap();
                before.push((
                    receiver,
                    record.genesis_address,
                    record.ticker,
                    seller_balance,
                    buyer.transfer_balance,
                    buyer.balance,
                ));
            }
        }
        indexer.process_block(&block).await.unwrap();
        for (receiver, sender, ticker, seller_before, reserved_before, available_before) in before {
            let seller = indexer
                .database
                .get_balance(receiver, ticker.clone())
                .unwrap();
            let buyer = indexer.database.get_balance(sender, ticker).unwrap();
            let amount = BigDecimal::from_str("10.5").unwrap();
            assert_eq!(seller.balance - seller_before, amount);
            assert_eq!(reserved_before - buyer.transfer_balance, amount);
            assert_eq!(buyer.balance, available_before);
            checked_swaps += 1;
        }
        indexer
            .database
            .connection
            .batch_execute("SELECT 1; COMMIT")
            .unwrap();
    }
    for label in ["segwit-canonical", "taproot-canonical"] {
        let fixture: Value = serde_json::from_str(
            &fs::read_to_string(format!("/tmp/ltc20-native-regtest/{label}/quote.json")).unwrap(),
        )
        .unwrap();
        let payment = fixture["input_plan"]["inputs"][1]["output"]
            .as_str()
            .unwrap();
        let transfer = indexer
            .database
            .get_inscription_by_output(payment.into())
            .unwrap();
        assert_eq!(transfer.valid, Some(true));
        assert_eq!(transfer.spent, Some(true));
        assert_eq!(transfer.amount, Some(BigDecimal::from_str("10.5").unwrap()));
        assert_eq!(
            transfer.spent_tx.as_deref(),
            fixture["quote"]["txid"].as_str()
        );
        let receipt = indexer
            .ordinals
            .get_output(format!("{}:1", fixture["quote"]["txid"].as_str().unwrap()))
            .await
            .unwrap();
        assert_eq!(
            transfer.address_receiver.as_deref(),
            Some(receipt.address.as_str())
        );
    }
    assert_eq!(checked_swaps, 2);
    assert!(indexer
        .database
        .get_status(CHECKPOINT_KEY.into())
        .optional()
        .unwrap()
        .is_none());
}
