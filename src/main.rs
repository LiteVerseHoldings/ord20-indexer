extern crate core;

mod checkpoint;
#[cfg(all(test, feature = "native-checkpoint-integration"))]
mod checkpoint_integration;
mod database;
mod indexer;
mod models;
#[cfg(all(test, feature = "native-chain-regtest"))]
mod native_chain_regtest;
mod ordinals;
mod schema;
mod util;

use crate::database::Database;
use crate::indexer::Indexer;
use crate::ordinals::Ordinals;
use dotenv::dotenv;
use std::env;
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() {
    // Inspection flags must return before dotenv, database connections or writer locks.
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments == ["--build-info"] {
        println!(
            "{}",
            serde_json::json!({
                "package": "ord20-indexer",
                "repository": "LiteVerseHoldings/ord20-indexer",
                "source_base_revision": option_env!("ORD20_SOURCE_REVISION").unwrap_or("unrecorded"),
                "source_tree_sha256": option_env!("ORD20_SOURCE_TREE_SHA256").unwrap_or("unrecorded"),
                "token_trade_checkpoint_version": 1
            })
        );
        return;
    }
    if !arguments.is_empty() {
        eprintln!("Unsupported argument; only --build-info is supported for read-only inspection");
        exit(2);
    }
    dotenv().ok();
    env_logger::init();

    ctrlc::set_handler(move || {
        if SHUTTING_DOWN.fetch_or(true, Ordering::Relaxed) {
            exit(1);
        }

        SHUTTING_DOWN.store(true, Ordering::SeqCst);
        println!("Ctrl+C received! Shutting down gracefully. Press <CTRL-C> again to shutdown immediately...");
    })
    .expect("Error setting Ctrl+C handler");

    let ordinals_url =
        env::var("ORDINALS_BASE_URL").expect("ORDINALS_BASE_URL must be set in .env file");
    let meta_protocol = env::var("META_PROTOCOL").expect("META_PROTOCOL must be set in .env file");
    let blocks_behind = env::var("BLOCKS_BEHIND")
        .unwrap_or_default()
        .parse::<i32>()
        .unwrap_or(0);

    let database = Database::new().unwrap();
    let ordinals: Ordinals = Ordinals::new(ordinals_url);

    let mut indexer = Indexer {
        ordinals,
        database,
        meta_protocol,
    };

    if let Err(error) = indexer.initialize_checkpoint().await {
        log::error!("Checkpoint initialization refused: {}", error);
        exit(1);
    }

    loop {
        let last_height = indexer
            .database
            .get_status("last_height".to_string())
            .expect("Database has not been correctly initialised. 'last_height' missing.")
            .value
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();

        if let Ok(current_height) = indexer.ordinals.get_block_height().await {
            if last_height < current_height - blocks_behind {
                if let Err(error) = indexer.get_blocks(current_height - blocks_behind).await {
                    log::error!("Indexing stopped: {}", error);
                    exit(1);
                }
            }
        }

        if SHUTTING_DOWN.load(Ordering::Relaxed) {
            log::info!("Shutting down indexer.");
            break;
        }

        sleep(Duration::from_secs(5));
    }
}
