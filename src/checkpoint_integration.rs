//! Runs only against the disposable, synthetic database created by tests/run-isolated.sh.
use crate::checkpoint::{Checkpoint, CHECKPOINT_KEY, INITIAL_HEIGHT, WRITER_LOCK};
use crate::{
    database::Database,
    indexer::Indexer,
    ordinals::{Block, Ordinals, Transaction, TransactionInscription},
};
use diesel::{connection::SimpleConnection, prelude::*};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

static COUNTER: AtomicUsize = AtomicUsize::new(0);
#[allow(dead_code, unused_imports)]
#[path = "../tests/fixtures/legacy_indexer.rs"]
mod legacy;
const DATABASE: &str =
    "postgresql:///token_trade_tests?host=/tmp/token-trade-pg&port=55432&user=postgres";

struct Responses {
    routes: HashMap<String, (u16, String)>,
    reorg_after_output: bool,
}
struct MockOrd {
    url: String,
    responses: Arc<Mutex<Responses>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}
impl MockOrd {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let mut routes = HashMap::new();
        routes.insert("/status".into(), (200, json!({"chain":"mainnet","height":INITIAL_HEIGHT+20,"unrecoverably_reorged":false,"inscriptions":100,"inscription_index":true}).to_string()));
        routes.insert(
            format!("/blockhash/{INITIAL_HEIGHT}"),
            (200, "a".repeat(64)),
        );
        routes.insert(
            format!("/blockhash/{}", INITIAL_HEIGHT + 1),
            (200, "b".repeat(64)),
        );
        let responses = Arc::new(Mutex::new(Responses {
            routes,
            reorg_after_output: false,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (shared, quitting) = (responses.clone(), stop.clone());
        let handle = thread::spawn(move || {
            while !quitting.load(Ordering::Relaxed) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut data = [0; 8192];
                let Ok(count) = stream.read(&mut data) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&data[..count]);
                let path = request.split_whitespace().nth(1).unwrap_or("");
                let (code, body) = {
                    let mut state = shared.lock().unwrap();
                    let response = state
                        .routes
                        .get(path)
                        .cloned()
                        .unwrap_or((404, "missing".into()));
                    if state.reorg_after_output && path.starts_with("/output/") {
                        state.routes.insert(
                            format!("/blockhash/{}", INITIAL_HEIGHT + 1),
                            (200, "c".repeat(64)),
                        );
                    }
                    response
                };
                let _ = write!(stream, "HTTP/1.1 {code} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
            }
        });
        Self {
            url,
            responses,
            stop,
            handle: Some(handle),
        }
    }
    fn route(&self, path: String, value: Value) {
        self.responses
            .lock()
            .unwrap()
            .routes
            .insert(path, (200, value.to_string()));
    }
}
impl Drop for MockOrd {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

fn indexer(server: &MockOrd) -> (Indexer, String) {
    assert_eq!(
        std::env::var("TEST_DATABASE_URL").unwrap(),
        DATABASE,
        "Integration tests require the isolated synthetic test database"
    );
    let mut connection = PgConnection::establish(DATABASE).unwrap();
    let schema = format!(
        "token_trade_test_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    connection
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path = {schema};"
        ))
        .unwrap();
    for migration in [
        include_str!("../migrations/0001_create_balances/up.sql"),
        include_str!("../migrations/0002_create_tracker/up.sql"),
        include_str!("../migrations/0003_create_history/up.sql"),
        include_str!("../migrations/0004_create_inscriptions/up.sql"),
        include_str!("../migrations/0005_create_status/up.sql"),
    ] {
        connection.batch_execute(migration).unwrap();
    }
    (
        Indexer {
            database: Database { connection },
            ordinals: Ordinals::new(server.url.clone()),
            meta_protocol: "ltc-20".into(),
        },
        schema,
    )
}
#[cfg(feature = "native-chain-regtest")]
pub(super) fn isolated_native_indexer(url: String) -> Indexer {
    let server = MockOrd::new();
    let (mut value, _) = indexer(&server);
    value.ordinals = Ordinals::new(url);
    value
}

fn checkpoint(indexer: &mut Indexer) -> Checkpoint {
    indexer.read_checkpoint().unwrap()
}
fn empty_block() -> Block {
    Block {
        hash: "b".repeat(64),
        height: INITIAL_HEIGHT + 1,
        previous_blockhash: "a".repeat(64),
        size: 1,
        target: "0".into(),
        timestamp: "2026-11-11 00:00:00 UTC".into(),
        transactions: vec![],
        weight: 1,
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("../tests/fixtures/ltc20-swap.json")).unwrap()
}

#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

fn synthetic_rows(connection: &mut PgConnection, table: &str) -> Vec<Value> {
    assert!(["balances", "tracker", "inscriptions", "history"].contains(&table));
    diesel::sql_query(format!(
        "SELECT row_to_json(item)::text AS value FROM (SELECT * FROM {table} ORDER BY id) item"
    ))
    .load::<JsonRow>(connection)
    .unwrap()
    .into_iter()
    .map(|row| serde_json::from_str(&row.value).unwrap())
    .collect()
}

#[tokio::test]
async fn shadow_replay_matches_organization_ledger_and_corrects_only_send_history_identity() {
    let server = MockOrd::new();
    let (mut candidate, _) = indexer(&server);
    let (baseline, _) = indexer(&server);
    let mut baseline = legacy::LegacyIndexer {
        database: baseline.database,
        ordinals: baseline.ordinals,
        meta_protocol: baseline.meta_protocol,
    };
    candidate.initialize_checkpoint().await.unwrap();
    let reserve = reservation_block();
    candidate.commit_block(&reserve).await.unwrap();
    baseline.process_block(&reserve).await;
    let f = fixture();
    let nft_output = f["plan"]["inputs"][0]["output"].as_str().unwrap();
    server.route(format!("/output/{nft_output}"), json!({"address":f["plan"]["seller_address"],"transaction":nft_output.split(':').next().unwrap(),"value":10000}));
    server.responses.lock().unwrap().routes.insert(
        format!("/blockhash/{}", INITIAL_HEIGHT + 2),
        (200, "c".repeat(64)),
    );
    let mut swap = swap_block();
    swap.height = INITIAL_HEIGHT + 2;
    swap.previous_blockhash = "b".repeat(64);
    swap.hash = "c".repeat(64);
    candidate.commit_block(&swap).await.unwrap();
    baseline.process_block(&swap).await;
    for table in ["balances", "tracker", "inscriptions"] {
        assert_eq!(
            synthetic_rows(&mut candidate.database.connection, table),
            synthetic_rows(&mut baseline.database.connection, table),
            "Shadow ledger differs in {table}"
        );
    }
    let actual = synthetic_rows(&mut candidate.database.connection, "history");
    let mut historical = synthetic_rows(&mut baseline.database.connection, "history");
    assert_eq!(actual.len(), historical.len());
    let send = historical
        .iter_mut()
        .find(|item| item["action"] == "send")
        .unwrap();
    assert_ne!(send["tx_id"], f["txid"]);
    send["tx_id"] = f["txid"].clone();
    send["height"] = json!(swap.height);
    send["timestamp"] = actual.last().unwrap()["timestamp"].clone();
    assert_eq!(
        actual, historical,
        "Only the send receipt's transaction/height/time should change"
    );
}

#[tokio::test]
async fn duplicate_genesis_output_rejection_preserves_legacy_ledger_and_commits_the_block() {
    let server = MockOrd::new();
    let (mut candidate, _) = indexer(&server);
    let (baseline, _) = indexer(&server);
    let mut baseline = legacy::LegacyIndexer {
        database: baseline.database,
        ordinals: baseline.ordinals,
        meta_protocol: baseline.meta_protocol,
    };
    candidate.initialize_checkpoint().await.unwrap();
    let mut block = reservation_block();
    let mint = &mut block.transactions[1];
    for index in 1..=4 {
        mint.inscriptions.push(TransactionInscription {
            inscription_id: format!("{}i{index}", mint.transaction),
            inscription_number: 3 + index,
            content_type: Some(hex::encode("application/json")),
            content: Some(hex::encode(
                json!({"p":"ltc-20","op":"mint","tick":"lite","amt":"100"}).to_string(),
            )),
        });
    }
    block.transactions.push(inscription_tx(
        "f".repeat(64),
        8,
        json!({"p":"ltc-20","op":"mint","tick":"lite","amt":"5"}),
    ));
    assert_eq!(candidate.commit_block(&block).await.unwrap(), 4);
    baseline.process_block(&block).await;
    for table in ["balances", "tracker", "inscriptions", "history"] {
        assert_eq!(
            synthetic_rows(&mut candidate.database.connection, table),
            synthetic_rows(&mut baseline.database.connection, table),
            "Expected duplicate-output rejection changed the legacy ledger in {table}"
        );
    }
    let buyer = fixture()["plan"]["buyer_address"]
        .as_str()
        .unwrap()
        .to_owned();
    let balance = candidate
        .database
        .get_balance(buyer, "lite".into())
        .unwrap();
    assert_eq!(balance.balance, 15.into());
    assert_eq!(balance.transfer_balance, 10.into());
    assert_eq!(
        candidate
            .database
            .get_tracker("lite".into())
            .unwrap()
            .supply_minted,
        25.into()
    );
    assert_eq!(checkpoint(&mut candidate).height, INITIAL_HEIGHT + 1);
}

#[tokio::test]
async fn duplicate_inscription_identity_on_a_different_output_still_rolls_back() {
    for duplicate_number in [false, true] {
        let server = MockOrd::new();
        let (mut candidate, _) = indexer(&server);
        candidate.initialize_checkpoint().await.unwrap();
        let mut block = reservation_block();
        if duplicate_number {
            block.transactions[1].inscriptions[0].inscription_number =
                block.transactions[0].inscriptions[0].inscription_number;
        } else {
            block.transactions[1].inscriptions[0].inscription_id =
                block.transactions[0].inscriptions[0].inscription_id.clone();
        }
        assert!(candidate.commit_block(&block).await.is_err());
        assert_eq!(checkpoint(&mut candidate).height, INITIAL_HEIGHT);
        for table in ["balances", "tracker", "inscriptions", "history"] {
            assert!(synthetic_rows(&mut candidate.database.connection, table).is_empty());
        }
    }
}

#[tokio::test]
async fn genesis_uses_actual_first_output_instead_of_inscription_id_suffix() {
    let server = MockOrd::new();
    let (mut candidate, _) = indexer(&server);
    candidate.initialize_checkpoint().await.unwrap();
    let mut reserve = reservation_block();
    let transfer = reserve.transactions.last_mut().unwrap();
    transfer.inscriptions[0].inscription_id = format!("{}i1", transfer.transaction);
    candidate.commit_block(&reserve).await.unwrap();
    let f = fixture();
    let record = candidate
        .database
        .get_inscription_by_output(f["plan"]["inputs"][1]["output"].as_str().unwrap().into())
        .unwrap();
    assert!(record.inscription_id.ends_with("i1"));
    assert!(record.output.ends_with(":0"));
    assert_eq!(record.valid, Some(true));
}

fn inscription_tx(txid: String, number: i64, content: Value) -> Transaction {
    Transaction {
        transaction: txid.clone(),
        inputs: vec![],
        outputs: vec![(format!("{txid}:0"), 10000)],
        output_addresses: vec![fixture()["plan"]["buyer_address"].as_str().unwrap().into()],
        inscriptions: vec![TransactionInscription {
            inscription_id: format!("{txid}i0"),
            inscription_number: number,
            content_type: Some(hex::encode("application/json")),
            content: Some(hex::encode(content.to_string())),
        }],
    }
}

fn reservation_block() -> Block {
    let mut block = empty_block();
    let transfer_txid = fixture()["plan"]["inputs"][1]["output"]
        .as_str()
        .unwrap()
        .split(':')
        .next()
        .unwrap()
        .to_string();
    block.transactions = vec![
        inscription_tx(
            "d".repeat(64),
            1,
            json!({"p":"ltc-20","op":"deploy","tick":"lite","max":"1000","lim":"100","dec":2}),
        ),
        inscription_tx(
            "e".repeat(64),
            2,
            json!({"p":"ltc-20","op":"mint","tick":"lite","amt":"20"}),
        ),
        inscription_tx(
            transfer_txid,
            3,
            json!({"p":"ltc-20","op":"transfer","tick":"lite","amt":"10"}),
        ),
    ];
    block
}

#[tokio::test]
async fn mint_counts_a_zero_balance_account_as_a_holder_again() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    let mut initial = reservation_block();
    initial.transactions.pop();
    indexer.commit_block(&initial).await.unwrap();
    let f = fixture();
    let buyer = f["plan"]["buyer_address"].as_str().unwrap().to_string();
    let seller = f["plan"]["seller_address"].as_str().unwrap().to_string();

    // Reserve the full balance, then spend it to a different holder.
    let mut reserve = empty_block();
    reserve.height = INITIAL_HEIGHT + 2;
    reserve.previous_blockhash = "b".repeat(64);
    reserve.hash = "c".repeat(64);
    let txid = f["plan"]["inputs"][1]["output"]
        .as_str()
        .unwrap()
        .split(':')
        .next()
        .unwrap()
        .to_string();
    reserve.transactions.push(inscription_tx(
        txid,
        3,
        json!({"p":"ltc-20","op":"transfer","tick":"lite","amt":"20"}),
    ));
    server.responses.lock().unwrap().routes.insert(
        format!("/blockhash/{}", reserve.height),
        (200, reserve.hash.clone()),
    );
    indexer.commit_block(&reserve).await.unwrap();
    assert_eq!(
        indexer.database.get_tracker("lite".into()).unwrap().holders,
        1
    );
    let nft = f["plan"]["inputs"][0]["output"].as_str().unwrap();
    server.route(
        format!("/output/{nft}"),
        json!({"address":seller,
        "transaction":nft.split(':').next().unwrap(),"value":10000}),
    );
    let mut spend = swap_block();
    spend.height = INITIAL_HEIGHT + 3;
    spend.previous_blockhash = "c".repeat(64);
    spend.hash = "d".repeat(64);
    server.responses.lock().unwrap().routes.insert(
        format!("/blockhash/{}", spend.height),
        (200, spend.hash.clone()),
    );
    indexer.commit_block(&spend).await.unwrap();
    let empty = indexer
        .database
        .get_balance(buyer.clone(), "lite".into())
        .unwrap();
    assert_eq!(empty.balance, 0.into());
    assert_eq!(empty.transfer_balance, 0.into());
    assert_eq!(
        indexer.database.get_tracker("lite".into()).unwrap().holders,
        1
    );

    let mut remint = empty_block();
    remint.height = INITIAL_HEIGHT + 4;
    remint.previous_blockhash = "d".repeat(64);
    remint.hash = "e".repeat(64);
    remint.transactions.push(inscription_tx(
        "f".repeat(64),
        4,
        json!({"p":"ltc-20","op":"mint","tick":"lite","amt":"5"}),
    ));
    server.responses.lock().unwrap().routes.insert(
        format!("/blockhash/{}", remint.height),
        (200, remint.hash.clone()),
    );
    let before: Vec<_> = ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .map(|table| synthetic_rows(&mut indexer.database.connection, table))
        .collect();
    indexer.database.connection.batch_execute(
        "ALTER TABLE history ADD CONSTRAINT synthetic_remint_failure CHECK (action <> 'mint') NOT VALID"
    ).unwrap();
    assert!(indexer.commit_block(&remint).await.is_err());
    assert_eq!(checkpoint(&mut indexer).height, spend.height);
    for (table, expected) in ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .zip(before)
    {
        assert_eq!(
            synthetic_rows(&mut indexer.database.connection, table),
            expected,
            "A failed remint must roll back holder bookkeeping and every ledger mutation"
        );
    }
    indexer
        .database
        .connection
        .batch_execute("ALTER TABLE history DROP CONSTRAINT synthetic_remint_failure")
        .unwrap();
    indexer.commit_block(&remint).await.unwrap();
    assert_eq!(
        indexer
            .database
            .get_balance(buyer, "lite".into())
            .unwrap()
            .balance,
        5.into()
    );
    assert_eq!(
        indexer
            .database
            .get_balance(seller, "lite".into())
            .unwrap()
            .balance,
        20.into()
    );
    let tracker = indexer.database.get_tracker("lite".into()).unwrap();
    assert_eq!(tracker.supply_minted, 25.into());
    assert_eq!(
        tracker.holders, 2,
        "A positive mint restores ownership to an existing zero account"
    );
    assert_eq!(checkpoint(&mut indexer).height, remint.height);
}

#[tokio::test]
async fn mint_does_not_double_count_available_or_reserved_holders() {
    for (reserved, new_recipient) in [
        (None, false),
        (Some(10), false),
        (Some(20), false),
        (None, true),
    ] {
        let server = MockOrd::new();
        let (mut indexer, _) = indexer(&server);
        indexer.initialize_checkpoint().await.unwrap();
        let mut initial = reservation_block();
        if let Some(amount) = reserved {
            initial.transactions[2].inscriptions[0].content = Some(hex::encode(
                json!({"p":"ltc-20","op":"transfer","tick":"lite","amt":amount.to_string()})
                    .to_string(),
            ));
        } else {
            initial.transactions.pop();
        }
        indexer.commit_block(&initial).await.unwrap();
        let f = fixture();
        let buyer = f["plan"]["buyer_address"].as_str().unwrap().to_string();
        let recipient = f["plan"][if new_recipient {
            "seller_address"
        } else {
            "buyer_address"
        }]
        .as_str()
        .unwrap()
        .to_string();
        let mut mint = empty_block();
        mint.height = INITIAL_HEIGHT + 2;
        mint.previous_blockhash = "b".repeat(64);
        mint.hash = "c".repeat(64);
        for (txid, number) in [("f".repeat(64), 4), ("1".repeat(64), 5)] {
            let mut tx = inscription_tx(
                txid,
                number,
                json!({"p":"ltc-20","op":"mint","tick":"lite","amt":"5"}),
            );
            tx.output_addresses[0] = recipient.clone();
            mint.transactions.push(tx);
        }
        server.responses.lock().unwrap().routes.insert(
            format!("/blockhash/{}", mint.height),
            (200, mint.hash.clone()),
        );
        indexer.commit_block(&mint).await.unwrap();
        let tracker = indexer.database.get_tracker("lite".into()).unwrap();
        assert_eq!(tracker.holders, if new_recipient { 2 } else { 1 });
        assert_eq!(tracker.supply_minted, 30.into());
        let balance = indexer
            .database
            .get_balance(recipient, "lite".into())
            .unwrap();
        assert_eq!(
            balance.balance,
            (if new_recipient {
                10
            } else {
                30 - reserved.unwrap_or(0)
            })
            .into()
        );
        assert_eq!(
            indexer
                .database
                .get_balance(buyer, "lite".into())
                .unwrap()
                .transfer_balance,
            reserved.unwrap_or(0).into()
        );
    }
}

fn recount(schema: &str, expected: Option<&Checkpoint>) -> std::process::Output {
    let mut command = std::process::Command::new("psql");
    command.args([
        "-X",
        "-A",
        "-t",
        "-q",
        "-h",
        "/tmp/token-trade-pg",
        "-p",
        "55432",
        "-U",
        "postgres",
        "-d",
        "token_trade_tests",
        "-f",
        "ops/validation/recount_holders.sql",
    ]);
    command.env("PGOPTIONS", format!("-c search_path={schema}"));
    if let Some(checkpoint) = expected {
        command.args([
            "-v",
            "apply=true",
            "-v",
            &format!("expected_height={}", checkpoint.height),
            "-v",
            &format!("expected_hash={}", checkpoint.block_hash),
        ]);
    }
    command.output().unwrap()
}

#[tokio::test]
async fn holder_recount_is_read_only_by_default_and_repairs_only_the_cached_counts() {
    let server = MockOrd::new();
    let (mut indexer, schema) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    let mut reserve = reservation_block();
    reserve.transactions[2].inscriptions[0].content = Some(hex::encode(
        json!({"p":"ltc-20","op":"transfer","tick":"lite","amt":"20"}).to_string(),
    ));
    indexer.commit_block(&reserve).await.unwrap();
    indexer.database.create_tracker(
        "none".into(),
        format!("{}i0", "1".repeat(64)),
        4,
        1000.into(),
        100.into(),
        2,
    );
    indexer
        .database
        .create_balance("synthetic-zero-account".into(), "lite".into())
        .unwrap();
    indexer
        .database
        .connection
        .batch_execute("UPDATE tracker SET holders=99")
        .unwrap();
    let before: Vec<_> = ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .map(|table| synthetic_rows(&mut indexer.database.connection, table))
        .collect();
    let checkpoint_before = checkpoint(&mut indexer);
    let audit = recount(&schema, None);
    assert!(audit.status.success());
    let summary: Value = serde_json::from_slice(&audit.stdout).unwrap();
    assert_eq!(summary["holder_count_mismatches"], 2);
    for (table, expected) in ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .zip(&before)
    {
        assert_eq!(
            &synthetic_rows(&mut indexer.database.connection, table),
            expected
        );
    }
    for wrong_height in [false, true] {
        let mut wrong = checkpoint_before.clone();
        if wrong_height {
            wrong.height += 1;
        } else {
            wrong.block_hash = "f".repeat(64);
        }
        assert!(!recount(&schema, Some(&wrong)).status.success());
        assert_eq!(
            synthetic_rows(&mut indexer.database.connection, "tracker"),
            before[1]
        );
    }
    let checkpoint_text = indexer
        .database
        .get_status(CHECKPOINT_KEY.into())
        .unwrap()
        .value
        .unwrap();
    for (field, value) in [
        ("ready", json!("true")),
        ("version", json!("1")),
        ("height", json!(checkpoint_before.height.to_string())),
    ] {
        let mut malformed: Value = serde_json::from_str(&checkpoint_text).unwrap();
        malformed[field] = value;
        indexer
            .database
            .update_status(CHECKPOINT_KEY.into(), malformed.to_string())
            .unwrap();
        assert!(!recount(&schema, Some(&checkpoint_before)).status.success());
        assert_eq!(
            synthetic_rows(&mut indexer.database.connection, "tracker"),
            before[1]
        );
    }
    indexer
        .database
        .update_status(CHECKPOINT_KEY.into(), checkpoint_text)
        .unwrap();
    let mut competing = PgConnection::establish(DATABASE).unwrap();
    competing
        .batch_execute(&format!(
            "SET search_path={schema}; BEGIN; LOCK TABLE tracker IN ROW EXCLUSIVE MODE"
        ))
        .unwrap();
    assert!(!recount(&schema, Some(&checkpoint_before)).status.success());
    competing.batch_execute("ROLLBACK").unwrap();
    let repaired = recount(&schema, Some(&checkpoint_before));
    assert!(repaired.status.success());
    let summary: Value = serde_json::from_slice(&repaired.stdout).unwrap();
    assert_eq!(summary["holder_count_mismatches"], 0);
    assert_eq!(summary["holder_rows_updated"], 2);
    assert_eq!(summary["other_ledger_writes"], 0);
    assert_eq!(
        indexer.database.get_tracker("lite".into()).unwrap().holders,
        1
    );
    assert_eq!(
        indexer.database.get_tracker("none".into()).unwrap().holders,
        0
    );
    for (table, expected) in ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .zip(before)
    {
        let mut after = synthetic_rows(&mut indexer.database.connection, table);
        let mut expected = expected;
        if *table == "tracker" {
            for row in after.iter_mut().chain(expected.iter_mut()) {
                row.as_object_mut().unwrap().remove("holders");
            }
        }
        assert_eq!(
            after, expected,
            "Recount changed fields outside the holder cache"
        );
    }
    assert_eq!(checkpoint(&mut indexer), checkpoint_before);
    let (mut legacy, legacy_schema) = self::indexer(&server);
    assert!(!recount(&legacy_schema, Some(&checkpoint_before))
        .status
        .success());
    assert!(synthetic_rows(&mut legacy.database.connection, "tracker").is_empty());
    let (mut triggered, triggered_schema) = self::indexer(&server);
    triggered.initialize_checkpoint().await.unwrap();
    triggered.commit_block(&reservation_block()).await.unwrap();
    triggered
        .database
        .connection
        .batch_execute(
            "UPDATE tracker SET holders=99; \
         CREATE FUNCTION synthetic_unwanted_write() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN UPDATE balances SET balance=balance+1; RETURN NEW; END $$; \
         CREATE TRIGGER synthetic_unwanted_write AFTER UPDATE ON tracker \
         FOR EACH ROW EXECUTE FUNCTION synthetic_unwanted_write()",
        )
        .unwrap();
    let triggered_checkpoint = checkpoint(&mut triggered);
    let before: Vec<_> = ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .map(|table| synthetic_rows(&mut triggered.database.connection, table))
        .collect();
    assert!(!recount(&triggered_schema, Some(&triggered_checkpoint))
        .status
        .success());
    for (table, expected) in ["balances", "tracker", "inscriptions", "history"]
        .iter()
        .zip(before)
    {
        assert_eq!(
            synthetic_rows(&mut triggered.database.connection, table),
            expected,
            "An unexpected trigger must roll back the entire recount"
        );
    }
}

#[tokio::test]
async fn exact_protocol_and_duplicate_output_selection_reserve_and_spend_only_once() {
    for leading_non_protocol in [false, true] {
        let server = MockOrd::new();
        let (mut indexer, schema) = indexer(&server);
        indexer.initialize_checkpoint().await.unwrap();
        let mut reserve = reservation_block();
        let transfer = reserve.transactions.last_mut().unwrap();
        let first_index = if leading_non_protocol { 1 } else { 0 };
        let chosen_id = format!("{}i{first_index}", transfer.transaction);
        let last_id = format!("{}i{}", transfer.transaction, first_index + 1);
        if leading_non_protocol {
            transfer.inscriptions[0].content = Some(hex::encode(
                json!({"p":"prefix-ltc-20-suffix","op":"transfer","tick":"lite","amt":"10"})
                    .to_string(),
            ));
        }
        for index in first_index..=first_index + 1 {
            if index == 0 {
                continue;
            }
            transfer.inscriptions.push(TransactionInscription {
                inscription_id: format!("{}i{index}", transfer.transaction),
                inscription_number: 3 + index,
                content_type: Some(hex::encode("application/json")),
                content: Some(hex::encode(
                    json!({"p":"ltc-20","op":"transfer","tick":"lite","amt":"10"}).to_string(),
                )),
            });
        }
        assert_eq!(indexer.commit_block(&reserve).await.unwrap(), 3);
        let f = fixture();
        let output = f["plan"]["inputs"][1]["output"].as_str().unwrap();
        let record = indexer
            .database
            .get_inscription_by_output(output.into())
            .unwrap();
        assert_eq!(record.inscription_id, chosen_id);
        assert_eq!(record.valid, Some(true));
        let buyer = f["plan"]["buyer_address"].as_str().unwrap().to_string();
        let balance = indexer
            .database
            .get_balance(buyer.clone(), "lite".into())
            .unwrap();
        assert_eq!(balance.balance, 10.into());
        assert_eq!(balance.transfer_balance, 10.into());
        assert!(
            !synthetic_rows(&mut indexer.database.connection, "inscriptions")
                .iter()
                .any(|r| r["inscription_id"] == last_id)
        );
        assert!(std::process::Command::new("php")
            .args(["tests/consume-selection.php", &schema, &chosen_id, &last_id])
            .status()
            .unwrap()
            .success());
        let nft = f["plan"]["inputs"][0]["output"].as_str().unwrap();
        server.route(
            format!("/output/{nft}"),
            json!({"address":f["plan"]["seller_address"],
            "transaction":nft.split(':').next().unwrap(),"value":10000}),
        );
        let mut swap = swap_block();
        swap.height = INITIAL_HEIGHT + 2;
        swap.previous_blockhash = "b".repeat(64);
        swap.hash = "c".repeat(64);
        server.responses.lock().unwrap().routes.insert(
            format!("/blockhash/{}", swap.height),
            (200, swap.hash.clone()),
        );
        indexer.commit_block(&swap).await.unwrap();
        assert_eq!(
            indexer
                .database
                .get_balance(buyer, "lite".into())
                .unwrap()
                .transfer_balance,
            0.into()
        );
        assert_eq!(
            indexer
                .database
                .get_balance(
                    f["plan"]["seller_address"].as_str().unwrap().into(),
                    "lite".into()
                )
                .unwrap()
                .balance,
            10.into()
        );
        let receipts: Vec<_> = synthetic_rows(&mut indexer.database.connection, "history")
            .into_iter()
            .filter(|r| r["action"] == "send")
            .collect();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0]["inscription_id"], chosen_id);
        assert_eq!(receipts[0]["tx_id"], f["txid"]);
    }
}

#[tokio::test]
async fn native_deploy_mint_transfer_reservation_then_swap() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    assert_eq!(indexer.commit_block(&reservation_block()).await.unwrap(), 3);
    let f = fixture();
    let plan = &f["plan"];
    let buyer = plan["buyer_address"].as_str().unwrap().to_string();
    let seller = plan["seller_address"].as_str().unwrap().to_string();
    let balance = indexer
        .database
        .get_balance(buyer.clone(), "lite".into())
        .unwrap();
    assert_eq!(balance.balance, 10.into());
    assert_eq!(balance.transfer_balance, 10.into());
    let transfer = indexer
        .database
        .get_inscription_by_output(plan["inputs"][1]["output"].as_str().unwrap().into())
        .unwrap();
    assert_eq!(transfer.valid, Some(true));
    assert_eq!(transfer.action, "transfer");
    let nft_output = plan["inputs"][0]["output"].as_str().unwrap();
    server.route(
        format!("/output/{nft_output}"),
        json!({"address":seller,"transaction":nft_output.split(':').next().unwrap(),"value":10000}),
    );
    server.responses.lock().unwrap().routes.insert(
        format!("/blockhash/{}", INITIAL_HEIGHT + 2),
        (200, "c".repeat(64)),
    );
    let mut swap = swap_block();
    swap.height = INITIAL_HEIGHT + 2;
    swap.previous_blockhash = "b".repeat(64);
    swap.hash = "c".repeat(64);
    indexer.commit_block(&swap).await.unwrap();
    assert_eq!(
        indexer
            .database
            .get_balance(seller, "lite".into())
            .unwrap()
            .balance,
        10.into()
    );
    let balance = indexer.database.get_balance(buyer, "lite".into()).unwrap();
    assert_eq!(balance.balance, 10.into());
    assert_eq!(balance.transfer_balance, 0.into());
    let transfer = indexer
        .database
        .get_inscription_by_output(plan["inputs"][1]["output"].as_str().unwrap().into())
        .unwrap();
    assert_eq!(transfer.spent_tx.as_deref(), f["txid"].as_str());
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT + 2);
}

#[tokio::test]
async fn failed_mint_write_rolls_back_whole_block_even_if_legacy_code_ignores_error() {
    use crate::schema::{balances, inscriptions, tracker};
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    indexer
        .database
        .connection
        .batch_execute(
            "ALTER TABLE history ADD CONSTRAINT synthetic_mint_failure CHECK (action <> 'mint')",
        )
        .unwrap();
    assert!(indexer.commit_block(&reservation_block()).await.is_err());
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    assert_eq!(
        balances::table
            .count()
            .get_result::<i64>(&mut indexer.database.connection)
            .unwrap(),
        0
    );
    assert_eq!(
        inscriptions::table
            .count()
            .get_result::<i64>(&mut indexer.database.connection)
            .unwrap(),
        0
    );
    assert_eq!(
        tracker::table
            .count()
            .get_result::<i64>(&mut indexer.database.connection)
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn processing_panic_rolls_back_prior_ledger_changes() {
    use crate::schema::{balances, inscriptions};
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    indexer
        .database
        .connection
        .batch_execute(
            "ALTER TABLE tracker ADD CONSTRAINT synthetic_deploy_failure CHECK (ticker <> 'fail')",
        )
        .unwrap();
    let mut block = reservation_block();
    block.transactions.push(inscription_tx(
        "f".repeat(64),
        4,
        json!({"p":"ltc-20","op":"deploy","tick":"fail","max":"1000","lim":"100","dec":2}),
    ));
    assert!(indexer
        .commit_block(&block)
        .await
        .unwrap_err()
        .contains("panicked"));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    assert_eq!(
        balances::table
            .count()
            .get_result::<i64>(&mut indexer.database.connection)
            .unwrap(),
        0
    );
    assert_eq!(
        inscriptions::table
            .count()
            .get_result::<i64>(&mut indexer.database.connection)
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn changed_previous_canonical_hash_is_refused() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    server.responses.lock().unwrap().routes.insert(
        format!("/blockhash/{INITIAL_HEIGHT}"),
        (200, "c".repeat(64)),
    );
    assert!(indexer
        .commit_block(&empty_block())
        .await
        .unwrap_err()
        .contains("Canonical"));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    assert!(indexer.initialize_checkpoint().await.is_err());
}

#[tokio::test]
async fn concurrent_legacy_writer_is_refused_during_block() {
    let server = MockOrd::new();
    let (mut indexer, schema) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    let mut other = PgConnection::establish(DATABASE).unwrap();
    other.batch_execute(&format!("SET search_path = {schema}; BEGIN; INSERT INTO status (key, value) VALUES ('synthetic_writer', 'active')")).unwrap();
    assert!(indexer
        .commit_block(&empty_block())
        .await
        .unwrap_err()
        .contains("writer"));
    other.batch_execute("ROLLBACK").unwrap();
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    indexer.commit_block(&empty_block()).await.unwrap();
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT + 1);
}

#[tokio::test]
async fn routine_maintenance_lock_can_finish_before_checkpoint_commit() {
    let server = MockOrd::new();
    let (mut indexer, schema) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    let (ready, waiting) = std::sync::mpsc::channel();
    let maintenance = thread::spawn(move || {
        let mut other = PgConnection::establish(DATABASE).unwrap();
        // The same table lock mode used by ordinary VACUUM and ANALYZE.
        other
            .batch_execute(&format!(
            "SET search_path = {schema}; BEGIN; LOCK TABLE tracker IN SHARE UPDATE EXCLUSIVE MODE"
        ))
            .unwrap();
        ready.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));
        other.batch_execute("ROLLBACK").unwrap();
    });
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    let result = indexer.commit_block(&empty_block()).await;
    maintenance.join().unwrap();
    result.unwrap();
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT + 1);
    assert_eq!(
        indexer.read_checkpoint().unwrap().block_hash,
        "b".repeat(64)
    );
}
fn swap_block() -> Block {
    let f = fixture();
    let plan = &f["plan"];
    let mut block = empty_block();
    block.transactions.push(Transaction {
        transaction: f["txid"].as_str().unwrap().into(),
        inputs: plan["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|input| (input["output"].as_str().unwrap().into(), 0))
            .collect(),
        outputs: plan["outputs"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, output)| {
                (
                    format!("{}:{i}", f["txid"].as_str().unwrap()),
                    output["value"].as_str().unwrap().parse().unwrap(),
                )
            })
            .collect(),
        output_addresses: vec![
            plan["buyer_address"].as_str().unwrap().into(),
            plan["seller_address"].as_str().unwrap().into(),
            plan["buyer_address"].as_str().unwrap().into(),
        ],
        inscriptions: vec![],
    });
    block
}
fn seed_transfer(indexer: &mut Indexer, server: &MockOrd) {
    let f = fixture();
    let plan = &f["plan"];
    let buyer = plan["buyer_address"].as_str().unwrap().to_string();
    indexer.database.create_tracker(
        "lite".into(),
        format!("{}i0", "d".repeat(64)),
        1,
        1000.into(),
        100.into(),
        2,
    );
    indexer
        .database
        .create_balance(buyer.clone(), "lite".into())
        .unwrap();
    indexer
        .database
        .update_balance(buyer.clone(), "lite".into(), 0.into(), 10.into())
        .unwrap();
    let record = indexer
        .database
        .create_inscription(
            plan["inputs"][1]["output"]
                .as_str()
                .unwrap()
                .split(':')
                .next()
                .unwrap()
                .into(),
            buyer,
            plan["transfer_inscription_id"].as_str().unwrap().into(),
            2,
            INITIAL_HEIGHT as i64,
            plan["inputs"][1]["output"].as_str().unwrap().into(),
            1,
            "lite".into(),
            "transfer".into(),
            None,
            None,
            None,
            Some(10.into()),
            10000,
        )
        .unwrap();
    indexer
        .database
        .update_inscription_valid(record.id, true)
        .unwrap();
    let nft = &plan["inputs"][0];
    server.route(format!("/output/{}", nft["output"].as_str().unwrap()), json!({"address":plan["seller_address"],"transaction":nft["output"].as_str().unwrap().split(':').next().unwrap(),"value":10000}));
}
fn unspent(indexer: &mut Indexer) -> bool {
    indexer
        .database
        .get_inscription_by_output(
            fixture()["plan"]["inputs"][1]["output"]
                .as_str()
                .unwrap()
                .into(),
        )
        .unwrap()
        .spent
        == Some(false)
}

#[tokio::test]
async fn empty_bootstrap_and_atomic_empty_block() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    assert_eq!(
        indexer.initialize_checkpoint().await.unwrap().height,
        INITIAL_HEIGHT
    );
    indexer.commit_block(&empty_block()).await.unwrap();
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT + 1);
    assert_eq!(
        indexer.initialize_checkpoint().await.unwrap().block_hash,
        "b".repeat(64)
    );
}

#[tokio::test]
async fn populated_legacy_ledger_is_never_certified() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    seed_transfer(&mut indexer, &server);
    assert!(indexer
        .initialize_checkpoint()
        .await
        .unwrap_err()
        .contains("legacy"));
    assert!(indexer
        .database
        .get_status(CHECKPOINT_KEY.into())
        .optional()
        .unwrap()
        .is_none());
    assert!(unspent(&mut indexer));
}

#[tokio::test]
async fn advanced_empty_legacy_ledger_is_not_bootstrapped() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer
        .database
        .update_status("last_height".into(), (INITIAL_HEIGHT + 1).to_string())
        .unwrap();
    assert!(indexer.initialize_checkpoint().await.is_err());
    assert!(indexer
        .database
        .get_status(CHECKPOINT_KEY.into())
        .optional()
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn swap_credits_exact_seller_and_php_accepts_committed_receipt() {
    let server = MockOrd::new();
    let (mut indexer, schema) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    let f = fixture();
    let seller = f["plan"]["seller_address"].as_str().unwrap();
    assert_eq!(indexer.commit_block(&swap_block()).await.unwrap(), 0);
    let record = indexer
        .database
        .get_inscription_by_output(f["plan"]["inputs"][1]["output"].as_str().unwrap().into())
        .unwrap();
    assert_eq!(record.spent, Some(true));
    assert_eq!(record.spent_tx.as_deref(), f["txid"].as_str());
    assert_eq!(record.address_receiver.as_deref(), Some(seller));
    assert_eq!(record.spent_height, Some((INITIAL_HEIGHT + 1) as i64));
    assert_eq!(
        indexer
            .database
            .get_balance(seller.into(), "lite".into())
            .unwrap()
            .balance,
        10.into()
    );
    assert_eq!(
        indexer
            .database
            .get_balance(
                f["plan"]["buyer_address"].as_str().unwrap().into(),
                "lite".into()
            )
            .unwrap()
            .transfer_balance,
        0.into()
    );
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT + 1);
    let status = std::process::Command::new("php")
        .args(["tests/consume-snapshot.php", &schema])
        .status()
        .unwrap();
    assert!(
        status.success(),
        "PHP consumer rejected native producer snapshot"
    );
}

#[tokio::test]
async fn required_prior_output_failure_rolls_back() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    server
        .responses
        .lock()
        .unwrap()
        .routes
        .retain(|path, _| !path.starts_with("/output/"));
    assert!(indexer
        .commit_block(&swap_block())
        .await
        .unwrap_err()
        .contains("prior input"));
    assert!(unspent(&mut indexer));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
}

#[tokio::test]
async fn wrong_prior_transaction_rolls_back() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    server.route(
        format!(
            "/output/{}",
            fixture()["plan"]["inputs"][0]["output"].as_str().unwrap()
        ),
        json!({"address":"test","transaction":"wrong","value":10000}),
    );
    assert!(indexer
        .commit_block(&swap_block())
        .await
        .unwrap_err()
        .contains("identity"));
    assert!(unspent(&mut indexer));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
}

#[tokio::test]
async fn reorg_after_ledger_mutation_rolls_back_balances_and_receipt() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    server.responses.lock().unwrap().reorg_after_output = true;
    assert!(indexer
        .commit_block(&swap_block())
        .await
        .unwrap_err()
        .contains("Canonical"));
    assert!(unspent(&mut indexer));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    assert!(indexer
        .database
        .get_balance(
            fixture()["plan"]["seller_address"].as_str().unwrap().into(),
            "lite".into()
        )
        .optional()
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sql_failure_cannot_publish_checkpoint_or_partial_transfer() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    indexer
        .database
        .connection
        .batch_execute(
            "ALTER TABLE history ADD CONSTRAINT synthetic_failure CHECK (action <> 'send')",
        )
        .unwrap();
    assert!(indexer.commit_block(&swap_block()).await.is_err());
    assert!(unspent(&mut indexer));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    assert_eq!(
        indexer
            .database
            .get_balance(
                fixture()["plan"]["buyer_address"].as_str().unwrap().into(),
                "lite".into()
            )
            .unwrap()
            .transfer_balance,
        10.into()
    );
}

#[tokio::test]
async fn missing_reservation_cannot_be_reported_as_paid() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    indexer
        .database
        .update_balance(
            fixture()["plan"]["buyer_address"].as_str().unwrap().into(),
            "lite".into(),
            0.into(),
            0.into(),
        )
        .unwrap();
    assert!(indexer
        .commit_block(&swap_block())
        .await
        .unwrap_err()
        .contains("reserved"));
    assert!(unspent(&mut indexer));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
}

#[tokio::test]
async fn gaps_wrong_parents_and_progress_mismatch_are_rejected() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    let mut block = empty_block();
    block.height += 1;
    assert!(indexer.commit_block(&block).await.is_err());
    block = empty_block();
    block.previous_blockhash = "d".repeat(64);
    assert!(indexer.commit_block(&block).await.is_err());
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
    indexer
        .database
        .update_status("last_height".into(), (INITIAL_HEIGHT + 1).to_string())
        .unwrap();
    assert!(indexer
        .commit_block(&empty_block())
        .await
        .unwrap_err()
        .contains("progress"));
}

#[tokio::test]
async fn competing_checkpoint_writer_is_refused() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    let mut other = PgConnection::establish(DATABASE).unwrap();
    other
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({WRITER_LOCK})"
        ))
        .unwrap();
    assert!(indexer
        .commit_block(&empty_block())
        .await
        .unwrap_err()
        .contains("writer"));
    other.batch_execute("ROLLBACK").unwrap();
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
}

#[tokio::test]
async fn unhealthy_or_nonmainnet_ord_is_refused() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    for status in [
        json!({"chain":"regtest","height":INITIAL_HEIGHT+20,"unrecoverably_reorged":false,"inscriptions":100}),
        json!({"chain":"mainnet","height":INITIAL_HEIGHT+20,"unrecoverably_reorged":true,"inscriptions":100}),
        json!({"chain":"mainnet","height":INITIAL_HEIGHT-1,"unrecoverably_reorged":false,"inscriptions":100}),
    ] {
        server.route("/status".into(), status);
        assert!(indexer.initialize_checkpoint().await.is_err());
        assert!(indexer
            .database
            .get_status(CHECKPOINT_KEY.into())
            .optional()
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn malformed_block_is_rejected_and_does_not_advance() {
    let server = MockOrd::new();
    let (mut indexer, _) = indexer(&server);
    indexer.initialize_checkpoint().await.unwrap();
    seed_transfer(&mut indexer, &server);
    let mut block = swap_block();
    block.timestamp = "invalid timestamp".into();
    assert!(indexer.commit_block(&block).await.is_err());
    assert!(unspent(&mut indexer));
    assert_eq!(checkpoint(&mut indexer).height, INITIAL_HEIGHT);
}
