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
