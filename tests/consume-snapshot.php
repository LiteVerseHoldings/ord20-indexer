<?php
// Synthetic local database only; no application secrets or real ledger data.
require_once '/consumer/application/libs/token_trade_transfer.php';
require_once '/consumer/application/model/model_ord20.php';
$schema = $argv[1] ?? '';
if (!preg_match('/\Atoken_trade_test_[0-9]+_[0-9]+\z/', $schema)) throw new Exception('Synthetic schema required');
$pdo = new PDO('pgsql:host=/tmp/token-trade-pg;port=55432;dbname=token_trade_tests', 'postgres', null,
    array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_OBJ));
$pdo->exec('SET search_path = ' . $schema);
$model = new ModelOrd20($pdo);
$fixture = json_decode(file_get_contents(__DIR__ . '/fixtures/ltc20-swap.json'), true);
$id = $fixture['plan']['transfer_inscription_id'];
$snapshot = $model->readLtc20TradeSnapshot(function () use ($model, $id) { return $model->getLtc20InscriptionByInscriptionId($id); });
$record = $snapshot['value'];
TokenTradeTransfer::verify($record, $id, 'lite', '10', true);
TokenTradeTransfer::checkpoint($snapshot['checkpoint'], 2465226, str_repeat('b', 64));
TokenTradeTransfer::recordCheckpoint($record, 2465226, $fixture['txid']);
TokenTradeTransfer::location($record, $fixture['txid'] . ':1', $fixture['plan']['seller_address'], true);
if ($pdo->inTransaction()) throw new Exception('Read snapshot remained open');
$pdo->exec("UPDATE status SET value = '2465227' WHERE key = 'last_height'");
$rejected = false;
try { $model->readLtc20TradeSnapshot(function () use ($model, $id) { return $model->getLtc20InscriptionByInscriptionId($id); }); }
catch (RuntimeException $error) { $rejected = true; }
if (!$rejected || $pdo->inTransaction()) throw new Exception('Mismatched progress was accepted or snapshot remained open');
$pdo->exec("UPDATE status SET value = '2465226' WHERE key = 'last_height'");
$other = new PDO('pgsql:host=/tmp/token-trade-pg;port=55432;dbname=token_trade_tests', 'postgres', null,
    array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
$other->exec('SET search_path = ' . $schema);
$stable = $model->readLtc20TradeSnapshot(function () use ($model, $id, $other) {
    $other->beginTransaction();
    $other->exec("UPDATE status SET value = '2465227' WHERE key = 'last_height'");
    $other->exec("UPDATE inscriptions SET amount = 999 WHERE action = 'transfer'");
    $other->commit();
    return $model->getLtc20InscriptionByInscriptionId($id);
});
if ($stable['checkpoint']['height'] !== 2465226 || TokenTradeTransfer::decimal($stable['value']->amount) !== '10') {
    throw new Exception('A concurrent commit mixed checkpoint and transfer snapshots');
}
echo "Native PostgreSQL to PHP receipt checks passed\n";
