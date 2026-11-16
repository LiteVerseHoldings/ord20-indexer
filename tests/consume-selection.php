<?php
// Isolated fixture database only. Real ledger records and wallets are never read.
require_once '/consumer/application/libs/token_trade_transfer.php';
require_once '/consumer/application/model/model_ord20.php';
$schema = $argv[1] ?? '';
$selected = $argv[2] ?? '';
$old = $argv[3] ?? '';
if (!preg_match('/\Atoken_trade_test_[0-9]+_[0-9]+\z/', $schema)
    || !preg_match('/\A[0-9a-f]{64}i[0-9]+\z/', $selected)
    || !preg_match('/\A[0-9a-f]{64}i[0-9]+\z/', $old) || $selected === $old) {
    throw new Exception('Isolated schema and two different fixture identities required');
}
$pdo = new PDO('pgsql:host=/tmp/token-trade-pg;port=55432;dbname=token_trade_tests', 'postgres', null,
    array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_OBJ));
$pdo->exec('SET search_path = ' . $schema);
$model = new ModelOrd20($pdo);
$fixture = json_decode(file_get_contents(__DIR__ . '/fixtures/ltc20-swap.json'), true);
$buyer = $fixture['plan']['buyer_address'];
$snapshot = $model->readLtc20TradeSnapshot(function () use ($model, $selected, $old, $buyer) {
    $record = $model->getLtc20InscriptionByInscriptionId($selected);
    TokenTradeTransfer::verify($record, $selected, 'lite', '10', false);
    TokenTradeTransfer::location($record, $record->output, $buyer, false);
    $pending = $model->getLtc20TradeTransfers($buyer, 'lite', '10');
    if (count($pending) !== 1 || $pending[0]->inscription_id !== $selected) {
        throw new Exception('One output must expose exactly one reserved transfer identity');
    }
    try {
        TokenTradeTransfer::verify($model->getLtc20InscriptionByInscriptionId($old), $old, 'lite', '10', false);
        throw new LogicException('Unselected historical identity was accepted');
    } catch (InvalidArgumentException $expected) {}
    try {
        TokenTradeTransfer::verify($record, $old, 'lite', '10', false);
        throw new LogicException('A shared output aliased two different inscription identities');
    } catch (InvalidArgumentException $expected) {}
    return true;
});
if ($snapshot['value'] !== true || $pdo->inTransaction()) throw new Exception('Identity snapshot failed');
echo "Native identity selection to PHP compatibility checks passed\n";
