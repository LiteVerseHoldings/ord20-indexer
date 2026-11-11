#!/bin/bash
set -euo pipefail
# Run only in a NEW local test container; Unix socket, no network database listener.
mkdir /tmp/token-trade-pg /tmp/token-trade-pgdata
chown postgres:postgres /tmp/token-trade-pg /tmp/token-trade-pgdata
runuser -u postgres -- /usr/lib/postgresql/15/bin/initdb -D /tmp/token-trade-pgdata --auth-local=trust --auth-host=reject >/tmp/token-trade-init.log
runuser -u postgres -- /usr/lib/postgresql/15/bin/pg_ctl -D /tmp/token-trade-pgdata -l /tmp/token-trade-postgres.log -o "-k /tmp/token-trade-pg -p 55432 -c listen_addresses=''" -w start
runuser -u postgres -- /usr/lib/postgresql/15/bin/createdb -h /tmp/token-trade-pg -p 55432 token_trade_tests
export TEST_DATABASE_URL='postgresql:///token_trade_tests?host=/tmp/token-trade-pg&port=55432&user=postgres'
unset DATABASE_URL
cargo test --locked --features native-checkpoint-integration -- --test-threads=1
php /consumer/tests/token-trade-transfer.php
php -l /consumer/application/controller/tradeevidence.php
php -l /consumer/application/model/model_ord20.php
php -l /consumer/application/libs/token_trade_transfer.php
