# Bounded Ord replay operations

This optional Python 3.9+ tool supplies a read-only Ord view to an **isolated
LTC-20 replay database**. It has no third-party Python dependencies. It does not
change the Rust indexer, Ord's indexing engine, a ledger, a wallet, or the
historical reconciliation rules. Installing it is a separate operator action;
adding these files to Git does not deploy them.

The tool is for controlled loopback validation, not a public HTTP service.
When production Ord is the upstream, the replay independently checks LTC-20
processing using shared Ord history. It does **not** independently rebuild or
certify Ord history. The `/status` view declares that distinction explicitly.

## Defaults and boundaries

`reader.example.json` is a generic example, not a production configuration.
Choose the upstream, an unused listen port, and the fixed **last allowed block
height** before running anything. Keep private profiles and runtime state under
the ignored `local/` directory, outside Git.

| Setting | Default / rule |
| --- | --- |
| Request rate | 10 starts/second, including transport retries; configurable from 0.1 to 100 |
| Concurrency | One forwarded upstream request at a time, including parallel callers |
| Listener | `127.0.0.1:18080`; public bind addresses are not supported |
| Upstream | Required literal loopback HTTP address; no credentials, environment proxies, or redirects |
| Height | Required inclusive ceiling, retained across rate changes and never lowered by the tuning commands |
| Response bound | 64 MiB maximum; operators can lower it |
| Retry policy | At most three attempts for transport errors and 429/500/502/503/504 |
| Other failures | Missing, malformed, wrong-height, unhealthy-chain and reorg responses fail closed |
| CPU allowance | Example systemd unit: 50%, meaning half of one logical core |

Only GET `/status`, `/blockheight`, numeric `/blockhash/H`, numeric `/block/H`
and well-formed `/output/TXID:VOUT` routes are forwarded. Block and hash requests
above the ceiling are rejected **before upstream access**. Write methods and
wallet/RPC routes are rejected. Historical block/hash/output response bytes are
preserved; status and available-height responses disclose the bounded view.
The indexer retains responsibility for its full schema, canonical hash,
previous-output, database transaction and checkpoint checks.

The reader samples its validated configuration before each request. Rate changes
and increasing the ceiling therefore require no reader or producer restart.
Changing the upstream/listener or lowering a ceiling while running fails closed.
An optional `--resource-guard PATH` accepts an operator-owned JSON status file
whose truthy `stop_reason` blocks reads; publish this file atomically.

## Inspect, run and tune

Create an untracked local copy of the example and edit it for the isolated job.
Do not copy an existing `.env`, credentials, databases, wallets, backups or raw
logs into this directory or into Git.

```sh
python3 -I -B ops/validation/ord_replay.py check-config --config /path/to/reader.local.json
python3 -I -B ops/validation/ord_replay.py serve \
  --config /path/to/reader.local.json --state-dir /path/to/validation-state
```

Configure the **test producer**, using its separate test database, to read from
`http://127.0.0.1:18080/`. Leave production consumers and their configurations
unchanged. The relay does not bootstrap a database or authorize a native binary.

Tuning commands show a plan unless `--apply` is supplied:

```sh
python3 -I -B ops/validation/ord_replay.py set-rate \
  --config /path/to/reader.local.json --requests-per-second 20

python3 -I -B ops/validation/ord_replay.py set-rate \
  --config /path/to/reader.local.json --requests-per-second 30 \
  --cpu-quota-percent 100 --apply

python3 -I -B ops/validation/ord_replay.py set-height \
  --config /path/to/reader.local.json --height-limit 3000100 --apply
```

CPU changes are optional and Linux/systemd-specific. They target **only** the
fixed `ord20-validation-reader.service` example unit, use runtime properties,
and refuse an inactive unit. They never start or restart a service. A failed
configuration write restores the preceding CPU allowance. Without a CPU option,
configuration changes work without systemd.

Use the tuning commands for **all live configuration writers**, including a
replay controller that advances the target. Updates share an exclusive lock,
reload the latest ceiling, preserve configuration ownership/mode, and replace
the file atomically. A stale lock requires review; the tool never removes a
lock it did not create or restores an old configuration over a newer height.

## Health monitoring and opt-in rollback

The watchdog checks upstream mainnet/inscription health, status latency, reader
restarts, and increases in aggregate fetch-error/retry counters. An optional
independent loopback `--tip-height-url http://127.0.0.1:PORT/blockheight` also
checks index lag; without it, `index_lag_checked` is false. It never contacts a
wallet or requests Core RPC credentials. Database checkpoint and reconciliation
gates remain in the indexer/controller.

```sh
# One read-only sample; neither files nor CPU settings are changed.
python3 -I -B ops/validation/ord_replay.py watch \
  --config /path/to/reader.local.json \
  --reader-state /path/to/validation-state/reader-stats.json --once

# Arm rollback only after confirming the exact config, state and reader unit.
python3 -I -B ops/validation/ord_replay.py watch \
  --config /path/to/reader.local.json \
  --reader-state /path/to/validation-state/reader-stats.json \
  --fallback-rps 10 --fallback-cpu-percent 50 --apply
```

Defaults are a 10-second polling interval, three consecutive unhealthy samples,
and a 0.25-second status-latency limit. Calibrate these against the upstream's
normal behavior before tuning. Rollback can only lower the current request
rate/CPU allowance. It keeps the **current** ceiling and touches no ledger rows,
wallet state or producer process. A successful rollback exits for operator
review and re-arming; it never automatically retries financial jobs or raises
the limits again. A CPU-control failure is reported, not hidden.

Telemetry contains counters and error classes, not response bodies, request
outpoints, upstream URLs or credentials. State is updated atomically without
forcing a disk flush for every request. Runtime counters identify each reader
instance; preserve the period summaries when comparing across restarts.

## Example systemd layout

The `systemd/` files are **templates**, not an installer. Provision the dedicated
`ord20-validation` account, install `ord_replay.py` as root-owned code at
`/opt/ord20-validation/ord_replay.py`, and create a reviewed configuration at
`/etc/ord20-validation/reader.json` owned by root, group `ord20-validation`, mode
0640. Its parent directory must be root-owned, group `ord20-validation`, mode
0750. Confirm that all names and paths are unused before installation.

The reader runs unprivileged with a read-only filesystem except its telemetry
directory, loopback-only network policy and memory/CPU bounds. The optional
watchdog uses root for the narrowly scoped systemd CPU property, with the
reader group for configuration/state access; it has no Linux capabilities and
can write only the dedicated configuration directory. Its `BindsTo` dependency
stops it when the reader stops. Review and re-arm monitoring after a restart.

Do not overwrite existing production units, reuse a live ledger directory,
disable existing capacity guards, or turn these templates into an automated
deployment without a separately reviewed job-specific runbook. Keep every
production service and private inscriber independent of this helper.

## Measured comparison-replay throughput

Short sequential mainnet replay samples measured the following configurations:

| Change | Reader CPU allowance | Baseline blocks/hour | Trial blocks/hour | Observed ratio |
| --- | --- | ---: | ---: | ---: |
| 10 to 20 requests/second | Half core throughout | 3,993 | 7,448 | 1.86x |
| 20 to 30 requests/second | Half core to one core | 7,284 | 10,842 | 1.49x |

Each baseline lasted about 60 seconds; each trial lasted about 180 seconds.
The 30-request trial changed **both** pacing and the reader's CPU allowance.
Block composition varied, so these are combined configuration observations,
not an isolated algorithm benchmark or a throughput guarantee. Native indexer
and Ord binaries, ledger validation and the one-request concurrency policy were
unchanged. No fetch errors/retries occurred in these trial windows; the preserved
checkpoint was checked against the canonical chain. Full historical ledger
reconciliation was still pending when these samples were taken.

For each new environment, measure committed blocks/hour, actual request rate,
reader CPU throttling, upstream latency/lag, errors and memory/disk headroom.
Preserve a known-good fallback and accept an increase only after the observation
window passes. Never infer final trade correctness from a faster relay.

## Tests

```sh
python3 -I -B -m unittest discover -s ops/validation/tests -v
```

Fixtures use temporary local files and unfunded loopback HTTP servers only.
Systemd mutations are mocked. Tests cover spacing, sequential forwarding,
allowlists, target bounds, malformed/missing data, redirects/proxy isolation,
retry limits, configuration races, read-only defaults and rollback behavior.
These tests neither connect to production nor replace native chain/ledger tests.
