\set ON_ERROR_STOP on
\if :{?apply}
\else
  \set apply false
\endif

\if :apply
  BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE;
  SET LOCAL lock_timeout = '2s';
  SET LOCAL statement_timeout = '30s';
  LOCK TABLE status, balances, tracker, inscriptions, history IN SHARE ROW EXCLUSIVE MODE;
  SELECT set_config('ord20.recount_height', :'expected_height', true) AS configured_height,
         set_config('ord20.recount_hash', :'expected_hash', true) AS configured_hash \gset
  DO $$
  DECLARE checkpoint jsonb;
  BEGIN
    SELECT value::jsonb INTO checkpoint FROM status WHERE key = 'token_trade_checkpoint';
    IF checkpoint IS NULL
       OR checkpoint->'version' IS DISTINCT FROM '1'::jsonb
       OR checkpoint->>'chain' IS DISTINCT FROM 'mainnet'
       OR checkpoint->>'protocol' IS DISTINCT FROM 'ltc-20'
       OR checkpoint->'ready' IS DISTINCT FROM 'true'::jsonb
       OR jsonb_typeof(checkpoint->'height') IS DISTINCT FROM 'number'
       OR checkpoint->>'height' IS DISTINCT FROM current_setting('ord20.recount_height')
       OR checkpoint->>'block_hash' IS DISTINCT FROM current_setting('ord20.recount_hash')
       OR current_setting('ord20.recount_hash') !~ '^[0-9a-f]{64}$'
       OR (SELECT value FROM status WHERE key = 'last_height') IS DISTINCT FROM checkpoint->>'height'
    THEN RAISE EXCEPTION 'Recount requires the expected existing native LTC-20 checkpoint';
    END IF;
    IF EXISTS (SELECT 1 FROM balances WHERE balance IS NULL OR transfer_balance IS NULL
               OR balance < 0 OR transfer_balance < 0)
    THEN RAISE EXCEPTION 'Refusing recount with invalid token balances';
    END IF;
    IF current_setting('track_counts') IS DISTINCT FROM 'on'
    THEN RAISE EXCEPTION 'Recount requires transaction write accounting';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='tracker'::regclass AND tgenabled<>'D')
       OR EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class='tracker'::regclass)
    THEN RAISE EXCEPTION 'Refusing recount with tracker triggers or rewrite rules';
    END IF;
  END $$;
  WITH counts AS (
    SELECT t.id, count(b.address) FILTER (WHERE b.balance + b.transfer_balance > 0) AS holders
    FROM tracker t LEFT JOIN balances b ON b.ticker = t.ticker GROUP BY t.id
  )
  UPDATE tracker t SET holders = c.holders FROM counts c
  WHERE t.id = c.id AND t.holders IS DISTINCT FROM c.holders;
  DO $$
  BEGIN
    IF EXISTS (
      SELECT 1 FROM pg_stat_xact_user_tables
      WHERE (relid = 'tracker'::regclass AND (n_tup_ins <> 0 OR n_tup_del <> 0))
         OR (relid <> 'tracker'::regclass AND (n_tup_ins <> 0 OR n_tup_upd <> 0 OR n_tup_del <> 0))
    ) THEN RAISE EXCEPTION 'Recount wrote outside the holder cache; rolling back';
    END IF;
  END $$;
\else
  BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
  SET LOCAL statement_timeout = '30s';
\endif

SELECT json_build_object('tokens_checked', count(*),
                         'holder_count_mismatches', count(*) FILTER (WHERE holders IS DISTINCT FROM actual),
                         'holder_rows_updated', COALESCE((SELECT n_tup_upd FROM pg_stat_xact_user_tables WHERE relid='tracker'::regclass),0),
                         'other_ledger_writes', COALESCE((SELECT sum(n_tup_ins+n_tup_upd+n_tup_del) FROM pg_stat_xact_user_tables WHERE relid<>'tracker'::regclass),0))
FROM (
  SELECT t.id, t.holders, count(b.address) FILTER (WHERE b.balance + b.transfer_balance > 0) AS actual
  FROM tracker t LEFT JOIN balances b ON b.ticker = t.ticker GROUP BY t.id
) counts;
COMMIT;
