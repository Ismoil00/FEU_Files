

SELECT * FROM cron.job c;


-- 1) Create/replace the job (daily at 01:00)
SELECT cron.schedule(
  'daily-usd-exchange',
  '0 1 * * *',
  $$SELECT commons.update_usd_exchange();$$
);

-- 2) Ensure DB/user/active are correct
SELECT cron.alter_job(
  (SELECT jobid FROM cron.job WHERE jobname = 'daily-usd-exchange'),
  '0 1 * * *',
  $$SELECT commons.update_usd_exchange();$$,
  current_database(),
  'postgres',
  true
);

-- 3) Force Unix socket (nodename must be non-NULL in your version)
UPDATE cron.job
SET nodename = '/var/run/postgresql',  -- use SHOW unix_socket_directories if different
    nodeport = 5432
WHERE jobname = 'daily-usd-exchange';