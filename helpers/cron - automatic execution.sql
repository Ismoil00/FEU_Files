/* CRON CONFIG ZONE */

select * from cron.job;

SELECT * FROM cron.job_run_details
ORDER BY end_time DESC
LIMIT 10;

SELECT jobid, jobname, database, username
FROM cron.job


/* PENSION SECTION */

SELECT cron.unschedule('tesing-cron-with-function');

SELECT cron.schedule(
    'monthly-pensioners-amount-update',
    '0 0 1 * *',
    $$SELECT pension.automatic_pension_amount_recalculation()$$
);


select * from pension.pensioner
where updated->>'user_id' = 'pension-amount-monthly-recalculation'


/* USD-EXCHANGE ZONE */

select * from commons.usd_exchange

CREATE OR REPLACE FUNCTION commons.update_usd_exchange(
	INOUT jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
begin
	update commons.usd_exchange SET
		value = (jdata->>'value')::numeric,
		active_date = (jdata->>'date')::date,
		-- disabled = (jdata->>'disabled')::boolean,
		updated_at = current_timestamp;

	select json_build_object('status', 200, 'msg', 'UPDATED!') into jdata;
end;
$BODY$;

