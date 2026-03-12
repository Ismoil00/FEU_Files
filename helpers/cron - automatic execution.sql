/* CRON CONFIG ZONE */

select * from cron.job;

SELECT
    j.jobname,
    d.runid,
    d.start_time,
    d.end_time,
    d.status,
    d.return_message,
    d.command
FROM cron.job_run_details d
JOIN cron.job j ON j.jobid = d.jobid
ORDER BY d.start_time DESC;

/* PENSION AMOUNT AUTOMATIC  CALCULATION */
CREATE OR REPLACE FUNCTION pension.run_pension_amount_recalculation_on_month_end()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXTRACT(DAY FROM (current_date + interval '1 day')) = 1 THEN
        PERFORM pension.automatic_pension_amount_recalculation();
    END IF;
END;
$$;

SELECT cron.schedule(
    'pension-amount-monthly-recalculation',
    '59 22 * * *',
    $$SELECT pension.run_pension_amount_recalculation_on_month_end()$$
);


/* PAYROLL AUTOMATIC CALCULATION */
SELECT cron.schedule(
    'monthly-pensioners-payroll-calculation',
    '59 23 * * *',
    $$SELECT pension.run_pensioners_payroll_on_month_end()$$
);

CREATE OR REPLACE FUNCTION pension.run_pensioners_payroll_on_month_end()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXTRACT(DAY FROM (current_date + interval '1 day')) = 1 THEN
        PERFORM pension.create_pensioners_payroll_automatically();
    END IF;
END;
$$;


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

