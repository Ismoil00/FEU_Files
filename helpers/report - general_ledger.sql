





-- select * from accounting.ledger;


select reports.get_general_ledger(
	'budget',
	'2026-01-01',
	'2026-03-06',
	true
);


CREATE OR REPLACE FUNCTION reports.get_general_ledger(
	_financing accounting.budget_distribution_type,
	_date_from date,
	_date_to date,
	monthly_report boolean DEFAULT false
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result jsonb;
	_accounts_count integer;
BEGIN
	IF monthly_report THEN
		WITH
		-- Ledger in period (no drafts, filter by financing)
		period_ledger AS (
			SELECT l.debit, l.credit, l.amount, (l.created_date)::date AS tx_date
			FROM accounting.ledger l
			WHERE l.draft = false
			  AND (l.financing = _financing OR (l.financing IS NULL AND _financing IS NULL))
			  AND (l.created_date)::date BETWEEN _date_from AND _date_to
		),
		-- All ledger for saldo (before period and within period; same filters except date)
		all_ledger AS (
			SELECT l.debit, l.credit, l.amount, (l.created_date)::date AS tx_date
			FROM accounting.ledger l
			WHERE l.draft = false
			  AND (l.financing = _financing OR (l.financing IS NULL AND _financing IS NULL))
			  AND (l.created_date)::date <= _date_to
		),
		-- Distinct accounts that have movement in the period
		accounts_in_period AS (
			SELECT DISTINCT acc AS account FROM (
				SELECT debit AS acc FROM period_ledger
				UNION
				SELECT credit AS acc FROM period_ledger
			) u
		),
		-- All months in range (inclusive)
		month_series AS (
			SELECT to_char(m, 'YYYY-MM') AS month_key,
			       (date_trunc('month', m)::date) AS month_start,
			       (date_trunc('month', m) + interval '1 month' - interval '1 day')::date AS month_end
			FROM generate_series(
				date_trunc('month', _date_from)::date,
				date_trunc('month', _date_to)::date,
				'1 month'::interval
			) m
		),
		-- Per-account, per-month: debit/credit in month, prev/next saldo, credited_accounts (only when main account debited)
		by_account_month AS (
			SELECT
				a.account,
				ms.month_key,
				ms.month_start,
				ms.month_end,
				COALESCE(SUM(CASE WHEN pl.debit = a.account THEN pl.amount ELSE 0 END), 0)::numeric AS debit_amount,
				COALESCE(SUM(CASE WHEN pl.credit = a.account THEN pl.amount ELSE 0 END), 0)::numeric AS credit_amount,
				-- prev_saldo: balance at start of month (all tx before month_start)
				COALESCE((
					SELECT SUM(CASE WHEN al.debit = a.account THEN al.amount WHEN al.credit = a.account THEN -al.amount ELSE 0 END)
					FROM all_ledger al
					WHERE al.tx_date < ms.month_start
					  AND (al.debit = a.account OR al.credit = a.account)
				), 0)::numeric AS prev_balance,
				-- next_saldo: balance at end of month (all tx up to month_end)
				COALESCE((
					SELECT SUM(CASE WHEN al.debit = a.account THEN al.amount WHEN al.credit = a.account THEN -al.amount ELSE 0 END)
					FROM all_ledger al
					WHERE al.tx_date <= ms.month_end
					  AND (al.debit = a.account OR al.credit = a.account)
				), 0)::numeric AS next_balance,
				-- credited_accounts: when this account was debited in this month, group by credit
				(
					SELECT jsonb_agg(
						jsonb_build_object('account', sub.credit, 'amount', sub.amount)
						ORDER BY sub.credit
					)
					FROM (
						SELECT pl_inner.credit, SUM(pl_inner.amount)::numeric AS amount
						FROM period_ledger pl_inner
						WHERE pl_inner.debit = a.account
						  AND pl_inner.tx_date >= ms.month_start
						  AND pl_inner.tx_date <= ms.month_end
						GROUP BY pl_inner.credit
					) sub
				) AS credited_accounts
			FROM accounts_in_period a
			CROSS JOIN month_series ms
			LEFT JOIN period_ledger pl ON (pl.debit = a.account OR pl.credit = a.account)
			  AND pl.tx_date >= ms.month_start AND pl.tx_date <= ms.month_end
			GROUP BY a.account, ms.month_key, ms.month_start, ms.month_end
		),
		-- Build month object for each account: include ALL months in range (even with zeros)
		month_objects AS (
			SELECT
				account,
				jsonb_object_agg(
					month_key,
					jsonb_build_object(
						'debit_amount', debit_amount,
						'credit_amount', credit_amount,
						'next_saldo_debit', (next_balance >= 0),
						'next_saldo_amount', CASE WHEN next_balance >= 0 THEN next_balance ELSE (-next_balance) END,
						'prev_saldo_debit', (prev_balance >= 0),
						'prev_saldo_amount', CASE WHEN prev_balance >= 0 THEN prev_balance ELSE (-prev_balance) END,
						'credited_accounts', COALESCE(credited_accounts, '[]'::jsonb)
					)
					ORDER BY month_key
				) AS months
			FROM by_account_month
			GROUP BY account
		),
		-- Only include accounts that have some activity or non-zero saldo in the period
		accounts_with_activity AS (
			SELECT m.account
			FROM month_objects m
			JOIN by_account_month b ON b.account = m.account
			WHERE (b.debit_amount <> 0 OR b.credit_amount <> 0 OR b.prev_balance <> 0 OR b.next_balance <> 0)
			GROUP BY m.account
		),
		accounts_json AS (
			SELECT jsonb_agg(
				jsonb_build_object('account', mo.account, 'months', mo.months)
				ORDER BY mo.account
			) AS arr
			FROM month_objects mo
			WHERE EXISTS (SELECT 1 FROM accounts_with_activity a WHERE a.account = mo.account)
		)
		SELECT COALESCE((SELECT arr FROM accounts_json), '[]'::jsonb) INTO _result;
	ELSE
		-- Total report (no monthly breakdown)
		WITH
		period_ledger AS (
			SELECT l.debit, l.credit, l.amount, (l.created_date)::date AS tx_date
			FROM accounting.ledger l
			WHERE l.draft = false
			  AND (l.financing = _financing OR (l.financing IS NULL AND _financing IS NULL))
			  AND (l.created_date)::date BETWEEN _date_from AND _date_to
		),
		all_ledger AS (
			SELECT l.debit, l.credit, l.amount, (l.created_date)::date AS tx_date
			FROM accounting.ledger l
			WHERE l.draft = false
			  AND (l.financing = _financing OR (l.financing IS NULL AND _financing IS NULL))
			  AND (l.created_date)::date <= _date_to
		),
		accounts_in_period AS (
			SELECT DISTINCT acc AS account FROM (
				SELECT debit AS acc FROM period_ledger UNION SELECT credit AS acc FROM period_ledger
			) u
		),
		per_account AS (
			SELECT
				a.account,
				COALESCE(SUM(CASE WHEN pl.debit = a.account THEN pl.amount ELSE 0 END), 0)::numeric AS debit_amount,
				COALESCE(SUM(CASE WHEN pl.credit = a.account THEN pl.amount ELSE 0 END), 0)::numeric AS credit_amount,
				COALESCE((
					SELECT SUM(CASE WHEN al.debit = a.account THEN al.amount WHEN al.credit = a.account THEN -al.amount ELSE 0 END)
					FROM all_ledger al
					WHERE al.tx_date < _date_from AND (al.debit = a.account OR al.credit = a.account)
				), 0)::numeric AS prev_balance,
				COALESCE((
					SELECT SUM(CASE WHEN al.debit = a.account THEN al.amount WHEN al.credit = a.account THEN -al.amount ELSE 0 END)
					FROM all_ledger al
					WHERE al.tx_date <= _date_to AND (al.debit = a.account OR al.credit = a.account)
				), 0)::numeric AS next_balance,
				(
					SELECT jsonb_agg(jsonb_build_object('account', sub.credit, 'amount', sub.amount) ORDER BY sub.credit)
					FROM (
						SELECT pl_inner.credit, SUM(pl_inner.amount)::numeric AS amount
						FROM period_ledger pl_inner
						WHERE pl_inner.debit = a.account
						GROUP BY pl_inner.credit
					) sub
				) AS credited_accounts
			FROM accounts_in_period a
			LEFT JOIN period_ledger pl ON (pl.debit = a.account OR pl.credit = a.account)
			GROUP BY a.account
		),
		filtered AS (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'next_saldo_amount', CASE WHEN next_balance >= 0 THEN next_balance ELSE (-next_balance) END,
				'prev_saldo_debit', (prev_balance >= 0),
				'prev_saldo_amount', CASE WHEN prev_balance >= 0 THEN prev_balance ELSE (-prev_balance) END,
				'credited_accounts', COALESCE(credited_accounts, '[]'::jsonb)
			) AS obj
			FROM per_account
			WHERE (debit_amount <> 0 OR credit_amount <> 0 OR prev_balance <> 0 OR next_balance <> 0)
		),
		accounts_json AS (
			SELECT jsonb_agg(obj ORDER BY (obj->>'account')::integer) AS arr FROM filtered
		)
		SELECT COALESCE((SELECT arr FROM accounts_json), '[]'::jsonb) INTO _result;
	END IF;

	SELECT jsonb_array_length(_result) INTO _accounts_count;

	_result := jsonb_build_object(
		'status', 200,
		'total', COALESCE(_accounts_count, 0),
		'accounts', COALESCE(_result, '[]'::jsonb),
		'department', (
			select department
			from commons.department
			where id = 12
		)
	);

	RETURN _result;
END;
$BODY$;









