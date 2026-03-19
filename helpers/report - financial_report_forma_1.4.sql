


select reports.get_financial_reports_form_1_4(
	12,
	'2026-01-01',
	'2026-04-04'
);



select * from accounting.ledger
where debit in (
	131100, 131210, 131211, 
		131212, 131213, 131214, 
		131215, 131216, 131217, 
		131218, 131219, 131250, 
		131000
) or credit in (
131100, 131210, 131211, 
		131212, 131213, 131214, 
		131215, 131216, 131217, 
		131218, 131219, 131250, 
		131000
);



CREATE OR REPLACE FUNCTION reports.get_financial_reports_form_1_4(
	_department_id integer,
	_date_from date,
	_date_to date
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result jsonb;
	-- accounts for form 1.4
	form_accounts int[] := array[
		131100, 131210, 131211, 
		131212, 131213, 131214, 
		131215, 131216, 131217, 
		131218, 131219, 131250, 
		131000
	];
BEGIN

	/* MAIN QUERY */
	WITH
	-- ledger: unpivot so each row has account, amount as debit or credit, financing, date
	ledger_unpivot AS (
		SELECT
			l.debit AS account,
			l.amount AS debit_amt,
			0::numeric AS credit_amt,
			l.financing,
			l.created_date::date AS created_date
		FROM accounting.ledger l
		LEFT JOIN commons.department d ON l.main_department_id = d.id
		WHERE l.draft IS NOT TRUE
		  AND (l.main_department_id = _department_id OR d.parent_id = _department_id)
		  AND l.debit = ANY(form_accounts)
		UNION ALL
		SELECT
			l.credit,
			0::numeric,
			l.amount,
			l.financing,
			l.created_date::date
		FROM accounting.ledger l
		LEFT JOIN commons.department d ON l.main_department_id = d.id
		WHERE l.draft IS NOT TRUE
		  AND (l.main_department_id = _department_id OR d.parent_id = _department_id)
		  AND l.credit = ANY(form_accounts)
	),
	ledger_agg AS (
		SELECT
			account,
			SUM(debit_amt) FILTER (WHERE created_date < _date_from)
				- SUM(credit_amt) FILTER (WHERE created_date < _date_from) AS prev_saldo,
			SUM(debit_amt) FILTER (WHERE created_date <= _date_to)
				- SUM(credit_amt) FILTER (WHERE created_date <= _date_to) AS next_saldo,
			COALESCE(SUM(debit_amt) FILTER (WHERE created_date BETWEEN _date_from AND _date_to), 0) AS debit_amount,
			COALESCE(SUM(credit_amt) FILTER (WHERE created_date BETWEEN _date_from AND _date_to), 0) AS credit_amount,
			financing
		FROM ledger_unpivot
		GROUP BY account, financing
	),
	ledger_by_account AS (
		SELECT
			account,
			ABS(SUM(prev_saldo)) AS prev_saldo_total,
			ABS(SUM(prev_saldo) FILTER (WHERE financing = 'special')) AS prev_saldo_special,
			SUM(debit_amount) AS debit_total_amount,
			SUM(debit_amount) FILTER (WHERE financing = 'special') AS debit_special_amount,
			SUM(credit_amount) AS credit_total_amount,
			SUM(credit_amount) FILTER (WHERE financing = 'special') AS credit_special_amount,
			ABS(SUM(next_saldo)) AS next_saldo_total,
			ABS(SUM(next_saldo) FILTER (WHERE financing = 'special')) AS next_saldo_special
		FROM ledger_agg
		GROUP BY account
	),
	table_data_rows AS (
		SELECT jsonb_build_object(
			'account', ac.account::text,
			'prev_saldo_total_amount', COALESCE(l.prev_saldo_total, 0),
			'prev_saldo_special_amount', COALESCE(l.prev_saldo_special, 0),
			'debit_total_amount', COALESCE(l.debit_total_amount, 0),
			'debit_special_amount', COALESCE(l.debit_special_amount, 0),
			'credit_total_amount', COALESCE(l.credit_total_amount, 0),
			'credit_special_amount', COALESCE(l.credit_special_amount, 0),
			'next_saldo_total_amount', COALESCE(l.next_saldo_total, 0),
			'next_saldo_special_amount', COALESCE(l.next_saldo_special, 0)
		) AS obj
		FROM (SELECT unnest(form_accounts) AS account) ac
		LEFT JOIN ledger_by_account l ON l.account = ac.account
		ORDER BY ac.account
	)
	SELECT jsonb_build_object(
		'status', 200,
		'table_data', COALESCE(
			(SELECT jsonb_agg(obj ORDER BY (obj->>'account')) FROM table_data_rows),
			'[]'::jsonb
		)
	) INTO _result;

	RETURN _result;
END;
$BODY$;




