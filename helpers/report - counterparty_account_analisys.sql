

-- counterparty ids = 150, 151, 149, 152




select reports.get_counterparty_account_analysis (
	'budget',
	'2026-01-01',
	'2026-03-03',
	150,
	1000,
	0
);



CREATE OR REPLACE FUNCTION reports.get_counterparty_account_analysis (
	_financing accounting.budget_distribution_type,
	_date_from date,
	_date_to date,
	_counterparty_id integer DEFAULT NULL,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
	RETURNS json
	LANGUAGE plpgsql
	COST 100
	VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result jsonb;
BEGIN

	/* 	
		MAIN: expand each ledger row to both account sides (debit and credit) 
		so we get every account related to the counterparty 
	*/
	WITH cc AS (
		SELECT
			cc.id,
			cc.contract AS contract_name,
			cc.counterparty_id,
			c.name AS counterparty_name
		FROM commons.counterparty_contracts cc
		JOIN accounting.counterparty c ON cc.counterparty_id = c.id
	),
	main AS (
		-- debit-account side: this account is debited
		SELECT
			l.debit AS account,
			l.amount AS debit,
			0::numeric AS credit,
			cc.counterparty_id,
			cc.counterparty_name,
			l.contract_id,
			cc.contract_name,
			l.created_date::date AS created_date
		FROM accounting.ledger l
		JOIN cc ON l.contract_id = cc.id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND l.contract_id IS NOT NULL
		  AND (_counterparty_id IS NULL OR cc.counterparty_id = _counterparty_id)
		  
		UNION ALL
		
		-- credit-account side: this account is credited
		SELECT
			l.credit AS account,
			0::numeric AS debit,
			l.amount AS credit,
			cc.counterparty_id,
			cc.counterparty_name,
			l.contract_id,
			cc.contract_name,
			l.created_date::date AS created_date
		FROM accounting.ledger l
		JOIN cc ON l.contract_id = cc.id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND l.contract_id IS NOT NULL
		  AND (_counterparty_id IS NULL OR cc.counterparty_id = _counterparty_id)
	),

	-- per-account totals (all accounts that were touched)
	account_agg AS (
		SELECT
			account,
			SUM(debit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_amount,
			SUM(credit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_amount,
			SUM(debit) FILTER (WHERE created_date < _date_from)
				- SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,
			SUM(debit) FILTER (WHERE created_date <= _date_to)
				- SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance
		FROM main
		GROUP BY account
	),
	account_count AS (
		SELECT COUNT(*) AS total_count FROM account_agg
	),
	account_paginated AS (
		SELECT *
		FROM account_agg
		ORDER BY account
		LIMIT _limit
		OFFSET _offset
	),

	-- per account, counterparty, contract
	contract_agg AS (
		SELECT
			account,
			counterparty_id,
			counterparty_name,
			contract_id,
			contract_name,
			SUM(debit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_amount,
			SUM(credit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_amount,
			SUM(debit) FILTER (WHERE created_date < _date_from)
				- SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,
			SUM(debit) FILTER (WHERE created_date <= _date_to)
				- SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance
		FROM main
		GROUP BY account, counterparty_id, counterparty_name, contract_id, contract_name
	),

	counterparty_totals AS (
		SELECT
			account,
			counterparty_id,
			counterparty_name,
			SUM(debit_amount) AS debit_amount,
			SUM(credit_amount) AS credit_amount,
			SUM(start_balance) AS start_balance,
			SUM(end_balance) AS end_balance
		FROM contract_agg
		GROUP BY account, counterparty_id, counterparty_name
	),

	contracts_grouped AS (
		SELECT
			account,
			counterparty_id,
			counterparty_name,
			jsonb_agg(
				jsonb_build_object(
					'contract_id', contract_id,
					'contract_name', contract_name,
					'debit_amount', COALESCE(debit_amount, 0),
					'credit_amount', COALESCE(credit_amount, 0),
					CASE WHEN start_balance > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
					ABS(COALESCE(start_balance, 0)),
					CASE WHEN end_balance > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
					ABS(COALESCE(end_balance, 0))
				)
				ORDER BY contract_name
			) AS contracts
		FROM contract_agg
		GROUP BY account, counterparty_id, counterparty_name
	),

	-- one row per account: list of counterparties (with contracts) that touched this account
	account_counterparties AS (
		SELECT
			ct.account,
			jsonb_agg(
				jsonb_build_object(
					'counterparty_id', ct.counterparty_id,
					'counterparty_name', ct.counterparty_name,
					'debit_amount', COALESCE(ct.debit_amount, 0),
					'credit_amount', COALESCE(ct.credit_amount, 0),
					CASE WHEN ct.start_balance > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
					ABS(COALESCE(ct.start_balance, 0)),
					CASE WHEN ct.end_balance > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
					ABS(COALESCE(ct.end_balance, 0)),
					'contracts', cg.contracts
				)
				ORDER BY ct.counterparty_name
			) AS counterparties
		FROM counterparty_totals ct
		JOIN contracts_grouped cg
			ON cg.account = ct.account AND cg.counterparty_id = ct.counterparty_id
		GROUP BY ct.account
	),

	-- table_data: one object per account (paginated), with account, amounts, saldos, counterparties
	table_data AS (
		SELECT jsonb_agg(
			jsonb_build_object(
				'account', a.account,
				'debit_amount', COALESCE(a.debit_amount, 0),
				'credit_amount', COALESCE(a.credit_amount, 0),
				CASE WHEN COALESCE(a.start_balance, 0) > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE WHEN COALESCE(a.end_balance, 0) > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
				ABS(COALESCE(a.end_balance, 0)),
				'counterparties', COALESCE(ac.counterparties, '[]'::jsonb)
			)
			ORDER BY a.account
		) AS table_data
		FROM account_paginated a
		LEFT JOIN account_counterparties ac ON ac.account = a.account
	)

	SELECT jsonb_build_object(
		'status', 200,
		'type', 'counterparty_contract',
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'total_count', (SELECT total_count FROM account_count),
		'table_data', COALESCE((SELECT table_data FROM table_data), '[]'::jsonb)
	) INTO _result;

	RETURN _result;
END;
$BODY$;








