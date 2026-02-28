-- QUERY: 3-level account journal order (main account → counterparties → contracts) with secondary accounts
CREATE OR REPLACE FUNCTION reports.account_order_journal_counterparty_contract(
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
	RETURNS jsonb
	LANGUAGE plpgsql
	COST 100
	VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result jsonb;
BEGIN

	/* QUERY */
	WITH main AS (
		SELECT
			_account AS account,
			CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
			CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
			CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
			l.created_date::date AS created_date,
			cc.counterparty_id,
			cc.counterparty_name,
			l.contract_id,
			cc.contract_name
		FROM accounting.ledger l
		JOIN (
			SELECT
				cc.id,
				cc.contract AS contract_name,
				cc.counterparty_id,
				c.name AS counterparty_name
			FROM commons.counterparty_contracts cc
			JOIN accounting.counterparty c
				ON cc.counterparty_id = c.id
		) cc ON l.contract_id = cc.id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.contract_id IS NOT NULL
	),

	-- маълумотой счёти асоси (total row)
	account_agg AS (
		SELECT
			account,

			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			SUM(debit) FILTER (WHERE created_date < _date_from)
				- SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,

			SUM(debit) FILTER (WHERE created_date <= _date_to)
				- SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance
		FROM main
		GROUP BY account
	),

	-- счётхои баракс барои строкаи хамаги
	secondary_agg AS (
		SELECT
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount
		FROM main
		GROUP BY secondary_account
	),
	secondary_combined AS (
		SELECT
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', sa.secondary_account,
							'debit_amount', sa.debit_amount
						) ORDER BY sa.secondary_account
					)
					FROM secondary_agg sa
					WHERE COALESCE(sa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits_data,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', sa.secondary_account,
							'credit_amount', sa.credit_amount
						) ORDER BY sa.secondary_account
					)
					FROM secondary_agg sa
					WHERE COALESCE(sa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits_data
		FROM (SELECT 1) _one
	),
	account_combined AS (
		SELECT
			jsonb_build_object(
				'account', COALESCE(a.account, _account),
				'debit_amount', COALESCE(a.debit_amount, 0),
				'credit_amount', COALESCE(a.credit_amount, 0),
				CASE WHEN COALESCE(a.start_balance, 0) > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE WHEN COALESCE(a.end_balance, 0) > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
				ABS(COALESCE(a.end_balance, 0)),
				'debits', (SELECT debits_data FROM secondary_combined),
				'credits', (SELECT credits_data FROM secondary_combined)
			) AS account_data
		FROM account_agg a
	),

	-- Level 3: контрактхо (per contract amounts + saldos)
	contract_agg AS (
		SELECT
			account,
			counterparty_id,
			counterparty_name,
			contract_id,
			contract_name,

			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			SUM(debit) FILTER (WHERE created_date < _date_from)
				- SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,

			SUM(debit) FILTER (WHERE created_date <= _date_to)
				- SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance
		FROM main
		GROUP BY account, counterparty_id, counterparty_name, contract_id, contract_name
	),

	-- счётхои баракс барои ҳар контракт
	contract_secondary_agg AS (
		SELECT
			account,
			counterparty_id,
			contract_id,
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount
		FROM main
		GROUP BY account, counterparty_id, contract_id, secondary_account
	),

	-- ҳар як контракт бо debits/credits
	contract_rows AS (
		SELECT
			ca.account,
			ca.counterparty_id,
			ca.counterparty_name,
			ca.contract_id,
			ca.contract_name,
			ca.debit_amount,
			ca.credit_amount,
			ca.start_balance,
			ca.end_balance,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object('account', csa.secondary_account, 'debit_amount', csa.debit_amount)
						ORDER BY csa.secondary_account
					)
					FROM contract_secondary_agg csa
					WHERE csa.account = ca.account
					  AND csa.counterparty_id = ca.counterparty_id
					  AND csa.contract_id = ca.contract_id
					  AND COALESCE(csa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object('account', csa.secondary_account, 'credit_amount', csa.credit_amount)
						ORDER BY csa.secondary_account
					)
					FROM contract_secondary_agg csa
					WHERE csa.account = ca.account
					  AND csa.counterparty_id = ca.counterparty_id
					  AND csa.contract_id = ca.contract_id
					  AND COALESCE(csa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits
		FROM contract_agg ca
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
					ABS(COALESCE(end_balance, 0)),
					'debits', debits,
					'credits', credits
				)
				ORDER BY contract_name
			) AS contracts
		FROM contract_rows
		GROUP BY account, counterparty_id, counterparty_name
	),

	-- Level 2: ҷамъи контрагент (totals per counterparty)
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

	-- счётхои баракс барои ҳар контрагент
	counterparty_secondary_agg AS (
		SELECT
			account,
			counterparty_id,
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount
		FROM main
		GROUP BY account, counterparty_id, secondary_account
	),

	counterparty_count AS (
		SELECT COUNT(*) AS total_count FROM counterparty_totals
	),
	counterparty_paginated AS (
		SELECT *
		FROM counterparty_totals
		ORDER BY counterparty_name
		LIMIT _limit
		OFFSET _offset
	),

	-- ҳар як контрагент бо debits/credits
	counterparty_rows AS (
		SELECT
			cp.account,
			cp.counterparty_id,
			cp.counterparty_name,
			cp.debit_amount,
			cp.credit_amount,
			cp.start_balance,
			cp.end_balance,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object('account', csa.secondary_account, 'debit_amount', csa.debit_amount)
						ORDER BY csa.secondary_account
					)
					FROM counterparty_secondary_agg csa
					WHERE csa.account = cp.account
					  AND csa.counterparty_id = cp.counterparty_id
					  AND COALESCE(csa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object('account', csa.secondary_account, 'credit_amount', csa.credit_amount)
						ORDER BY csa.secondary_account
					)
					FROM counterparty_secondary_agg csa
					WHERE csa.account = cp.account
					  AND csa.counterparty_id = cp.counterparty_id
					  AND COALESCE(csa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits
		FROM counterparty_paginated cp
	),

	counterparty_grouped AS (
		SELECT
			cp.account,
			jsonb_agg(
				jsonb_build_object(
					'counterparty_id', cp.counterparty_id,
					'counterparty_name', cp.counterparty_name,
					'debit_amount', COALESCE(cp.debit_amount, 0),
					'credit_amount', COALESCE(cp.credit_amount, 0),
					CASE WHEN cp.start_balance > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
					ABS(COALESCE(cp.start_balance, 0)),
					CASE WHEN cp.end_balance > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
					ABS(COALESCE(cp.end_balance, 0)),
					'debits', cp.debits,
					'credits', cp.credits,
					'contracts', cg.contracts
				)
				ORDER BY cp.counterparty_name
			) AS counterparties
		FROM counterparty_rows cp
		JOIN contracts_grouped cg
			ON cg.account = cp.account
			AND cg.counterparty_id = cp.counterparty_id
		GROUP BY cp.account
	)

	-- final packaging
	SELECT jsonb_build_object(
		'status', 200,
		'type', 'counterparty_contract',
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'total_count', (SELECT total_count FROM counterparty_count),
		'account_data', (
			SELECT account_data FROM account_combined
		),
		'content_data', COALESCE(
			(SELECT counterparties FROM counterparty_grouped),
			'[]'::jsonb
		)
	) INTO _result;

	RETURN _result;
END;
$BODY$;



