



CREATE OR REPLACE FUNCTION reports.get_account_order_journal (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _related_selector varchar(100);
BEGIN
	
	/* WE DEFINE WHAT TYPE IS THE ACCOUNT */
	SELECT related_selector
	into _related_selector
	FROM accounting.accounts
	where account = _account;

	if _related_selector = 'cash_flow_article' then
		return reports.account_order_journal_cash_flow_article (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);

	elsif _related_selector = 'counterparty_contract' then
		return reports.account_order_journal_counterparty_contract (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	elsif _related_selector = 'staff' then
		return reports.account_order_journal_staff (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	elsif _related_selector = 'products' then
	 	return reports.account_order_journal_products (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	elsif _related_selector = 'estimates' then
	 	return reports.account_order_journal_estimates (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 	
	else
		return reports.account_order_journal_not_content (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
		
	end if;
END;
$BODY$;

























/*
  ========================
	 	 NO CONTENT
  ========================
*/

select * from accounting.ledger l
join accounting.accounts a
	on a.account = l.debit
where a.related_selector = 'staff'

select reports.account_order_journal_not_content (
	'budget',
	111100,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
)

CREATE OR REPLACE FUNCTION reports.account_order_journal_not_content(
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
			l.created_date::date AS created_date
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
	),

	-- маълумотой счёти асоси
	account_agg AS (
		SELECT
			account,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
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
			)  AS credit_amount
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
				CASE 
					WHEN COALESCE(a.start_balance, 0) > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE 
					WHEN COALESCE(a.end_balance, 0) > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(a.end_balance, 0)),
				'debits', (
					SELECT debits_data 
					FROM secondary_combined
				),
				'credits', (
					SELECT credits_data 
					FROM secondary_combined
				)
			) AS account_data
		FROM account_agg a
	)

	-- final packaging (account total row only; no content)
	SELECT jsonb_build_object(
		'status', 200,
		'type', 'account',
		'department', (
			SELECT department 
			FROM commons.department 
			WHERE id = 12
		),
		'total_count', 0,
		'account_data', (
			SELECT account_data 
			FROM account_combined
		)
	) INTO _result;

	RETURN _result;
END;
$BODY$;


















/*
  ========================
	 	  STAFF
  ========================
*/
CREATE OR REPLACE FUNCTION reports.account_order_journal_staff(
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
			l.staff_id,
			concat_ws(' ', s.lastname, s.firstname, s.middlename) AS fullname,
			l.created_date::date AS created_date
		FROM accounting.ledger l
		JOIN hr.staff s ON l.staff_id = s.id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.staff_id IS NOT NULL
	),

	-- маълумотой счёти асоси
	account_agg AS (
		SELECT
			account,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
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
			)  AS credit_amount
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
				CASE 
					WHEN COALESCE(a.start_balance, 0) > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE 
					WHEN COALESCE(a.end_balance, 0) > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(a.end_balance, 0)),
				'debits', (
					SELECT debits_data 
					FROM secondary_combined
				),
				'credits', (
					SELECT credits_data 
					FROM secondary_combined
				)
			) AS account_data
		FROM account_agg a
	),

	-- Контент
	content_agg AS (
		SELECT
			staff_id,
			fullname,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
		FROM main
		GROUP BY staff_id, fullname
	) ,
	content_count AS (
		SELECT COUNT(*) AS total_count FROM content_agg
	),
	content_paginated AS (
		SELECT *
		FROM content_agg
		ORDER BY fullname
		LIMIT _limit
		OFFSET _offset
	),

	-- счётхои баракс барои Контент
	content_secondary_agg AS (
		SELECT
			staff_id,
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			)  AS credit_amount
		FROM main
		GROUP BY staff_id, secondary_account
	),

	-- Строкаи хар як контент: счётхои асоси + ва счётхои баракс (дар массив)
	content_rows AS (
		SELECT
			cp.staff_id,
			cp.fullname,
			cp.debit_amount,
			cp.credit_amount,
			cp.start_balance,
			cp.end_balance,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'debit_amount', csa.debit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.staff_id = cp.staff_id 
					AND COALESCE(csa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'credit_amount', csa.credit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.staff_id = cp.staff_id 
					AND COALESCE(csa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits
		FROM content_paginated cp
	),
	content_combined AS (
		SELECT jsonb_agg(
			jsonb_build_object(
				'id', staff_id,
				'name', fullname,
				'debit_amount', COALESCE(debit_amount, 0),
				'credit_amount', COALESCE(credit_amount, 0),
				CASE 
					WHEN start_balance > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(start_balance, 0)),
				CASE 
					WHEN end_balance > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(end_balance, 0)),
				'debits', debits,
				'credits', credits
			)
			ORDER BY fullname
		) AS content_data
		FROM content_rows
	)

	-- final packaging
	SELECT jsonb_build_object(
		'status', 200,
		'type', 'staff',
		'department', (
			SELECT department 
			FROM commons.department 
			WHERE id = 12
		),
		'total_count', (
			SELECT total_count 
			FROM content_count
		),
		'account_data', (
			SELECT account_data 
			FROM account_combined
		),
		'content_data', COALESCE(
			(
				SELECT content_data 
				FROM content_combined
			), 
			'[]'::jsonb
		)
	) into _result;

	RETURN _result;
END;
$BODY$;








	





/*
  ========================
	 	 ESTIMATES
  ========================
*/
CREATE OR REPLACE FUNCTION reports.account_order_journal_estimates (
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
			
			coalesce(mo.estimate_id, poo.estimate_id, cpo.estimate_id) as estimate_id,
			e.name
		FROM accounting.ledger l
		left join accounting.manual_operations mo
			on l.id = mo.ledger_id
		left join accounting.payment_order_outgoing poo
			on l.id = poo.ledger_id
		left join accounting.cash_payment_order cpo
			on l.id = cpo.ledger_id
		left join accounting.estimates e
			on e.id = coalesce(mo.estimate_id, poo.estimate_id, cpo.estimate_id)
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  and coalesce(mo.estimate_id, poo.estimate_id, cpo.estimate_id) is not null
	),

	-- маълумотой счёти асоси
	account_agg AS (
		SELECT
			account,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
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
			)  AS credit_amount
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
				CASE 
					WHEN COALESCE(a.start_balance, 0) > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE 
					WHEN COALESCE(a.end_balance, 0) > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(a.end_balance, 0)),
				'debits', (
					SELECT debits_data 
					FROM secondary_combined
				),
				'credits', (
					SELECT credits_data 
					FROM secondary_combined
				)
			) AS account_data
		FROM account_agg a
	),

	-- Контент
	content_agg AS (
		SELECT
			estimate_id,
			name,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
		FROM main
		GROUP BY estimate_id, name
	) ,
	content_count AS (
		SELECT COUNT(*) AS total_count FROM content_agg
	),
	content_paginated AS (
		SELECT *
		FROM content_agg
		ORDER BY name->>'ru'
		LIMIT _limit
		OFFSET _offset
	),

	-- счётхои баракс барои Контент
	content_secondary_agg AS (
		SELECT
			estimate_id,
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			)  AS credit_amount
		FROM main
		GROUP BY estimate_id, secondary_account
	),

	-- Строкаи хар як контент: счётхои асоси + ва счётхои баракс (дар массив)
	content_rows AS (
		SELECT
			cp.estimate_id,
			cp.name,
			cp.debit_amount,
			cp.credit_amount,
			cp.start_balance,
			cp.end_balance,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'debit_amount', csa.debit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.estimate_id = cp.estimate_id 
					AND COALESCE(csa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'credit_amount', csa.credit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.estimate_id = cp.estimate_id 
					AND COALESCE(csa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits
		FROM content_paginated cp
	),
	content_combined AS (
		SELECT jsonb_agg(
			jsonb_build_object(
				'id', estimate_id,
				'name', name,
				'debit_amount', COALESCE(debit_amount, 0),
				'credit_amount', COALESCE(credit_amount, 0),
				CASE 
					WHEN start_balance > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(start_balance, 0)),
				CASE 
					WHEN end_balance > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(end_balance, 0)),
				'debits', debits,
				'credits', credits
			)
			ORDER BY name->>'ru'
		) AS content_data
		FROM content_rows
	)

	-- final packaging
	SELECT jsonb_build_object(
		'status', 200,
		'type', 'estimates',
		'department', (
			SELECT department 
			FROM commons.department 
			WHERE id = 12
		),
		'total_count', (
			SELECT total_count 
			FROM content_count
		),
		'account_data', (
			SELECT account_data 
			FROM account_combined
		),
		'content_data', COALESCE(
			(
				SELECT content_data 
				FROM content_combined
			), 
			'[]'::jsonb
		)
	) into _result;

	RETURN _result;
END;
$BODY$;






















/*
  ========================
	 	 PRODUCTS
  ========================
*/


select reports.account_order_journal_products(
	'budget',
	131230,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
)


select * from accounting.ledger l
join accounting.accounts a
	on a.account = l.debit
where a.related_selector = 'cash_flow_article'



CREATE OR REPLACE FUNCTION reports.account_order_journal_products (
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
			
			coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id) as name_id,
			n.name
		FROM accounting.ledger l
		left join accounting.warehouse_incoming wi
			on l.id = wi.ledger_id
		left join accounting.warehouse_outgoing wo
			on l.id = wo.ledger_id
		left join accounting.product_transfer pt
			on l.id = pt.ledger_id
		left join accounting.advance_report_tmzos art
			on l.id = art.ledger_id
		left join commons.nomenclature n
			on n.id = coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id)
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  and coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id) is not null
	),

	-- маълумотой счёти асоси
	account_agg AS (
		SELECT
			account,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
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
			)  AS credit_amount
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
				CASE 
					WHEN COALESCE(a.start_balance, 0) > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE 
					WHEN COALESCE(a.end_balance, 0) > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(a.end_balance, 0)),
				'debits', (
					SELECT debits_data 
					FROM secondary_combined
				),
				'credits', (
					SELECT credits_data 
					FROM secondary_combined
				)
			) AS account_data
		FROM account_agg a
	),

	-- Контент
	content_agg AS (
		SELECT
			name_id,
			name,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
		FROM main
		GROUP BY name_id, name
	) ,
	content_count AS (
		SELECT COUNT(*) AS total_count FROM content_agg
	),
	content_paginated AS (
		SELECT *
		FROM content_agg
		ORDER BY name->>'ru'
		LIMIT _limit
		OFFSET _offset
	),

	-- счётхои баракс барои Контент
	content_secondary_agg AS (
		SELECT
			name_id,
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			)  AS credit_amount
		FROM main
		GROUP BY name_id, secondary_account
	),

	-- Строкаи хар як контент: счётхои асоси + ва счётхои баракс (дар массив)
	content_rows AS (
		SELECT
			cp.name_id,
			cp.name,
			cp.debit_amount,
			cp.credit_amount,
			cp.start_balance,
			cp.end_balance,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'debit_amount', csa.debit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.name_id = cp.name_id 
					AND COALESCE(csa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'credit_amount', csa.credit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.name_id = cp.name_id 
					AND COALESCE(csa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits
		FROM content_paginated cp
	),
	content_combined AS (
		SELECT jsonb_agg(
			jsonb_build_object(
				'id', name_id,
				'name', name,
				'debit_amount', COALESCE(debit_amount, 0),
				'credit_amount', COALESCE(credit_amount, 0),
				CASE 
					WHEN start_balance > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(start_balance, 0)),
				CASE 
					WHEN end_balance > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(end_balance, 0)),
				'debits', debits,
				'credits', credits
			)
			ORDER BY name->>'ru'
		) AS content_data
		FROM content_rows
	)

	-- final packaging
	SELECT jsonb_build_object(
		'status', 200,
		'type', 'products',
		'department', (
			SELECT department 
			FROM commons.department 
			WHERE id = 12
		),
		'total_count', (
			SELECT total_count 
			FROM content_count
		),
		'account_data', (
			SELECT account_data 
			FROM account_combined
		),
		'content_data', COALESCE(
			(
				SELECT content_data 
				FROM content_combined
			), 
			'[]'::jsonb
		)
	) into _result;

	RETURN _result;
END;
$BODY$;


















/*
  ========================
	 CASH FLOW ARTICLES
  ========================
*/
CREATE OR REPLACE FUNCTION reports.account_order_journal_cash_flow_article (
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
			
			coalesce(
				por.cash_flow_article_id,
				pou.cash_flow_article_id,
				cpo.cash_flow_article_id,
				cro.cash_flow_article_id
			)  as cash_flow_article_id,
			cfa.name
		FROM accounting.ledger l
		left join accounting.payment_order_incoming por
			on l.id = por.ledger_id
		left join accounting.payment_order_outgoing pou
			on l.id = pou.ledger_id
		left join accounting.cash_payment_order cpo
			on l.id = cpo.ledger_id
		left join accounting.cash_receipt_order cro
			on l.id = cro.ledger_id
		left join commons.accouting_cash_flow_articles cfa
			on cfa.id = coalesce(
				por.cash_flow_article_id,
				pou.cash_flow_article_id,
				cpo.cash_flow_article_id,
				cro.cash_flow_article_id
			)
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  and coalesce(
			por.cash_flow_article_id,
			pou.cash_flow_article_id,
			cpo.cash_flow_article_id,
			cro.cash_flow_article_id
		) is not null
	),

	-- маълумотой счёти асоси
	account_agg AS (
		SELECT
			account,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
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
			)  AS credit_amount
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
				CASE 
					WHEN COALESCE(a.start_balance, 0) > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(a.start_balance, 0)),
				CASE 
					WHEN COALESCE(a.end_balance, 0) > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(a.end_balance, 0)),
				'debits', (
					SELECT debits_data 
					FROM secondary_combined
				),
				'credits', (
					SELECT credits_data 
					FROM secondary_combined
				)
			) AS account_data
		FROM account_agg a
	),

	-- Контент
	content_agg AS (
		SELECT
			cash_flow_article_id,
			name,

			-- гардиш
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,

			-- салдо бар аввали гардиш
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,

			-- салдо бар охири гардиш
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
		FROM main
		GROUP BY cash_flow_article_id, name
	) ,
	content_count AS (
		SELECT COUNT(*) AS total_count FROM content_agg
	),
	content_paginated AS (
		SELECT *
		FROM content_agg
		ORDER BY name->>'ru'
		LIMIT _limit
		OFFSET _offset
	),

	-- счётхои баракс барои Контент
	content_secondary_agg AS (
		SELECT
			cash_flow_article_id,
			secondary_account,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			)  AS credit_amount
		FROM main
		GROUP BY cash_flow_article_id, secondary_account
	),

	-- Строкаи хар як контент: счётхои асоси + ва счётхои баракс (дар массив)
	content_rows AS (
		SELECT
			cp.cash_flow_article_id,
			cp.name,
			cp.debit_amount,
			cp.credit_amount,
			cp.start_balance,
			cp.end_balance,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'debit_amount', csa.debit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.cash_flow_article_id = cp.cash_flow_article_id 
					AND COALESCE(csa.debit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS debits,
			COALESCE(
				(
					SELECT jsonb_agg(
						jsonb_build_object(
							'account', csa.secondary_account, 
							'credit_amount', csa.credit_amount
						) ORDER BY csa.secondary_account
					)
				 	FROM content_secondary_agg csa 
					WHERE csa.cash_flow_article_id = cp.cash_flow_article_id 
					AND COALESCE(csa.credit_amount, 0) <> 0
				),
				'[]'::jsonb
			) AS credits
		FROM content_paginated cp
	),
	content_combined AS (
		SELECT jsonb_agg(
			jsonb_build_object(
				'id', cash_flow_article_id,
				'name', name,
				'debit_amount', COALESCE(debit_amount, 0),
				'credit_amount', COALESCE(credit_amount, 0),
				CASE 
					WHEN start_balance > 0 
					THEN 'prev_saldo_debit' 
					ELSE 'prev_saldo_credit' 
				END,
				ABS(COALESCE(start_balance, 0)),
				CASE 
					WHEN end_balance > 0 
					THEN 'next_saldo_debit' 
					ELSE 'next_saldo_credit' 
				END,
				ABS(COALESCE(end_balance, 0)),
				'debits', debits,
				'credits', credits
			)
			ORDER BY name->>'ru'
		) AS content_data
		FROM content_rows
	)

	-- final packaging
	SELECT jsonb_build_object(
		'status', 200,
		'type', 'cash_flow_article',
		'department', (
			SELECT department 
			FROM commons.department 
			WHERE id = 12
		),
		'total_count', (
			SELECT total_count 
			FROM content_count
		),
		'account_data', (
			SELECT account_data 
			FROM account_combined
		),
		'content_data', COALESCE(
			(
				SELECT content_data 
				FROM content_combined
			), 
			'[]'::jsonb
		)
	) into _result;

	RETURN _result;
END;
$BODY$;























/*
  ==============================
	 COUNTERPARTY CONTRACTS
  ==============================
*/

select * from accounting.accounts a
left join accounting.ledger l
	on a.account = l.debit
where related_selector = ''

SELECT reports.account_order_journal_counterparty_contract (
	'budget',
	211110,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
)

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

