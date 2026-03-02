




CREATE OR REPLACE FUNCTION reports.get_account_analysis (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_staff_id bigint DEFAULT null,
	products_expanded boolean default false,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
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
		return reports.account_analysis_cash_flow_article (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);

	 elsif _related_selector = 'counterparty_contract' then
		return reports.account_analysis_counterparty_contract (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'staff' then
		return reports.account_analysis_staff (
				_financing,
				_account,
				_date_from,
				_date_to,
				_staff_id,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'products' then
	 	return reports.account_analysis_products (
				_financing,
				_account,
				_date_from,
				_date_to,
				products_expanded,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'estimates' then
	 	return reports.account_analysis_estimates (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 	
	 else
	 	return reports.account_analysis_not_content (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
		
	 end if;
end;
$BODY$;














/* -------------------------------------- */
--          products
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'products';

select * from payments.payroll_sheet_line

select reports.account_analysis_products (
	'budget',
	131230,
	'2026-01-01',
	'2026-03-03',
	true,
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_analysis_products (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	products_expanded boolean default false,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN

	if products_expanded is true then
		/* MAIN QUERY */
		WITH main AS (
			SELECT
				_account AS account,
				CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
				CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
				CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
				l.created_date::date AS created_date,
				COALESCE(wi.quantity, wo.quantity, pt.quantity, art.quantity, 0) AS quantity,
				CASE WHEN l.debit = _account THEN COALESCE(wi.quantity, wo.quantity, pt.quantity, art.quantity, 0) ELSE 0 END AS debit_quantity,
				CASE WHEN l.credit = _account THEN COALESCE(wi.quantity, wo.quantity, pt.quantity, art.quantity, 0) ELSE 0 END AS credit_quantity,
				COALESCE(wi.name_id, wo.name_id, pt.name_id, art.name_id) AS name_id,
				COALESCE(
					wi.storage_location_id,
					wo.storage_location_id,
					CASE WHEN l.debit = _account THEN pt.from_storage_location_id ELSE pt.to_storage_location_id END,
					1
				) AS storage_location_id,
				n.name AS product_name,
				sl.name AS location_name
			FROM accounting.ledger l
			LEFT JOIN accounting.warehouse_incoming wi ON l.id = wi.ledger_id
			LEFT JOIN accounting.warehouse_outgoing wo ON l.id = wo.ledger_id
			LEFT JOIN accounting.product_transfer pt ON l.id = pt.ledger_id
			LEFT JOIN accounting.advance_report_tmzos art ON l.id = art.ledger_id
			LEFT JOIN commons.nomenclature n ON n.id = COALESCE(wi.name_id, wo.name_id, pt.name_id, art.name_id)
			LEFT JOIN commons.storage_location sl ON sl.id = COALESCE(
				wi.storage_location_id,
				wo.storage_location_id,
				CASE WHEN l.debit = _account THEN pt.from_storage_location_id ELSE pt.to_storage_location_id END,
				1
			)
			WHERE l.draft IS NOT TRUE
			  AND l.financing = _financing
			  AND (l.debit = _account OR l.credit = _account)
			  AND COALESCE(wi.name_id, wo.name_id, pt.name_id, art.name_id) IS NOT NULL
			  AND COALESCE(
				wi.storage_location_id,
				wo.storage_location_id,
				CASE WHEN l.debit = _account THEN pt.from_storage_location_id ELSE pt.to_storage_location_id END,
				1
			  ) IS NOT NULL
		),
		
		-- Main account totals (amount + quantity, saldos)
		account_agg AS (
			SELECT
				account,
				SUM(debit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_amount,
				SUM(credit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_amount,
				SUM(debit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_quantity,
				SUM(credit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_quantity,
				SUM(debit) FILTER (WHERE created_date < _date_from) - SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,
				SUM(debit) FILTER (WHERE created_date <= _date_to) - SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance,
				SUM(debit_quantity) FILTER (WHERE created_date < _date_from) - SUM(credit_quantity) FILTER (WHERE created_date < _date_from) AS start_balance_qty,
				SUM(debit_quantity) FILTER (WHERE created_date <= _date_to) - SUM(credit_quantity) FILTER (WHERE created_date <= _date_to) AS end_balance_qty
			FROM main
			GROUP BY account
		),
		
		-- Secondary (correspondent) account totals: swap debit/credit vs main
		secondary_agg AS (
			SELECT
				secondary_account,
				SUM(credit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_amount,
				SUM(debit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_amount,
				SUM(credit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_quantity,
				SUM(debit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_quantity,
				SUM(credit) FILTER (WHERE created_date < _date_from) - SUM(debit) FILTER (WHERE created_date < _date_from) AS start_balance,
				SUM(credit) FILTER (WHERE created_date <= _date_to) - SUM(debit) FILTER (WHERE created_date <= _date_to) AS end_balance,
				SUM(credit_quantity) FILTER (WHERE created_date < _date_from) - SUM(debit_quantity) FILTER (WHERE created_date < _date_from) AS start_balance_qty,
				SUM(credit_quantity) FILTER (WHERE created_date <= _date_to) - SUM(debit_quantity) FILTER (WHERE created_date <= _date_to) AS end_balance_qty
			FROM main
			GROUP BY secondary_account
		),
		secondary_count AS (
			SELECT COUNT(*) AS total_count FROM secondary_agg
		),
		secondary_paginated AS (
			SELECT * FROM secondary_agg
			ORDER BY secondary_account
			LIMIT _limit OFFSET _offset
		),
		
		-- Per secondary_account + name_id (product): for products array
		product_agg AS (
			SELECT
				secondary_account,
				name_id,
				(array_agg(product_name))[1] AS product_name,
				SUM(debit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_amount,
				SUM(credit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_amount,
				SUM(debit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_quantity,
				SUM(credit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_quantity,
				SUM(debit) FILTER (WHERE created_date < _date_from) - SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,
				SUM(debit) FILTER (WHERE created_date <= _date_to) - SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance,
				SUM(debit_quantity) FILTER (WHERE created_date < _date_from) - SUM(credit_quantity) FILTER (WHERE created_date < _date_from) AS start_balance_qty,
				SUM(debit_quantity) FILTER (WHERE created_date <= _date_to) - SUM(credit_quantity) FILTER (WHERE created_date <= _date_to) AS end_balance_qty
			FROM main
			GROUP BY secondary_account, name_id
		),
		-- Per secondary_account + name_id + storage_location_id: for locations array
		location_agg AS (
			SELECT
				secondary_account,
				name_id,
				storage_location_id,
				(array_agg(location_name))[1] AS location_name,
				SUM(debit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_amount,
				SUM(credit) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_amount,
				SUM(debit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS debit_quantity,
				SUM(credit_quantity) FILTER (WHERE created_date BETWEEN _date_from AND _date_to) AS credit_quantity,
				SUM(debit) FILTER (WHERE created_date < _date_from) - SUM(credit) FILTER (WHERE created_date < _date_from) AS start_balance,
				SUM(debit) FILTER (WHERE created_date <= _date_to) - SUM(credit) FILTER (WHERE created_date <= _date_to) AS end_balance,
				SUM(debit_quantity) FILTER (WHERE created_date < _date_from) - SUM(credit_quantity) FILTER (WHERE created_date < _date_from) AS start_balance_qty,
				SUM(debit_quantity) FILTER (WHERE created_date <= _date_to) - SUM(credit_quantity) FILTER (WHERE created_date <= _date_to) AS end_balance_qty
			FROM main
			GROUP BY secondary_account, name_id, storage_location_id
		),
		
		-- Build location objects (per secondary + product): location_id, location_name, amounts, quantities, prev/next saldo (amount key + quantity)
		locations_json AS (
			SELECT
				la.secondary_account,
				la.name_id,
				jsonb_agg(
					jsonb_build_object(
						'location_id', la.storage_location_id,
						'location_name', la.location_name,
						'debit_amount', COALESCE(la.debit_amount, 0),
						'debit_quantity', COALESCE(la.debit_quantity, 0),
						'credit_amount', COALESCE(la.credit_amount, 0),
						'credit_quantity', COALESCE(la.credit_quantity, 0),
						CASE WHEN COALESCE(la.end_balance, 0) > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
						ABS(COALESCE(la.end_balance, 0)),
						'next_saldo_quantity', ABS(COALESCE(la.end_balance_qty, 0)),
						CASE WHEN COALESCE(la.start_balance, 0) > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
						ABS(COALESCE(la.start_balance, 0)),
						'prev_saldo_quantity', ABS(COALESCE(la.start_balance_qty, 0))
					)
					ORDER BY la.storage_location_id
				) AS locations
			FROM location_agg la
			GROUP BY la.secondary_account, la.name_id
		),
		
		-- Build product rows with locations (per secondary)
		products_json AS (
			SELECT
				pa.secondary_account,
				pa.name_id,
				pa.product_name,
				pa.debit_amount,
				pa.credit_amount,
				pa.debit_quantity,
				pa.credit_quantity,
				pa.start_balance,
				pa.end_balance,
				pa.start_balance_qty,
				pa.end_balance_qty,
				COALESCE(lj.locations, '[]'::jsonb) AS locations
			FROM product_agg pa
			LEFT JOIN locations_json lj ON lj.secondary_account = pa.secondary_account AND lj.name_id = pa.name_id
		),
		
		-- Products array per secondary account (each product: product_id, product_name, amounts, quantities, saldos, locations)
		products_per_secondary AS (
			SELECT
				secondary_account,
				jsonb_agg(
					jsonb_build_object(
						'product_id', name_id,
						'product_name', product_name,
						'debit_amount', COALESCE(debit_amount, 0),
						'debit_quantity', COALESCE(debit_quantity, 0),
						'credit_amount', COALESCE(credit_amount, 0),
						'credit_quantity', COALESCE(credit_quantity, 0),
						CASE WHEN COALESCE(end_balance, 0) > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
						ABS(COALESCE(end_balance, 0)),
						'next_saldo_quantity', ABS(COALESCE(end_balance_qty, 0)),
						CASE WHEN COALESCE(start_balance, 0) > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
						ABS(COALESCE(start_balance, 0)),
						'prev_saldo_quantity', ABS(COALESCE(start_balance_qty, 0)),
						'locations', locations
					)
					ORDER BY name_id
				) AS products
			FROM products_json
			GROUP BY secondary_account
		),
		
		-- Secondary account objects with products array (paginated: only _limit secondaries at _offset)
		secondary_accounts_json AS (
			SELECT
				jsonb_agg(
					jsonb_build_object(
						'account', sa.secondary_account,
						'debit_amount', COALESCE(sa.debit_amount, 0),
						'debit_quantity', COALESCE(sa.debit_quantity, 0),
						'credit_amount', COALESCE(sa.credit_amount, 0),
						'credit_quantity', COALESCE(sa.credit_quantity, 0),
						CASE WHEN COALESCE(sa.end_balance, 0) > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
						ABS(COALESCE(sa.end_balance, 0)),
						'next_saldo_quantity', ABS(COALESCE(sa.end_balance_qty, 0)),
						CASE WHEN COALESCE(sa.start_balance, 0) > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
						ABS(COALESCE(sa.start_balance, 0)),
						'prev_saldo_quantity', ABS(COALESCE(sa.start_balance_qty, 0)),
						'products', COALESCE(pps.products, '[]'::jsonb)
					)
					ORDER BY sa.secondary_account
				) AS secondary_accounts
			FROM secondary_paginated sa
			LEFT JOIN products_per_secondary pps ON pps.secondary_account = sa.secondary_account
		),
		
		account_combined AS (
			SELECT
				jsonb_build_object(
					'account', COALESCE(a.account, _account),
					'debit_amount', COALESCE(a.debit_amount, 0),
					'debit_quantity', COALESCE(a.debit_quantity, 0),
					'credit_amount', COALESCE(a.credit_amount, 0),
					'credit_quantity', COALESCE(a.credit_quantity, 0),
					CASE WHEN COALESCE(a.end_balance, 0) > 0 THEN 'next_saldo_debit' ELSE 'next_saldo_credit' END,
					ABS(COALESCE(a.end_balance, 0)),
					'next_saldo_quantity', ABS(COALESCE(a.end_balance_qty, 0)),
					CASE WHEN COALESCE(a.start_balance, 0) > 0 THEN 'prev_saldo_debit' ELSE 'prev_saldo_credit' END,
					ABS(COALESCE(a.start_balance, 0)),
					'prev_saldo_quantity', ABS(COALESCE(a.start_balance_qty, 0)),
					'secondary_accounts', (SELECT COALESCE(secondary_accounts, '[]'::jsonb) FROM secondary_accounts_json)
				) AS account_data
			FROM (SELECT _account AS account) params
			LEFT JOIN account_agg a ON a.account = params.account
		)
		
		SELECT jsonb_build_object(
			'status', 200,
			'type', 'products',
			'department', (
				SELECT department
				FROM commons.department
				WHERE id = 12
			),
			'total_count', (SELECT total_count FROM secondary_count),
			'account_data', (SELECT account_data FROM account_combined)
		) INTO _result;
		
	else
		/* MAIN QUERY */
		WITH main AS (
			SELECT
				_account AS account,
				CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
				CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
				CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
				l.created_date::date AS created_date
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
	
		-- счётхои баракс (reverse/correspondent): 
		-- when main is debited secondary is credited and vice versa; 
		-- secondary can equal main for same-account transfers
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
		secondary_count AS (
			SELECT COUNT(*) AS total_count FROM secondary_agg
		),
		secondary_accounts_json AS (
			SELECT COALESCE(
				jsonb_agg(
					jsonb_build_object(
						'account', secondary_account,
						'debit_amount', COALESCE(debit_amount, 0),
						'credit_amount', COALESCE(credit_amount, 0)
					) ORDER BY secondary_account
				),
				'[]'::jsonb
			) AS secondary_accounts
			FROM secondary_agg
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
					'secondary_accounts', (SELECT secondary_accounts FROM secondary_accounts_json)
				) AS account_data
			FROM account_agg a
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
			'total_count', (SELECT total_count FROM secondary_count),
			'account_data', (SELECT account_data FROM account_combined)
		) into _result;
	end if;

	 return _result;
end;
$BODY$;



















/* -------------------------------------- */
--          account
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector is null;

select * from payments.payroll_sheet_line

select reports.account_analysis_not_content (
	'budget',
	111100,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_analysis_not_content (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN

	/* MAIN QUERY */
	WITH main AS (
		SELECT
			_account AS account,
			CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
			CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
			l.created_date::date AS created_date
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
	),

	account_agg AS (
		SELECT
			account,
			SUM(debit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS debit_amount,
			SUM(credit) FILTER (
				WHERE created_date BETWEEN _date_from AND _date_to
			) AS credit_amount,
			SUM(debit) FILTER (
				WHERE created_date < _date_from
			) - SUM(credit) FILTER (
				WHERE created_date < _date_from
			) AS start_balance,
			SUM(debit) FILTER (
				WHERE created_date <= _date_to
			) - SUM(credit) FILTER (
				WHERE created_date <= _date_to
			) AS end_balance
		FROM main
		GROUP BY account
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
				ABS(COALESCE(a.end_balance, 0))
			) AS account_data
		FROM account_agg a
	)

	SELECT jsonb_build_object(
		'status', 200,
		'type', 'account',
		'department', (
			SELECT department 
			FROM commons.department 
			WHERE id = 12
		),
		'total_count', 1,
		'account_data', (SELECT account_data FROM account_combined)
	) INTO _result;

	 return _result;
end;
$BODY$;





















/* -------------------------------------- */
--          estimates
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'estimates';

select * from payments.payroll_sheet_line

select reports.account_analysis_estimates (
	'budget',
	510100,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_analysis_estimates (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN

	/* MAIN QUERY */
	WITH main AS (
		SELECT
			_account AS account,
			CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
			CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
			CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
			l.created_date::date AS created_date
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

	-- счётхои баракс (reverse/correspondent): 
	-- when main is debited secondary is credited and vice versa; 
	-- secondary can equal main for same-account transfers
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
	secondary_count AS (
		SELECT COUNT(*) AS total_count FROM secondary_agg
	),
	secondary_accounts_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'account', secondary_account,
					'debit_amount', COALESCE(debit_amount, 0),
					'credit_amount', COALESCE(credit_amount, 0)
				) ORDER BY secondary_account
			),
			'[]'::jsonb
		) AS secondary_accounts
		FROM secondary_agg
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
				'secondary_accounts', (SELECT secondary_accounts FROM secondary_accounts_json)
			) AS account_data
		FROM account_agg a
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
		'total_count', (SELECT total_count FROM secondary_count),
		'account_data', (SELECT account_data FROM account_combined)
	) into _result;

	 return _result;
end;
$BODY$;


















/* -------------------------------------- */
--          staff
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'staff';


select * from payments.payroll_sheet_line

select reports.account_analysis_staff (
	'budget',
	111110,
	'2026-01-01',
	'2026-03-03',
	null,
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_analysis_staff (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_staff_id bigint default null,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN

	/* MAIN QUERY */
	WITH main AS (
		SELECT
			_account AS account,
			CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
			CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
			CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
			l.created_date::date AS created_date
		FROM accounting.ledger l
		LEFT JOIN hr.staff s ON l.staff_id = s.id
		LEFT JOIN payments.payroll_sheet_line p 
			ON l.payroll_sheet_line_id = p.id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND (s.id IS NOT NULL or p.id is not null)
		  AND (_staff_id is null or _staff_id = coalesce(s.id, p.staff_id))
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

	-- счётхои баракс (reverse/correspondent): 
	-- when main is debited secondary is credited and vice versa; 
	-- secondary can equal main for same-account transfers
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
	secondary_count AS (
		SELECT COUNT(*) AS total_count FROM secondary_agg
	),
	secondary_accounts_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'account', secondary_account,
					'debit_amount', COALESCE(debit_amount, 0),
					'credit_amount', COALESCE(credit_amount, 0)
				) ORDER BY secondary_account
			),
			'[]'::jsonb
		) AS secondary_accounts
		FROM secondary_agg
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
				'secondary_accounts', (SELECT secondary_accounts FROM secondary_accounts_json)
			) AS account_data
		FROM account_agg a
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
		'total_count', (SELECT total_count FROM secondary_count),
		'account_data', (SELECT account_data FROM account_combined)
	) into _result;

	 return _result;
end;
$BODY$;
















/* -------------------------------------- */
--          counterparty_contract
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'counterparty_contract';


select reports.account_analysis_counterparty_contract (
	'budget',
	211110,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_analysis_counterparty_contract (
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN

	/* MAIN QUERY */
	WITH main AS (
		SELECT
			_account AS account,
			CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
			CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
			CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
			l.created_date::date AS created_date
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

	-- счётхои баракс (reverse/correspondent): 
	-- when main is debited secondary is credited and vice versa; 
	-- secondary can equal main for same-account transfers
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
	secondary_count AS (
		SELECT COUNT(*) AS total_count FROM secondary_agg
	),
	secondary_accounts_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'account', secondary_account,
					'debit_amount', COALESCE(debit_amount, 0),
					'credit_amount', COALESCE(credit_amount, 0)
				) ORDER BY secondary_account
			),
			'[]'::jsonb
		) AS secondary_accounts
		FROM secondary_agg
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
				'secondary_accounts', (SELECT secondary_accounts FROM secondary_accounts_json)
			) AS account_data
		FROM account_agg a
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
		'total_count', (SELECT total_count FROM secondary_count),
		'account_data', (SELECT account_data FROM account_combined)
	) into _result;

	 return _result;
end;
$BODY$;













/* -------------------------------------- */
--          CASH FLOW ARTICLES
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'cash_flow_article';


select reports.account_analysis_cash_flow_article(
	'budget',
	111254,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_analysis_cash_flow_article(
	_financing accounting.budget_distribution_type,
	_account integer,
	_date_from date,
	_date_to date,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN

	/* MAIN QUERY */
	WITH main AS (
		SELECT
			_account AS account,
			CASE WHEN l.debit = _account THEN l.credit ELSE l.debit END AS secondary_account,
			CASE WHEN l.debit = _account THEN round(l.amount, 4) ELSE 0 END AS debit,
			CASE WHEN l.credit = _account THEN round(l.amount, 4) ELSE 0 END AS credit,
			l.created_date::date AS created_date
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

	-- счётхои баракс (reverse/correspondent): 
	-- when main is debited secondary is credited and vice versa; 
	-- secondary can equal main for same-account transfers
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
	secondary_count AS (
		SELECT COUNT(*) AS total_count FROM secondary_agg
	),
	secondary_accounts_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'account', secondary_account,
					'debit_amount', COALESCE(debit_amount, 0),
					'credit_amount', COALESCE(credit_amount, 0)
				) ORDER BY secondary_account
			),
			'[]'::jsonb
		) AS secondary_accounts
		FROM secondary_agg
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
				'secondary_accounts', (SELECT secondary_accounts FROM secondary_accounts_json)
			) AS account_data
		FROM account_agg a
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
		'total_count', (SELECT total_count FROM secondary_count),
		'account_data', (SELECT account_data FROM account_combined)
	) into _result;

	 return _result;
end;
$BODY$;


