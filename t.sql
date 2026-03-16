CREATE OR REPLACE FUNCTION reports.get_financial_reports_form_1_3(
	_department_id integer,
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
	-- account groups for form 1.3
	assets_accounts int[] := array[141110, 141120, 141210, 141220, 141310, 141320, 141400];
	long_term_accounts int[] := array[141500, 144000, 146000, 147000, 149000, 151500];
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
		  AND (l.debit = ANY(assets_accounts) OR l.debit = ANY(long_term_accounts))
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
		  AND (l.credit = ANY(assets_accounts) OR l.credit = ANY(long_term_accounts))
	),
	ledger_agg AS (
		SELECT
			account,
			-- prev_saldo: balance before _date_from (debit - credit)
			SUM(debit_amt) FILTER (WHERE created_date < _date_from)
				- SUM(credit_amt) FILTER (WHERE created_date < _date_from) AS prev_saldo,
			-- next_saldo: balance through _date_to
			SUM(debit_amt) FILTER (WHERE created_date <= _date_to)
				- SUM(credit_amt) FILTER (WHERE created_date <= _date_to) AS next_saldo,
			-- period debit/credit
			COALESCE(SUM(debit_amt) FILTER (WHERE created_date BETWEEN _date_from AND _date_to), 0) AS debit_amount,
			COALESCE(SUM(credit_amt) FILTER (WHERE created_date BETWEEN _date_from AND _date_to), 0) AS credit_amount,
			financing
		FROM ledger_unpivot
		GROUP BY account, financing
	),
	-- per-account totals: total = all financing, special = only financing = 'special'
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
	-- depreciation: total depreciated amount from created_date till _date_to per asset row, then sum by account and financing
	dep_per_row AS (
		SELECT
			ar.debit AS account,
			ar.financing,
			(ar.quantity * COALESCE(ar.unit_price, 0))
				* (COALESCE(ar.depreciation_percent, 0) / 100) / 12
				* GREATEST(FLOOR((_date_to - (ar.created->>'date')::date)::numeric / 30.44), 0) AS total_depreciation
		FROM accounting.assets_recognition ar
		LEFT JOIN commons.department d ON ar.main_department_id = d.id
		WHERE ar.depreciation IS TRUE
		  AND (ar.exported IS NOT TRUE OR ar.exported IS NULL)
		  AND (ar.main_department_id = _department_id OR d.parent_id = _department_id)
		  AND (ar.debit = ANY(assets_accounts) OR ar.debit = ANY(long_term_accounts))
		  AND (ar.created->>'date')::date <= _date_to
	),
	dep_agg AS (
		SELECT
			account,
			SUM(total_depreciation) AS depreciation_total_amount,
			SUM(total_depreciation) FILTER (WHERE financing = 'special') AS depreciation_special_amount
		FROM dep_per_row
		GROUP BY account
	),
	-- build one row per account with all fields (ledger + depreciation, revaluation 0)
	assets_data AS (
		SELECT
			ac.account,
			COALESCE(l.prev_saldo_total, 0) AS prev_saldo_total_amount,
			COALESCE(l.prev_saldo_special, 0) AS prev_saldo_special_amount,
			COALESCE(l.debit_total_amount, 0) AS debit_total_amount,
			COALESCE(l.debit_special_amount, 0) AS debit_special_amount,
			COALESCE(l.credit_total_amount, 0) AS credit_total_amount,
			COALESCE(l.credit_special_amount, 0) AS credit_special_amount,
			COALESCE(d.depreciation_total_amount, 0) AS depreciation_total_amount,
			COALESCE(d.depreciation_special_amount, 0) AS depreciation_special_amount,
			0::numeric AS revaluation_total_amount,
			0::numeric AS revaluation_special_amount,
			COALESCE(l.next_saldo_total, 0) AS next_saldo_total_amount,
			COALESCE(l.next_saldo_special, 0) AS next_saldo_special_amount
		FROM (SELECT unnest(assets_accounts) AS account) ac
		LEFT JOIN ledger_by_account l ON l.account = ac.account
		LEFT JOIN dep_agg d ON d.account = ac.account
	),
	long_term_data AS (
		SELECT
			ac.account,
			COALESCE(l.prev_saldo_total, 0) AS prev_saldo_total_amount,
			COALESCE(l.prev_saldo_special, 0) AS prev_saldo_special_amount,
			COALESCE(l.debit_total_amount, 0) AS debit_total_amount,
			COALESCE(l.debit_special_amount, 0) AS debit_special_amount,
			COALESCE(l.credit_total_amount, 0) AS credit_total_amount,
			COALESCE(l.credit_special_amount, 0) AS credit_special_amount,
			COALESCE(d.depreciation_total_amount, 0) AS depreciation_total_amount,
			COALESCE(d.depreciation_special_amount, 0) AS depreciation_special_amount,
			COALESCE(l.next_saldo_total, 0) AS next_saldo_total_amount,
			COALESCE(l.next_saldo_special, 0) AS next_saldo_special_amount
		FROM (SELECT unnest(long_term_accounts) AS account) ac
		LEFT JOIN ledger_by_account l ON l.account = ac.account
		LEFT JOIN dep_agg d ON d.account = ac.account
	),
	assets_rows AS (
		SELECT jsonb_build_object(
			'account', account::text,
			'prev_saldo_total_amount', prev_saldo_total_amount,
			'prev_saldo_special_amount', prev_saldo_special_amount,
			'debit_total_amount', debit_total_amount,
			'debit_special_amount', debit_special_amount,
			'credit_total_amount', credit_total_amount,
			'credit_special_amount', credit_special_amount,
			'depreciation_total_amount', depreciation_total_amount,
			'depreciation_special_amount', depreciation_special_amount,
			'revaluation_total_amount', 0::numeric,
			'revaluation_special_amount', 0::numeric,
			'next_saldo_total_amount', next_saldo_total_amount,
			'next_saldo_special_amount', next_saldo_special_amount
		) AS obj
		FROM assets_data
		ORDER BY account
	),
	long_term_rows AS (
		SELECT jsonb_build_object(
			'account', account::text,
			'prev_saldo_total_amount', prev_saldo_total_amount,
			'prev_saldo_special_amount', prev_saldo_special_amount,
			'debit_total_amount', debit_total_amount,
			'debit_special_amount', debit_special_amount,
			'credit_total_amount', credit_total_amount,
			'credit_special_amount', credit_special_amount,
			'depreciation_total_amount', depreciation_total_amount,
			'depreciation_special_amount', depreciation_special_amount,
			'revaluation_total_amount', 0::numeric,
			'revaluation_special_amount', 0::numeric,
			'next_saldo_total_amount', next_saldo_total_amount,
			'next_saldo_special_amount', next_saldo_special_amount
		) AS obj
		FROM long_term_data
		ORDER BY account
	),
	others_obj AS (
		SELECT jsonb_build_object(
			'others', true,
			'prev_saldo_total_amount', 0,
			'prev_saldo_special_amount', 0,
			'debit_total_amount', 0,
			'debit_special_amount', 0,
			'credit_total_amount', 0,
			'credit_special_amount', 0,
			'depreciation_total_amount', 0,
			'depreciation_special_amount', 0,
			'revaluation_total_amount', 0,
			'revaluation_special_amount', 0,
			'next_saldo_total_amount', 0,
			'next_saldo_special_amount', 0
		) AS obj
	)
	SELECT jsonb_build_object(
		'status', 200,
		'assets', COALESCE((SELECT jsonb_agg(obj ORDER BY (obj->>'account')) FROM assets_rows), '[]'::jsonb) || (SELECT obj FROM others_obj LIMIT 1),
		'long_term_assets', COALESCE((SELECT jsonb_agg(obj ORDER BY (obj->>'account')) FROM long_term_rows), '[]'::jsonb) || (SELECT obj FROM others_obj LIMIT 1)
	) INTO _result;

	RETURN _result;
END;
$BODY$;