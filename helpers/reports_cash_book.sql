

select * from accounting.cash_payment_order


select * from accounting.cash_receipt_order


select * from accounting.cash_payment_order


select * from accounting.counterparty



select reports.get_cash_book (
	'budget',
	'2025-01-01',
	'2025-12-31',
	100,
	0
);

CREATE OR REPLACE FUNCTION reports.get_cash_book(
	_financing accounting.budget_distribution_type,
	start_date text,
	end_date text,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_date_from date = start_date::date;
    _date_to date = end_date::date;
	_result jsonb;
BEGIN

	with main as (
		select 
			id,
			(created->>'date')::date as created_date,
			true as cash_receipt_order,
			credit as account,
			amount as debit_amount,
			null as credit_amount,
			staff_id,
			counterparty_id,
			received_from as base
		from accounting.cash_receipt_order
		where financing = _financing 
		and (created->>'date')::date >= _date_from
		and (created->>'date')::date <= _date_to

		union all 

		select 
			id,
			(created->>'date')::date as created_date,
			false as cash_receipt_order,
			debit as account,
			null as debit_amount,
			amount as credit_amount,
			staff_id,
			counterparty_id,
			given_to as base
		from accounting.cash_payment_order
		where financing = _financing 
		and (created->>'date')::date >= _date_from
		and (created->>'date')::date <= _date_to
	),
	main_filtered_and_ordered as (
		select jsonb_build_object(
			'key', row_number() over(order by created_date desc),
			'id', m.id,
			'document_number', m.id || ', ' || 'аз ' || m.created_date,
			'name', case 
					when m.base = 'staff' then concat_ws(' ', s.lastname, s.firstname, s.middlename)
					when m.base = 'counterparty' then c.name
					else null
				end,
			'cash_receipt_order', m.cash_receipt_order,
			'account', m.account,
			'debit_amount', m.debit_amount,
			'credit_amount', m.credit_amount
		) as paginated_rows
		from main m
		left join hr.staff s 
			on m.staff_id = s.id
		left join accounting.counterparty c 
			on m.counterparty_id = c.id
		order by created_date desc
		limit _limit offset _offset
	),
	main_paginated as (
		select jsonb_agg(paginated_rows)
		as aggregated_rows
		from main_filtered_and_ordered
	),
	table_total_count as (
		select count(*) as total from main
	),
	period_start as (
		select
			case 
				when (incomings.total_amount - expenses.total_amount) > 0
				then 'period_start_debit'
				when (incomings.total_amount - expenses.total_amount) < 0
				then 'period_start_credit'
				else null
			end col_name,
			abs(incomings.total_amount - expenses.total_amount) left_amount
		from (
			select sum(amount) total_amount
			from accounting.cash_receipt_order
			where (created->>'date')::date < _date_from
		) incomings cross join (
			select sum(amount) total_amount
			from accounting.cash_payment_order
			where (created->>'date')::date < _date_from
		) expenses
	),
	period_end as (
		select
			case 
				when (incomings.total_amount - expenses.total_amount) > 0
				then 'period_end_debit'
				when (incomings.total_amount - expenses.total_amount) < 0
				then 'period_end_credit'
				else null
			end col_name,
			abs(incomings.total_amount - expenses.total_amount) left_amount
		from (
			select sum(amount) total_amount
			from accounting.cash_receipt_order
			where (created->>'date')::date < _date_to
		) incomings cross join (
			select sum(amount) total_amount
			from accounting.cash_payment_order
			where (created->>'date')::date < _date_to
		) expenses
	),
	totals as (
		select 
			sum(debit_amount) total_debit_amount, 
			sum(credit_amount) total_credit_amount
		from main
	) select jsonb_build_object(
		'total_debit_amount', t.total_debit_amount,
		'total_credit_amount', t.total_credit_amount,
		ps.col_name, ps.left_amount,
		pe.col_name, pe.left_amount,
		'table_data', mp.aggregated_rows,
		'total', (select total from table_total_count),
		'status', 200
	) into _result from totals t, period_start ps, 
		period_end pe, main_paginated mp;
	
	return _result;
end;
$BODY$;





















/* TEST RECORDS FOR THE OUTGOING ORDERS */
INSERT INTO accounting.cash_payment_order (
    cash_flow_article_id,
    amount,
    credit,
    debit,
    advance_debit,
    description,
    based_on,
    staff_id,
    staff_id_document,
    created,
    given_to,
    financing
)
SELECT
    1,
    round((random() * 10000 + 100)::numeric, 2),
    111110,

    accs.debit,
    accs.advance_debit,

    'Auto-generated record',
    'Generated for testing',

    staff.staff_id,
    'PASSPORT-' || gs,

    jsonb_build_object(
        'date',
        DATE '2023-01-01'
        + (random() * (DATE '2025-12-31' - DATE '2023-01-01'))::int,
        'user_id', 'b325f38d-f247-462e-adf1-40bce7806302'
    ),

    'counterparty',
    'budget'

FROM generate_series(1, 1000) gs

/* ---- RANDOM ACCOUNTS (FORCED PER ROW) ---- */
CROSS JOIN LATERAL (
    SELECT
        accs[1] AS debit,
        accs[2] AS advance_debit
    FROM (
        SELECT array_agg(a ORDER BY md5(gs::text || a::text)) AS accs
        FROM unnest(ARRAY[
            110000,111000,111100,111110,111120,111130,111140,111141,111142,
            111143,111200,111210,111220,111230,111231,111239,111240,111241,
            111249,111250,111251,111252,111253,111254,111260,111270,111300,
            111310,111320,111330,111340,111350,111351,111352,111353,111354,
            111360,111400,111410,111420,111421,111430,111440,111441,111442,
            111443,111444,111450,111490,111491
        ]) a
    ) s
) accs

/* ---- RANDOM STAFF (FORCED PER ROW) ---- */
CROSS JOIN LATERAL (
    SELECT a AS staff_id
    FROM unnest(ARRAY[
        1,8,9,10,11,12,13,
        14,16,17,18,19,
        20,21,22,23,24,25,26,27
    ]) a
    ORDER BY md5(gs::text || a::text)
    LIMIT 1
) staff;






/* TEST RECORDS FOR THE INCOMING ORDERS */
INSERT INTO accounting.cash_receipt_order (
    cash_flow_article_id,
    amount,
	
    debit,
    credit,
    advance_credit,
	
    description,
    based_on,
    staff_id,
    staff_id_document,
    created,
    received_from,
    financing
)
SELECT
    1,
    round((random() * 10000 + 100)::numeric, 2),

    111110,
    accs.credit,
    accs.advance_credit,

    'Auto-generated record',
    'Generated for testing',

    staff.staff_id,
    'PASSPORT-' || gs,

    jsonb_build_object(
        'date',
        DATE '2023-01-01'
        + (random() * (DATE '2025-12-31' - DATE '2023-01-01'))::int,
        'user_id', 'b325f38d-f247-462e-adf1-40bce7806302'
    ),

    'staff',
    'budget'

FROM generate_series(1, 1000) gs

/* ---- RANDOM ACCOUNTS (FORCED PER ROW) ---- */
CROSS JOIN LATERAL (
    SELECT
        accs[1] AS credit,
        accs[2] AS advance_credit
    FROM (
        SELECT array_agg(a ORDER BY md5(gs::text || a::text)) AS accs
        FROM unnest(ARRAY[
            110000,111000,111100,111110,111120,111130,111140,111141,111142,
            111143,111200,111210,111220,111230,111231,111239,111240,111241,
            111249,111250,111251,111252,111253,111254,111260,111270,111300,
            111310,111320,111330,111340,111350,111351,111352,111353,111354,
            111360,111400,111410,111420,111421,111430,111440,111441,111442,
            111443,111444,111450,111490,111491
        ]) a
    ) s
) accs

/* ---- RANDOM STAFF (FORCED PER ROW) ---- */
CROSS JOIN LATERAL (
    SELECT a AS staff_id
    FROM unnest(ARRAY[
        1,8,9,10,11,12,13,
        14,16,17,18,19,
        20,21,22,23,24,25,26,27
    ]) a
    ORDER BY md5(gs::text || a::text)
    LIMIT 1
) staff;

