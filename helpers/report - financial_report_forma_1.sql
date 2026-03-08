





CREATE OR REPLACE FUNCTION reports.get_financial_reports_form_1 (
	_department_id integer,
	_financing accounting.budget_distribution_type,
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
	with main as (
		select
			debit as account,
			amount as debit_amount,
			0 as credit_amount,
			created_date
		from accounting.ledger
		where draft is not true
		and financing = _financing
		and left(debit::text, 1) in ('1', '2', '3')

		union all

		select
			credit as account,
			0 as debit_amount,
			amount as credit_amount,
			created_date
		from accounting.ledger
		where draft is not true
		and financing = _financing
		and left(credit::text, 1) in ('1', '2', '3')
	),
	groupped as (
		select
			account,
			-- start balance
			round(
				sum(debit_amount) filter (where created_date < _date_from)
					- sum(credit_amount) filter (where created_date < _date_from),
				4
			) as start_balance,
			-- end balance
			round(
				sum(debit_amount) filter (where created_date <= _date_to)
					- sum(credit_amount) filter (where created_date <= _date_to),
				4
			) as end_balance
		from main
		group by account
	),
	total_count as (
		select count(*) as total from groupped
	),
	paginated as (
		select
			account,
			start_balance,
			end_balance
		from groupped
		order by account
		limit _limit
		offset _offset
	),
	-- classify by first digit: 1=assets, 2=liabilities, 3=equities, 4=incomes, 5=expenses
	with_category as (
		select
			account,
			start_balance,
			end_balance,
			left(account::text, 1) as first_digit
		from paginated
	),
	assets_agg as (
		select jsonb_agg(
			jsonb_build_object(
				'account', account,
				'prev_saldo_debit', (coalesce(start_balance, 0) >= 0),
				'prev_saldo_amount', abs(coalesce(start_balance, 0)),
				'next_saldo_debit', (coalesce(end_balance, 0) >= 0),
				'next_saldo_amount', abs(coalesce(end_balance, 0))
			) order by account
		) as arr
		from with_category
		where first_digit = '1'
	),
	liabilities_agg as (
		select jsonb_agg(
			jsonb_build_object(
				'account', account,
				'prev_saldo_debit', (coalesce(start_balance, 0) >= 0),
				'prev_saldo_amount', abs(coalesce(start_balance, 0)),
				'next_saldo_debit', (coalesce(end_balance, 0) >= 0),
				'next_saldo_amount', abs(coalesce(end_balance, 0))
			) order by account
		) as arr
		from with_category
		where first_digit = '2'
	),
	equities_agg as (
		select jsonb_agg(
			jsonb_build_object(
				'account', account,
				'prev_saldo_debit', (coalesce(start_balance, 0) >= 0),
				'prev_saldo_amount', abs(coalesce(start_balance, 0)),
				'next_saldo_debit', (coalesce(end_balance, 0) >= 0),
				'next_saldo_amount', abs(coalesce(end_balance, 0))
			) order by account
		) as arr
		from with_category
		where first_digit = '3'
	)
	select jsonb_build_object(
		'status', 200,
		'total', (select total from total_count),
		'department', (
			select department
			from commons.department
			where id = coalesce(_department_id, 12)
		),
		'assets', coalesce((select arr from assets_agg), '[]'::jsonb),
		'liabilities', coalesce((select arr from liabilities_agg), '[]'::jsonb),
		'equities', coalesce((select arr from equities_agg), '[]'::jsonb)
	) into _result;

	 return _result;
end;
$BODY$;









