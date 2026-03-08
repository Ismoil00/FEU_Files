

	


select reports.get_financial_reports_form_1_1 (
	12,
	2026,
	4,
	null,
	1000,
	0
);










CREATE OR REPLACE FUNCTION reports.get_financial_reports_form_1_1 (
	_department_id integer,
	_year integer,
	_month integer,
	_financing accounting.budget_distribution_type default null,
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

	/* MAIN QUERY: ledger rows for the given month/year only; 
	saldo per month; budget + special by financing */
	with main as (
		select
			debit as account,
			amount as debit_amount,
			0::numeric as credit_amount,
			created_date,
			financing
		from accounting.ledger
		where draft is not true
		and extract(year from created_date) <= _year
		and extract(month from created_date) <= _month
		and left(debit::text, 1) in ('1', '2')

		union all

		select
			credit as account,
			0::numeric as debit_amount,
			amount as credit_amount,
			created_date,
			financing
		from accounting.ledger
		where draft is not true
		and extract(year from created_date) <= _year
		and extract(month from created_date) <= _month
		and left(credit::text, 1) in ('1', '2')
	),
	-- saldo per account per financing for this month (debit - credit)
	by_financing as (
		select
			account,
			financing,
			round(sum(debit_amount) - sum(credit_amount), 4) as saldo
		from main
		group by account, financing
	),
	-- one row per account: budget_amount, special_amount; 
	-- amount = sum when _financing null else that type
	per_account as (
		select
			account,
			coalesce(sum(saldo) filter (where financing = 'budget'), 0) as budget_amount,
			coalesce(sum(saldo) filter (where financing = 'special'), 0) as special_amount
		from by_financing
		group by account
	),
	with_amount as (
		select
			account,
			make_date(_year, _month, 1)::timestamp as created_date,
			budget_amount,
			special_amount,
			case
				when _financing is null then coalesce(budget_amount, 0) + coalesce(special_amount, 0)
				when _financing = 'budget' then coalesce(budget_amount, 0)
				when _financing = 'special' then coalesce(special_amount, 0)
				else 0
			end as amount
		from per_account
	),
	total_count as (
		select count(*) as total from with_amount
	),
	paginated as (
		select account, created_date, amount, special_amount, budget_amount
		from with_amount
		order by account
		limit _limit
		offset _offset
	),
	with_category as (
		select
			account,
			created_date,
			amount,
			special_amount,
			budget_amount,
			left(account::text, 1) as first_digit
		from paginated
	),
	assets_agg as (
		select jsonb_agg(
			jsonb_build_object(
				'account', account,
				'created_date', created_date,
				'amount', abs(amount),
				'special_amount', abs(special_amount),
				'budget_amount', abs(budget_amount)
			) order by account
		) as arr
		from with_category
		where first_digit = '1'
	),
	liabilities_agg as (
		select jsonb_agg(
			jsonb_build_object(
				'account', account,
				'created_date', created_date,
				'amount', abs(amount),
				'special_amount', abs(special_amount),
				'budget_amount', abs(budget_amount)
			) order by account
		) as arr
		from with_category
		where first_digit = '2'
	)
	select jsonb_build_object(
		'total', (select total from total_count),
		'assets', coalesce((select arr from assets_agg), '[]'::jsonb),
		'liabilities', coalesce((select arr from liabilities_agg), '[]'::jsonb),
		'status', 200,
		'department', (
			select department
			from commons.department
			where id = coalesce(_department_id, 12)
		)
	) into _result;

	return _result;
end;
$BODY$;












