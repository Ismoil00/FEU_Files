

select * from accounting.cash_payment_order


select * from accounting.cash_receipt_order


select * from accounting.cash_payment_order


select * from accounting.counterparty


select reports.get_turnover_and_balance_sheet (
	'budget',
	'2025-01-01',
	'2025-12-31',
	100,
	0
);


{
    key: 1,
    account: 1111110,
    prev_saldo_debit: 1111,
    prev_saldo_credit: 2222,
    debit_amount: 2222,
    credit_amount: 2222,
    next_saldo_debit: 1111,
    next_saldo_credit: 2222,
  },



CREATE OR REPLACE FUNCTION reports.get_turnover_and_balance_sheet(
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
			debit as account,
			amount as debit,
			0 as credit,
			created_date
		from accounting.ledger
		where draft is not true
		and financing = _financing
	
		union all
	
		select 
			credit as account,
			0 as debit,
			amount as credit,
			created_date
		from accounting.ledger
		where draft is not true
		and financing = _financing
	),
	period_mid as (
		select 
			account,
			sum(debit) as debit_amount,
			sum(credit) as credit_amount
		from main
		where created_date >= _date_from
		and created_date <= _date_to
		group by account
	),
	period_start as (
		select 
			account,
			abs(sum(debit) - sum(credit)) as left_amount,
			case 
				when sum(debit) - sum(credit) > 0
				then 'prev_saldo_debit'
				when sum(debit) - sum(credit) < 0
				then 'prev_saldo_credit'
				else null
			end as col_name
		from main
		where created_date < _date_from
		group by account
	),
	period_end as (
		select 
			account,
			abs(sum(debit) - sum(credit)) as left_amount,
			case 
				when sum(debit) - sum(credit) > 0
				then 'next_saldo_debit'
				when sum(debit) - sum(credit) < 0
				then 'next_saldo_credit'
				else null
			end as col_name
		from main
		where created_date <= _date_to
		group by account
	),
	columns_combined as (
		select jsonb_strip_nulls(
			jsonb_build_object (
				'key', row_number() over (order by account),
				'account', coalesce(pm.account, pe.account),
				'debit_amount', pm.debit_amount,
				'credit_amount', pm.credit_amount,
				case 
					when ps.col_name is not null 
					then ps.col_name else ''
				end, 
				ps.left_amount,
				case 
					when pe.col_name is not null 
					then pe.col_name else ''
				end, 
				pe.left_amount
			)
		) as paginated_columns
		from period_mid pm
		left join period_start ps
		using (account)
		left join period_end pe
		using (account)
		order by pm.account
	),
	filtered as (
		select *
		from columns_combined
		limit _limit offset _offset
	),
	table_total_count as (
		select count(*) as total
		from columns_combined
	)
	select jsonb_build_object(
		'status', 200,
		'total', (select total from table_total_count),
		'results', jsonb_agg(f.paginated_columns)
	) into _result from filtered f;
	
	return _result;
end;
$BODY$;


















