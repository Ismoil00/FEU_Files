CREATE OR REPLACE FUNCTION reports.account_turnover_and_balance_sheet_not_content(
	_financing accounting.budget_distribution_type,
	_department_id integer,
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
		with main as (
		 	select
				case when _account = l.debit then l.debit else l.credit end as account,
				case when _account = l.debit then l.amount else 0 end as debit,
				case when _account = l.credit then l.amount else 0 end as credit,
				l.created_date::date
			from accounting.ledger l
			left join commons.department d
				on l.main_department_id = d.id
			where l.draft is not true
			AND (l.main_department_id = _department_id or d.parent_id = _department_id)
			and l.financing = _financing
			and (l.debit = _account or l.credit = _account)
		),
		account_agg as (
			select 
				account,
	
				-- mid period
				sum(debit) filter (
					where created_date between _date_from and _date_to
				) as debit_amount,
				sum(credit) filter (
					where created_date between _date_from and _date_to
				) as credit_amount,
	
				-- start saldo
			    sum(debit) filter (
			        where created_date < _date_from
			    ) -
			    sum(credit) filter (
			        where created_date < _date_from
			    ) as start_balance,
	
				-- end saldo
		        sum(debit) filter (
		            where created_date <= _date_to
		        ) -
		        sum(credit) filter (
		            where created_date <= _date_to
		        ) as end_balance
			from main
			group by account
		)
		-- final packing
		select jsonb_build_object(
			'status', 200,
			'type', 'account',
			'department', (
				select department 
				from commons.department
				where id = _department_id
			),
			'account_data', jsonb_build_object(
		        'account', account,
		        'debit_amount', coalesce(debit_amount, 0),
		        'credit_amount', coalesce(credit_amount, 0),
		        case when start_balance > 0
		            then 'prev_saldo_debit'
		            else 'prev_saldo_credit'
		        end,
		        abs(coalesce(start_balance, 0)),
		        case when end_balance > 0
		            then 'next_saldo_debit'
		            else 'next_saldo_credit'
		        end,
		        abs(coalesce(end_balance, 0))
		    )
		) into _result from account_agg;

	 return _result;
end;
$BODY$;