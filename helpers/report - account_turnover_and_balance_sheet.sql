


drop function reports.get_account_turnover_and_balance_sheet



CREATE INDEX idx_ledger_budget_debit_active
ON accounting.ledger (debit, created_date)
WHERE draft IS NOT TRUE AND financing = 'budget';

CREATE INDEX idx_ledger_budget_credit_active
ON accounting.ledger (credit, created_date)
WHERE draft IS NOT TRUE AND financing = 'budget';

CREATE INDEX idx_ledger_contract_active
ON accounting.ledger (contract_id)
WHERE contract_id IS NOT NULL;



CREATE OR REPLACE FUNCTION reports.get_account_turnover_and_balance_sheet(
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
	_related_selector varchar(100);
BEGIN

	/* WE DEFINE WHAT TYPE IS THE ACCOUNT */
	SELECT related_selector
	into _related_selector
	FROM accounting.accounts
	where account = _account;

	 if _related_selector = 'cash_flow_article' then
		return reports.account_turnover_and_balance_sheet_cash_flow_article (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);

	 elsif _related_selector = 'counterparty_contract' then
		return reports.account_turnover_and_balance_sheet_counterparty_contract (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'staff' then
		return reports.account_turnover_and_balance_sheet_staff (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'products' then
	 	return reports.account_turnover_and_balance_sheet_products (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'estimates' then
	 	return reports.account_turnover_and_balance_sheet_estimates (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 	
	 else
	 
	 end if;
end;
$BODY$;














/* ESTIMATES */

select * from accounting.ledger l
where staff_id is not null

select reports.account_turnover_and_balance_sheet_estimates (
	'budget',
	131230,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);

select * from accounting.manual_operations

select * from accounting.payment_order_outgoing

select * from accounting.cash_payment_order

select * from accounting.estimates



CREATE OR REPLACE FUNCTION reports.account_turnover_and_balance_sheet_estimates (
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
	with main as (
	 	select
			case when _account = l.debit then l.debit else l.credit end as account,
			case when _account = l.debit then l.amount else 0 end as debit,
			case when _account = l.credit then l.amount else 0 end as credit,
			coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id) as name_id,
			n.name,
			l.created_date::date
		from accounting.ledger l
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
		where l.draft is not true
		and l.financing = _financing
		and (l.debit = _account or l.credit = _account)
		and coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id) is not null
	),
	-- account
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
	),
	account_combined as (
	    select
	        account,
	        jsonb_build_object(
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
	        ) as account_data
	    from account_agg
	),
	-- content
	content_agg as (
	    select
	        account,
	        name_id,
	        name,

			-- main period
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
	    group by account, name_id, name
	),
	content_count as (
	    select count(*) as total_count
	    from content_agg
	),
	content_paginated as (
	    select *
	    from content_agg
	    order by name->>'ru'
	    limit _limit
	    offset _offset
	),
	content_combined as (
	    select
	        jsonb_agg(
	            jsonb_build_object(
	                'id', name_id,
	                'name', name,
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
	        ) as content_data
	    from content_paginated
	)
	-- final packing
	select jsonb_build_object(
		'status', 200,
		'type', 'products',
		'total_count', (select total_count from content_count),
		'content_data', (
			select content_data from content_combined
		),
		'account_data', (
			select account_data from account_combined
		)
	) into _result;

	 return _result;
end;
$BODY$;




















/* PRODUCTS */

select * from accounting.ledger l
where staff_id is not null

select reports.account_turnover_and_balance_sheet_products (
	'budget',
	131230,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);

select * from accounting.warehouse_incoming;

select * from accounting.warehouse_outgoing;

select * from accounting.product_transfer;

select * from accounting.advance_report_tmzos;

select * from commons.nomenclature



CREATE OR REPLACE FUNCTION reports.account_turnover_and_balance_sheet_products (
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
	with main as (
	 	select
			case when _account = l.debit then l.debit else l.credit end as account,
			case when _account = l.debit then l.amount else 0 end as debit,
			case when _account = l.credit then l.amount else 0 end as credit,
			coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id) as name_id,
			n.name,
			l.created_date::date
		from accounting.ledger l
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
		where l.draft is not true
		and l.financing = _financing
		and (l.debit = _account or l.credit = _account)
		and coalesce(wi.name_id, wo.name_id, pt.name_id, art.name_id) is not null
	),
	-- account
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
	),
	account_combined as (
	    select
	        account,
	        jsonb_build_object(
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
	        ) as account_data
	    from account_agg
	),
	-- content
	content_agg as (
	    select
	        account,
	        name_id,
	        name,

			-- main period
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
	    group by account, name_id, name
	),
	content_count as (
	    select count(*) as total_count
	    from content_agg
	),
	content_paginated as (
	    select *
	    from content_agg
	    order by name->>'ru'
	    limit _limit
	    offset _offset
	),
	content_combined as (
	    select
	        jsonb_agg(
	            jsonb_build_object(
	                'id', name_id,
	                'name', name,
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
	        ) as content_data
	    from content_paginated
	)
	-- final packing
	select jsonb_build_object(
		'status', 200,
		'type', 'products',
		'total_count', (select total_count from content_count),
		'content_data', (
			select content_data from content_combined
		),
		'account_data', (
			select account_data from account_combined
		)
	) into _result;

	 return _result;
end;
$BODY$;














/* STAFF */

select * from accounting.ledger l
where staff_id is not null


select reports.account_turnover_and_balance_sheet_staff (
	'budget',
	211510,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);



CREATE OR REPLACE FUNCTION reports.account_turnover_and_balance_sheet_staff (
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
	with main as (
	 	select
			case when 211510 = l.debit then l.debit else l.credit end as account,
			case when 211510 = l.debit then l.amount else 0 end as debit,
			case when 211510 = l.credit then l.amount else 0 end as credit,
			l.staff_id,
			concat_ws(' ', s.lastname, s.firstname, s.middlename) as fullname,
			l.created_date::date
		from accounting.ledger l
		join hr.staff s
			on l.staff_id = s.id
		where l.draft is not true
		and l.financing = _financing
		and (l.debit = _account or l.credit = _account)
		and staff_id is not null
	),
	-- account
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
	),
	account_combined as (
	    select
	        account,
	        jsonb_build_object(
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
	        ) as account_data
	    from account_agg
	),
	-- content
	content_agg as (
	    select
	        account,
	        staff_id,
	        fullname,

			-- main period
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
	    group by account, staff_id, fullname
	),
	content_count as (
	    select count(*) as total_count
	    from content_agg
	),
	content_paginated as (
	    select *
	    from content_agg
	    order by fullname
	    limit _limit
	    offset _offset
	),
	content_combined as (
	    select
	        jsonb_agg(
	            jsonb_build_object(
	                'id', staff_id,
	                'fullname', fullname,
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
	        ) as content_data
	    from content_paginated
	)
	-- final packing
	select jsonb_build_object(
		'status', 200,
		'type', 'staff',
		'total_count', (select total_count from content_count),
		'content_data', (
			select content_data from content_combined
		),
		'account_data', (
			select account_data from account_combined
		)
	) into _result;

	 return _result;
end;
$BODY$;























/* COUNTERPARTT -> CONTRACT */
select * from commons.counterparty_contracts


CREATE OR REPLACE FUNCTION reports.account_turnover_and_balance_sheet_counterparty_contract(
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
	with main as (
	 	select
			case when _account = l.debit then l.debit else l.credit end as account,
			case when _account = l.debit then l.amount else 0 end as debit,
			case when _account = l.credit then l.amount else 0 end as credit,
		 	cc.counterparty_id,
		 	cc.counterparty_name,
			l.contract_id,
			cc.contract_name,
			l.created_date::date
		from accounting.ledger l
		join (
			select
				cc.id,
				cc.contract as contract_name,
				cc.counterparty_id,
				c.name as counterparty_name
			from commons.counterparty_contracts cc
			join accounting.counterparty c
				on cc.counterparty_id = c.id
		) cc on l.contract_id = cc.id
		where l.draft is not true
		and l.financing = _financing
		and (l.debit = _account or l.credit = _account)
		and l.contract_id is not null
	),
	-- account
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
	),
	account_combined as (
	    select
	        account,
	        jsonb_build_object(
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
	        ) as account_data
	    from account_agg
	),
	-- countarparty -> contract
	contract_agg as (
	    select
	        account,
	        counterparty_id,
	        counterparty_name,
	        contract_id,
	        contract_name,
	
	        -- period
	        sum(debit) filter (
	            where created_date between _date_from and _date_to
	        ) as debit_amount,
	
	        sum(credit) filter (
	            where created_date between _date_from and _date_to
	        ) as credit_amount,
	
	        -- start balance
	        sum(debit) filter (
	            where created_date < _date_from
	        ) -
	        sum(credit) filter (
	            where created_date < _date_from
	        ) as start_balance,
	
	        -- end balance
	        sum(debit) filter (
	            where created_date <= _date_to
	        ) -
	        sum(credit) filter (
	            where created_date <= _date_to
	        ) as end_balance
	
	    from main
	    group by
	        account,
	        counterparty_id,
	        counterparty_name,
	        contract_id,
	        contract_name
	),
	counterparty_totals as (
	    select
	        account,
	        counterparty_id,
	        counterparty_name,
	
	        -- period totals
	        sum(debit_amount)  as debit_amount,
	        sum(credit_amount) as credit_amount,
	
	        -- balances
	        sum(start_balance) as start_balance,
	        sum(end_balance)   as end_balance
	
	    from contract_agg
	    group by
	        account,
	        counterparty_id,
	        counterparty_name
	),
	counterparty_count as (
	    select count(*) as total_count
	    from counterparty_totals
	),
	counterparty_paginated as (
	    select *
	    from counterparty_totals
	    order by counterparty_name
	    limit _limit
	    offset _offset
	),
	contracts_grouped as (
	    select
	        account,
	        counterparty_id,
	        counterparty_name,
	
	        jsonb_agg(
	            jsonb_build_object(
	                'contract_id', contract_id,
	                'contract_name', contract_name,
	                'debit_amount', coalesce(debit_amount, 0),
	                'credit_amount', coalesce(credit_amount, 0),
	
	                case when start_balance > 0
	                    then 'prev_saldo_debit'
	                    else 'prev_saldo_credit'
	                end,
	                abs(coalesce(start_balance,0)),
	
	                case when end_balance > 0
	                    then 'next_saldo_debit'
	                    else 'next_saldo_credit'
	                end,
	                abs(coalesce(end_balance, 0))
	            )
	            order by contract_name
	        ) as contracts
	
	    from contract_agg
	    group by
	        account,
	        counterparty_id,
	        counterparty_name
	),
	counterparty_grouped as (
	    select
	        cp.account,
	
	        jsonb_agg(
	            jsonb_build_object(
	                'counterparty_id', cp.counterparty_id,
	                'counterparty_name', cp.counterparty_name,
	
	                -- COUNTERPARTY TOTALS
	                'debit_amount', coalesce(cp.debit_amount, 0),
	                'credit_amount', coalesce(cp.credit_amount, 0),
	
	                case when cp.start_balance > 0
	                    then 'prev_saldo_debit'
	                    else 'prev_saldo_credit'
	                end,
	                abs(coalesce(cp.start_balance,0)),
	
	                case when cp.end_balance > 0
	                    then 'next_saldo_debit'
	                    else 'next_saldo_credit'
	                end,
	                abs(coalesce(cp.end_balance, 0)),
	
	                -- CONTRACTS
	                'contracts', cg.contracts
	            )
	            order by cp.counterparty_name
	        ) as counterparties
	
	    from counterparty_paginated cp
	    join contracts_grouped cg
	      on cg.account = cp.account
	     and cg.counterparty_id = cp.counterparty_id
	
	    group by cp.account
	)
	-- final packing
	select jsonb_build_object(
		'status', 200,
		'type', 'counterparty_contract',
		'total_count', (select total_count from counterparty_count),
		'content_data', (
			select counterparties from counterparty_grouped
		),
		'account_data', (
			select account_data from account_combined
		)
	);

	 return _result;
end;
$BODY$;

















/* CASH FLOW ARTICLE */

select * from accounting.payment_order_incoming
select * from accounting.payment_order_outgoing
select * from accounting.cash_payment_order
select * from accounting.cash_receipt_order

		
CREATE OR REPLACE FUNCTION reports.account_turnover_and_balance_sheet_cash_flow_article(
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
	with main as (
	 	select
			case when _account = l.debit then l.debit else l.credit end as account,
			case when _account = l.debit then l.amount else 0 end as debit,
			case when _account = l.credit then l.amount else 0 end as credit,
			coalesce(
				por.cash_flow_article_id,
				pou.cash_flow_article_id,
				cpo.cash_flow_article_id,
				cro.cash_flow_article_id
			)  as cash_flow_article_id,
			cfa.name,
			l.created_date::date
		from accounting.ledger l
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
		where l.draft is not true
		and l.financing = _financing
		and (l.debit = _account or l.credit = _account)
		and coalesce(
			por.cash_flow_article_id,
			pou.cash_flow_article_id,
			cpo.cash_flow_article_id,
			cro.cash_flow_article_id
		) is not null
	),
	-- account
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
	),
	account_combined as (
	    select
	        account,
	        jsonb_build_object(
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
	        ) as account_data
	    from account_agg
	),
	-- content
	content_agg as (
	    select
	        account,
	        cash_flow_article_id,
	        name,

			-- main period
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
	    group by account, cash_flow_article_id, name
	),
	content_count as (
	    select count(*) as total_count
	    from content_agg
	),
	content_paginated as (
	    select *
	    from content_agg
	    order by name->>'ru'
	    limit _limit
	    offset _offset
	),
	content_combined as (
	    select
	        jsonb_agg(
	            jsonb_build_object(
	                'id', cash_flow_article_id,
	                'name', name,
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
	        ) as content_data
	    from content_paginated
	)
	-- final packing
	select jsonb_build_object(
		'status', 200,
		'type', 'cash_flow_article',
		'total_count', (select total_count from content_count),
		'content_data', (
			select content_data from content_combined
		),
		'account_data', (
			select account_data from account_combined
		)
	) into _result;

	 return _result;
end;
$BODY$;








