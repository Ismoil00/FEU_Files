





CREATE OR REPLACE FUNCTION reports.get_account_order_journal(
	_financing accounting.budget_distribution_type,
	_date_from date,
	_date_to date,
	_account bigint,
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
	
	WITH ledger_base AS (
	    SELECT
	        l.id,
	        l.debit,
	        l.credit,
	        l.amount,
	        l.created_date,
	        l.contract_id
	    FROM accounting.ledger l
	    WHERE l.financing = _financing
	      AND l.draft IS NOT TRUE
	      AND (l.debit = _account OR l.credit = _account)
	      AND l.created_date::date <= _date_to
	),
	
	ledger_with_type AS (
	    SELECT
	        lb.*,
	        o.article_id,
			w.name_id,
			w.storage_location_id,
			wn.service_nomenclature_id,
			wn.quantity as service_quantity,
			w.quantity as unit_quantity,
	        CASE
				WHEN w.storage_location_id IS NOT NULL or wn.service_nomenclature_id is not null
					THEN 'storage'
	            WHEN lb.contract_id IS NOT NULL THEN 'contract'
	            WHEN o.article_id IS NOT NULL THEN 'article'
	            ELSE 'extences'
	        END AS object_type
	    FROM ledger_base lb
	    LEFT JOIN (
	        SELECT ledger_id, cash_flow_article_id AS article_id FROM accounting.cash_payment_order
	        UNION ALL
	        SELECT ledger_id, cash_flow_article_id FROM accounting.cash_receipt_order
	        UNION ALL
	        SELECT ledger_id, cash_flow_article_id FROM accounting.payment_order_incoming
	        UNION ALL
	        SELECT ledger_id, cash_flow_article_id FROM accounting.payment_order_outgoing
	    ) o ON o.ledger_id = lb.id
		left join (
			select ledger_id, name_id, storage_location_id, quantity 
			from accounting.warehouse_incoming
			where status = 'approved'
			union all
			select ledger_id, name_id, storage_location_id, quantity
			from accounting.warehouse_outgoing
			where status = 'approved'
		) w on w.ledger_id = lb.id
		left join (
			select ledger_id, service_nomenclature_id, quantity
			from accounting.warehouse_services
			where status = 'approved'
		) wn on wn.ledger_id = lb.id
	),
		
	start_saldo as(
		SELECT
	        CASE 
	            WHEN sum(debit) - sum(credit) > 0 THEN sum(debit) - sum(credit)
	            ELSE 0
	        END AS prev_saldo_debit,
	        CASE 
	            WHEN sum(debit) - sum(credit) < 0 THEN abs(sum(debit) - sum(credit))
	            ELSE 0
	        END AS prev_saldo_credit
		FROM ledger_with_type
    	WHERE created_date < _date_from 
	),

	end_saldo as (
		select 
			CASE 
	            WHEN sum(debit) - sum(credit) > 0 THEN sum(debit) - sum(credit)
	            ELSE 0
	        END AS next_saldo_debit,
	        CASE 
	            WHEN sum(debit) - sum(credit) < 0 THEN abs(sum(debit) - sum(credit))
	            ELSE 0
	        END AS next_saldo_credit
		from ledger_with_type
    	WHERE created_date <= _date_to
	),
	
	moves AS (
	    SELECT
	        lw.id,
	        lw.object_type,
	        lw.contract_id,
	        lw.article_id,
			lw.service_quantity,
			lw.unit_quantity,
			lw.service_nomenclature_id,
			lw.storage_location_id,
			lw.name_id,
	        CASE WHEN lw.debit = _account THEN lw.credit ELSE lw.debit END AS opposite_account,
	        lw.amount,
	        lw.created_date,
	        CASE WHEN lw.debit = _account THEN 'debit' ELSE 'credit' END AS move_type
	    FROM ledger_with_type lw
	    WHERE lw.created_date::date BETWEEN _date_from AND _date_to
	),
	
	grouped AS (
	    SELECT
	        m.object_type,
	        jsonb_agg(
	            jsonb_build_object(
	                'ledger_id', m.id,
	                'account', m.opposite_account,
	                'type', m.move_type,
	                'amount', m.amount,
	                'date', m.created_date::date,
	                'contract', c.contract,
	                'counterparty', cp.name,
	                'article', a.name ->> 'tj',
					'service', sn.name ->> 'tj',
					'storage', sl.name ->> 'tj',
					'quantity', coalesce(m.unit_quantity, m.service_quantity),
					'unit_name', n.name ->> 'tj'
	            )
	            ORDER BY m.created_date
	        ) AS operations
	    FROM moves m
	    LEFT JOIN commons.counterparty_contracts c ON m.contract_id = c.id
	    LEFT JOIN accounting.counterparty cp ON c.counterparty_id = cp.id
	    LEFT JOIN commons.accouting_cash_flow_articles a ON m.article_id = a.id
		left join commons.services_nomenclature sn on m.service_nomenclature_id = sn.id
		left join commons.storage_location sl on m.storage_location_id = sl.id
		left join commons.nomenclature n on m.name_id = n.id
	    GROUP BY m.object_type
	)
	
	SELECT jsonb_build_object(
	    'account', _account,
	    'prev_debit', COALESCE(s.prev_saldo_debit,0),
	    'prev_credit', COALESCE(s.prev_saldo_credit,0),
	    'next_debit', COALESCE(e.next_saldo_debit,0),
	    'next_credit', COALESCE(e.next_saldo_credit,0),
	    'data', (
	        SELECT jsonb_agg(
	            jsonb_build_object(
	                'type', g.object_type,
	                'operations', g.operations
	            )
	        )
	        FROM grouped g
	    )
	)
	INTO _result
	FROM start_saldo s
	CROSS JOIN end_saldo e;
	
	RETURN _result;

END;
$BODY$;





