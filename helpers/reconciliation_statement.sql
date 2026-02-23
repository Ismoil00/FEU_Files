





select department from 
commons.department
where id = 12





create or replace function accounting.get_reconciliation_statement (
	_counterparty_id bigint,
	_financing accounting.budget_distribution_type default null,
	_contract_id bigint default null,
	_limit int default 100,
	_offset int default 0
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result jsonb;
begin

	/* QUERY */
	with all_operations as (
		select
			l.created_date::date as created_date,
			l.amount as main_debit,
			0 as main_credit,
			0 as counter_debit,
			l.amount as counter_credit,
			jsonb_build_object(
				'ru', 'Операции, введенные вручную № ' || mo.id,
				'tj', 'Амалиётҳои дастӣ воридшуда № ' || mo.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Поступление денежных средств, товаров или услуг',
				'tj', 'Воридшавии маблағҳои пулӣ, молҳо ё хизматрасониҳо'
			) as counter_document
		from accounting.manual_operations mo
		join accounting.ledger l
			on mo.ledger_id = l.id
		where _counterparty_id = mo.counterparty_id
		and (_financing is null or mo.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	
		union all
	
		select
			l.created_date::date as created_date,
			0 as main_debit,
			l.amount as main_credit,
			l.amount as counter_debit,
			0 as counter_credit,
			jsonb_build_object(
				'ru', 'Платежное поручение входящее № ' || poi.id,
				'tj', 'Фармони пардохти воридотӣ № ' || poi.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Оплата денежных средств',
				'tj', 'Пардохти маблағҳои пулӣ'
			) as counter_document
		from accounting.payment_order_incoming poi
		join accounting.ledger l
			on poi.ledger_id = l.id
		where _counterparty_id = poi.counterparty_id
		and (_financing is null or poi.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	
		union all
	
		select
			l.created_date::date as created_date,
			l.amount as main_debit,
			0 as main_credit,
			0 as counter_debit,
			l.amount as counter_credit,
			jsonb_build_object(
				'ru', 'Платежное поручение исходящее № ' || poo.id,
				'tj', 'Фармони пардохти содиротӣ № ' || poo.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Поступление денежных средств',
				'tj', 'Воридоти маблағҳои пулӣ'
			) as counter_document
		from accounting.payment_order_outgoing poo
		join accounting.ledger l
			on poo.ledger_id = l.id
		where _counterparty_id = poo.counterparty_id
		and (_financing is null or poo.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)

		union all
		
		select
			l.created_date::date as created_date,
			0 as main_debit,
			l.amount as main_credit,
			l.amount as counter_debit,
			0 as counter_credit,
			jsonb_build_object(
				'ru', 'Приходный кассовый ордер № ' || cro.id,
				'tj', 'ОРДЕРИ ВОРИДОТИ ХАЗИНАВӢ № ' || cro.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Оплата денежных средств',
				'tj', 'Пардохти маблағҳои пулӣ'
			) as counter_document
		from accounting.cash_receipt_order cro
		join accounting.ledger l
			on cro.ledger_id = l.id
		where _counterparty_id = cro.counterparty_id
		and (_financing is null or cro.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	
		union all
	
		select
			l.created_date::date as created_date,
			l.amount as main_debit,
			0 as main_credit,
			0 as counter_debit,
			l.amount as counter_credit,
			jsonb_build_object(
				'ru', 'Расходный кассовый ордер № ' || cpo.id,
				'tj', 'ОРДЕРИ СОДИРОТИ ХАЗИНАВӢ № ' || cpo.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Поступление денежных средств',
				'tj', 'Воридоти маблағҳои пулӣ'
			) as counter_document
		from accounting.cash_payment_order cpo
		join accounting.ledger l
			on cpo.ledger_id = l.id
		where _counterparty_id = cpo.counterparty_id
		and (_financing is null or cpo.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	
		union all
	
		select
			l.created_date::date as created_date,
			0 as main_debit,
			l.amount as main_credit,
			l.amount as counter_debit,
			0 as counter_credit,
			jsonb_build_object(
				'ru', 'Поступление ТМЗ № ' || wi.id,
				'tj', 'Қабули ММВ № ' || wi.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Реализация ТМЗ',
				'tj', 'Фурӯши ТМЗ'
			) as counter_document
		from accounting.warehouse_incoming wi
		join accounting.ledger l
			on wi.ledger_id = l.id
		where _counterparty_id = wi.counterparty_id
		and (_financing is null or wi.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	
		union all
	
		select
			l.created_date::date as created_date,
			0 as main_debit,
			l.amount as main_credit,
			l.amount as counter_debit,
			0 as counter_credit,
			jsonb_build_object(
				'ru', 'Поступление Услуг № ' || ws.id,
				'tj', 'Қабули Хизматрасониҳо № ' || ws.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Реализация Услуг',
				'tj', 'Фурӯши хизматрасониҳо'
			) as counter_document
		from accounting.warehouse_services ws
		join accounting.ledger l
			on ws.ledger_id = l.id
		where _counterparty_id = ws.counterparty_id
		and (_financing is null or ws.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	
		-- union all
	
		-- select
		-- 	l.created_date::date as created_date,
		-- 	0 as main_debit,
		-- 	l.amount as main_credit,
		-- 	l.amount as counter_debit,
		-- 	0 as counter_credit,
		-- 	jsonb_build_object(
		-- 		'ru', 'Возврат ТМЗ поставщику № ' || gr.id,
		-- 		'tj', 'Баргардонидани ТМЗ ба таъминкунанда № ' || gr.id
		-- 	) as main_document,
		-- 	jsonb_build_object(
		-- 		'ru', 'Возврат товара (от покупателя)',
		-- 		'tj', 'Баргардонидани мол (аз харидор)'
		-- 	) as counter_document
		-- from accounting.goods_return gr
		-- join accounting.ledger l
		-- 	on gr.ledger_id = l.id
		-- where _counterparty_id = gr.counterparty_id
		-- and (_financing is null or gr.financing = _financing)
		-- and (_contract_id is null or l.contract_id = _contract_id)
	
		union all
	
		select
			l.created_date::date as created_date,
			0 as main_debit,
			l.amount as main_credit,
			l.amount as counter_debit,
			0 as counter_credit,
			jsonb_build_object(
				'ru', 'Авансовый отчёт - Оплата № ' || aro.id,
				'tj', 'ПЕШПАРДОХТҲO № ' || aro.id
			) as main_document,
			jsonb_build_object(
				'ru', 'Оплата денежных средств',
				'tj', 'Пардохти маблағҳои пулӣ'
			) as counter_document
		from accounting.advance_report_oplata aro
		join accounting.ledger l
			on aro.ledger_id = l.id
		where _counterparty_id = aro.counterparty_id
		and (_financing is null or aro.financing = _financing)
		and (_contract_id is null or l.contract_id = _contract_id)
	),
	table_total as (
		select count(*) as total from all_operations
	),
	totals as (
		select
			sum(main_debit) as total_main_debit,
			sum(main_credit) as total_main_credit,
			sum(counter_debit) as total_counter_debit,
			sum(counter_credit) as total_counter_credit,
			
			case 
				when sum(main_debit) - sum(main_credit) > 0
				then 'left_main_debit'
				else 'left_main_credit'
			end as main_col_name,
			abs(sum(main_debit) - sum(main_credit)) as main_col_absolute,
			
			case 
				when sum(counter_debit) - sum(counter_credit) > 0
				then 'left_counter_debit'
				else 'left_counter_credit'
			end as counter_col_name,
			abs(sum(main_debit) - sum(main_credit)) as counter_col_absolute
		from all_operations
	),
	aggregated as (
		select jsonb_agg(t.paginated) as aggregateds
		from (
			select jsonb_build_object(
				'key', row_number() over (order by created_date),
				'created_date', created_date,
				'main_debit', main_debit,
				'main_credit', main_credit,
				'counter_debit', counter_debit,
				'counter_credit', counter_credit,
				'main_document', main_document,
				'counter_document', counter_document
			) as paginated 
			from all_operations
			order by created_date
			limit 1000 offset 0
		) t
	)
	select jsonb_build_object(
		'status', 200,
		'total', (select total from table_total),
		'table_data', (select aggregateds from aggregated),
		'total_main_debit', t.total_main_debit,
		'total_main_credit', t.total_main_credit,
		'total_counter_debit', t.total_counter_debit,
		'total_counter_credit', t.total_counter_credit,
		t.main_col_name, t.main_col_absolute,
		t.counter_col_name, t.counter_col_absolute
	) into _result
	from totals t;
	
	return _result;
end;
$BODY$;









