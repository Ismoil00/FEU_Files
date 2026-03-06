






select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector is null;




CREATE OR REPLACE FUNCTION reports.get_account_card (
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
		return reports.account_card_cash_flow_article (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);

	 elsif _related_selector = 'counterparty_contract' then
		return reports.account_card_counterparty_contract (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'staff' then
		return reports.account_card_staff (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'products' then
	 	return reports.account_card_products (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 
	 elsif _related_selector = 'estimates' then
	 	return reports.account_card_estimates (
				_financing,
				_account,
				_date_from,
				_date_to,
				_limit,
				_offset
			);
	 	
	 else
	 	return reports.account_card_not_content (
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
--          cash_flow_article
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'cash_flow_article';


		select * from accounting.payment_order_incoming por
		
		select * from accounting.payment_order_outgoing pou
		
		select * from accounting.cash_payment_order cpo
		
		select * from accounting.cash_receipt_order cro
		

select reports.account_card_cash_flow_article (
	'budget',
	111254,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);




CREATE OR REPLACE FUNCTION reports.account_card_cash_flow_article (
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

	/* QUERY */
	WITH main AS (
		SELECT
			l.created_date::date AS created_date,
			l.debit,
			l.credit,
			l.amount,
			
			-- METADATAs
			case
				when por.cash_flow_article_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Платежное поручение входящее', por.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти воридотӣ', por.id, 'аз', l.created_date)
				) 
				when pou.cash_flow_article_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Платежное поручение исходящее', pou.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти содиротӣ', pou.id, 'аз', l.created_date)
				) 
				when cpo.cash_flow_article_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Расходный кассовый ордер', cpo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ СОДИРОТИ ХАЗИНАВӢ', cpo.id, 'аз', l.created_date)
				) 
				when cro.cash_flow_article_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Приходный кассовый ордер', cro.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ ВОРИДОТИ ХАЗИНАВӢ', cro.id, 'аз', l.created_date)
				)
				else jsonb_build_object(
					'ru', null,
					'tj', null
				)
			end as document,
			'' as analytic_debit,
			'' as analytic_credit
		FROM accounting.ledger l
		left join accounting.payment_order_incoming por
			on l.id = por.ledger_id
		left join accounting.payment_order_outgoing pou
			on l.id = pou.ledger_id
		left join accounting.cash_payment_order cpo
			on l.id = cpo.ledger_id
		left join accounting.cash_receipt_order cro
			on l.id = cro.ledger_id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date BETWEEN _date_from AND _date_to
		  and coalesce(
			por.cash_flow_article_id,
			pou.cash_flow_article_id,
			cpo.cash_flow_article_id,
			cro.cash_flow_article_id
		  ) is not null
	),
	-- opening balance for _account before _date_from
	prev_saldo_raw AS (
		SELECT
			COALESCE(SUM(l.amount) FILTER (WHERE l.debit = _account), 0)
				- COALESCE(SUM(l.amount) FILTER (WHERE l.credit = _account), 0) AS balance
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date < _date_from
	),
	-- totals in period for the main account
	period_totals AS (
		SELECT
			COALESCE(SUM(m.amount) FILTER (WHERE m.debit = _account), 0) AS debit_amount,
			COALESCE(SUM(m.amount) FILTER (WHERE m.credit = _account), 0) AS credit_amount
		FROM main m
	),
	-- ordered rows with running saldo (saldo after each row, including that row)
	ordered_with_saldo AS (
		SELECT
			m.*,
			(p.balance + SUM(
				CASE WHEN m.debit = _account THEN m.amount WHEN m.credit = _account THEN -m.amount ELSE 0 END
			) OVER (ORDER BY m.created_date ROWS UNBOUNDED PRECEDING)) AS saldo_amount
		FROM main m
		CROSS JOIN prev_saldo_raw p
	),
	total_count_cte AS (
		SELECT COUNT(*) AS total_count FROM main
	),
	-- paginated rows with sequential id for table_data
	paginated AS (
		SELECT
			row_number() OVER (ORDER BY created_date) AS id,
			created_date,
			document,
			analytic_debit,
			analytic_credit,
			debit,
			credit,
			amount,
			ABS(saldo_amount) AS saldo_amount,
			(saldo_amount >= 0) AS saldo_debit
		FROM ordered_with_saldo
		ORDER BY created_date
		LIMIT _limit
		OFFSET _offset
	),
	-- account_data: prev_saldo and next_saldo (opening and closing for period)
	account_data_cte AS (
		SELECT
			_account AS account,
			pt.debit_amount,
			pt.credit_amount,
			p.balance AS prev_balance,
			p.balance + pt.debit_amount - pt.credit_amount AS next_balance
		FROM prev_saldo_raw p
		CROSS JOIN period_totals pt
	),
	table_data_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'id', id,
					'created_date', created_date,
					'document', document,
					'analytic_debit', analytic_debit,
					'analytic_credit', analytic_credit,
					'debit', debit,
					'credit', credit,
					'amount', amount,
					'saldo_amount', saldo_amount,
					'saldo_debit', saldo_debit
				) ORDER BY id
			),
			'[]'::jsonb
		) AS table_data
		FROM paginated
	)
	SELECT jsonb_build_object(
		'type', 'cash_flow_article',
		'status', 200,
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'table_data', (SELECT table_data FROM table_data_json),
		'total_count', (SELECT total_count FROM total_count_cte),
		'account_data', (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'prev_saldo_debit', (prev_balance >= 0),
				'next_saldo_amount', ABS(next_balance),
				'prev_saldo_amount', ABS(prev_balance)
			)
			FROM account_data_cte
		)
	) INTO _result;

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

		
-- select ledger_id from accounting.manual_operations

-- select ledger_id from accounting.payment_order_incoming por

-- select ledger_id from accounting.payment_order_outgoing pou

-- select ledger_id from accounting.cash_payment_order cpo

-- select ledger_id from accounting.cash_receipt_order cro

-- select ledger_id from accounting.warehouse_services

-- select ledger_id from accounting.warehouse_incoming

select ledger_id from accounting.advance_report_oplata
		

select reports.account_card_counterparty_contract (
	'budget',
	211110,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);




CREATE OR REPLACE FUNCTION reports.account_card_counterparty_contract (
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

	/* QUERY */
	WITH main AS (
		SELECT
			l.created_date::date AS created_date,
			l.debit,
			l.credit,
			l.amount,
			
			-- METADATAs
			case
				when por.contract_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Платежное поручение входящее', por.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти воридотӣ', por.id, 'аз', l.created_date)
				) 
				when pou.contract_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Платежное поручение исходящее', pou.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти содиротӣ', pou.id, 'аз', l.created_date)
				) 
				when cpo.contract_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Расходный кассовый ордер', cpo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ СОДИРОТИ ХАЗИНАВӢ', cpo.id, 'аз', l.created_date)
				) 
				when cro.contract_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Приходный кассовый ордер', cro.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ ВОРИДОТИ ХАЗИНАВӢ', cro.id, 'аз', l.created_date)
				)
				when mo.contract_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Операции, введенные вручную', mo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Амалиётҳои дастӣ воридшуда', mo.id, 'аз', l.created_date)
				)
				when ws.contract_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Поступление Услуг', ws.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Қабули Хизматрасониҳо', ws.id, 'аз', l.created_date)
				)
				when wi.contract_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Поступление ТМЗ', wi.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Қабули ММВ', wi.id, 'аз', l.created_date)
				)
				when aro.contract_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Авансовый отчёт Оплата', aro.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ПЕШПАРДОХТҲO Пардохт', aro.id, 'аз', l.created_date)
				)
				else jsonb_build_object(
					'ru', null,
					'tj', null
				)
			end as document,
			'' as analytic_debit,
			'' as analytic_credit
		FROM accounting.ledger l
		left join accounting.payment_order_incoming por
			on l.contract_id = por.contract_id
		left join accounting.payment_order_outgoing pou
			on l.contract_id = pou.contract_id
		left join accounting.cash_payment_order cpo
			on l.contract_id = cpo.contract_id
		left join accounting.cash_receipt_order cro
			on l.contract_id = cro.contract_id
		left join accounting.manual_operations mo
			on l.contract_id = mo.contract_id
		left join accounting.warehouse_services ws
			on l.contract_id = ws.contract_id
		left join accounting.warehouse_incoming wi
			on l.contract_id = wi.contract_id
		left join accounting.advance_report_oplata aro
			on l.contract_id = aro.contract_id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date BETWEEN _date_from AND _date_to
		  AND l.contract_id IS NOT NULL
	),
	-- opening balance for _account before _date_from
	prev_saldo_raw AS (
		SELECT
			COALESCE(SUM(l.amount) FILTER (WHERE l.debit = _account), 0)
				- COALESCE(SUM(l.amount) FILTER (WHERE l.credit = _account), 0) AS balance
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date < _date_from
	),
	-- totals in period for the main account
	period_totals AS (
		SELECT
			COALESCE(SUM(m.amount) FILTER (WHERE m.debit = _account), 0) AS debit_amount,
			COALESCE(SUM(m.amount) FILTER (WHERE m.credit = _account), 0) AS credit_amount
		FROM main m
	),
	-- ordered rows with running saldo (saldo after each row, including that row)
	ordered_with_saldo AS (
		SELECT
			m.*,
			(p.balance + SUM(
				CASE WHEN m.debit = _account THEN m.amount WHEN m.credit = _account THEN -m.amount ELSE 0 END
			) OVER (ORDER BY m.created_date ROWS UNBOUNDED PRECEDING)) AS saldo_amount
		FROM main m
		CROSS JOIN prev_saldo_raw p
	),
	total_count_cte AS (
		SELECT COUNT(*) AS total_count FROM main
	),
	-- paginated rows with sequential id for table_data
	paginated AS (
		SELECT
			row_number() OVER (ORDER BY created_date) AS id,
			created_date,
			document,
			analytic_debit,
			analytic_credit,
			debit,
			credit,
			amount,
			ABS(saldo_amount) AS saldo_amount,
			(saldo_amount >= 0) AS saldo_debit
		FROM ordered_with_saldo
		ORDER BY created_date
		LIMIT _limit
		OFFSET _offset
	),
	-- account_data: prev_saldo and next_saldo (opening and closing for period)
	account_data_cte AS (
		SELECT
			_account AS account,
			pt.debit_amount,
			pt.credit_amount,
			p.balance AS prev_balance,
			p.balance + pt.debit_amount - pt.credit_amount AS next_balance
		FROM prev_saldo_raw p
		CROSS JOIN period_totals pt
	),
	table_data_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'id', id,
					'created_date', created_date,
					'document', document,
					'analytic_debit', analytic_debit,
					'analytic_credit', analytic_credit,
					'debit', debit,
					'credit', credit,
					'amount', amount,
					'saldo_amount', saldo_amount,
					'saldo_debit', saldo_debit
				) ORDER BY id
			),
			'[]'::jsonb
		) AS table_data
		FROM paginated
	)
	SELECT jsonb_build_object(
		'type', 'counterparty_contract',
		'status', 200,
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'table_data', (SELECT table_data FROM table_data_json),
		'total_count', (SELECT total_count FROM total_count_cte),
		'account_data', (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'prev_saldo_debit', (prev_balance >= 0),
				'next_saldo_amount', ABS(next_balance),
				'prev_saldo_amount', ABS(prev_balance)
			)
			FROM account_data_cte
		)
	) INTO _result;

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

		
-- select staff_id, ledger_id from accounting.advance_report_tmzos

-- select staff_id, ledger_id from accounting.advance_report_oplata

-- select staff_id, ledger_id from accounting.advance_report_prochee

-- select staff_id from accounting.payment_order_incoming por

-- select staff_id from accounting.payment_order_outgoing pou

-- select staff_id from accounting.cash_payment_order cpo

-- select staff_id from accounting.cash_receipt_order cro

-- select staff_id from accounting.warehouse_outgoing

		

select reports.account_card_staff (
	'budget',
	211510,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);




CREATE OR REPLACE FUNCTION reports.account_card_staff (
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

	/* QUERY */
	WITH main AS (
		SELECT
			l.created_date::date AS created_date,
			l.debit,
			l.credit,
			l.amount,
			
			-- METADATAs
			case
				when por.staff_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Платежное поручение входящее', por.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти воридотӣ', por.id, 'аз', l.created_date)
				) 
				when pou.staff_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Платежное поручение исходящее', pou.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти содиротӣ', pou.id, 'аз', l.created_date)
				) 
				when cpo.staff_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Расходный кассовый ордер', cpo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ СОДИРОТИ ХАЗИНАВӢ', cpo.id, 'аз', l.created_date)
				) 
				when cro.staff_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Приходный кассовый ордер', cro.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ ВОРИДОТИ ХАЗИНАВӢ', cro.id, 'аз', l.created_date)
				)
				when wo.staff_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Списание ОС, МБП', wo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Аз ҳисоб барорӣ ВА, МКА', wo.id, 'аз', l.created_date)
				)
				when art.staff_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Авансовый отчёт ТМЗ/ОС', art.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ПЕШПАРДОХТҲO ТМЗ/ОС', art.id, 'аз', l.created_date)
				)
				when aro.staff_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Авансовый отчёт Оплата', aro.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ПЕШПАРДОХТҲO Пардохт', aro.id, 'аз', l.created_date)
				)
				when arp.staff_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Авансовый отчёт Прочее', arp.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ПЕШПАРДОХТҲO Дигар', arp.id, 'аз', l.created_date)
				)
				else jsonb_build_object(
					'ru', concat_ws(' ', 'Выплата заработной платы', l.payroll_sheet_line_id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Пардохти музди меҳнат', l.payroll_sheet_line_id, 'аз', l.created_date)
				)
			end as document,
			'' as analytic_debit,
			'' as analytic_credit
		FROM accounting.ledger l
		left join accounting.payment_order_incoming por
			on l.staff_id = por.staff_id
		left join accounting.payment_order_outgoing pou
			on l.staff_id = pou.staff_id
		left join accounting.cash_payment_order cpo
			on l.staff_id = cpo.staff_id
		left join accounting.cash_receipt_order cro
			on l.staff_id = cro.staff_id
		left join accounting.warehouse_outgoing wo
			on l.staff_id = wo.staff_id
		left join accounting.advance_report_tmzos art
			on l.staff_id = art.staff_id
		left join accounting.advance_report_oplata aro
			on l.staff_id = aro.staff_id
		left join accounting.advance_report_prochee arp
			on l.staff_id = arp.staff_id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date BETWEEN _date_from AND _date_to
		  AND (l.staff_id IS NOT NULL OR l.payroll_sheet_line_id IS NOT NULL)
	),
	-- opening balance for _account before _date_from
	prev_saldo_raw AS (
		SELECT
			COALESCE(SUM(l.amount) FILTER (WHERE l.debit = _account), 0)
				- COALESCE(SUM(l.amount) FILTER (WHERE l.credit = _account), 0) AS balance
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date < _date_from
	),
	-- totals in period for the main account
	period_totals AS (
		SELECT
			COALESCE(SUM(m.amount) FILTER (WHERE m.debit = _account), 0) AS debit_amount,
			COALESCE(SUM(m.amount) FILTER (WHERE m.credit = _account), 0) AS credit_amount
		FROM main m
	),
	-- ordered rows with running saldo (saldo after each row, including that row)
	ordered_with_saldo AS (
		SELECT
			m.*,
			(p.balance + SUM(
				CASE WHEN m.debit = _account THEN m.amount WHEN m.credit = _account THEN -m.amount ELSE 0 END
			) OVER (ORDER BY m.created_date ROWS UNBOUNDED PRECEDING)) AS saldo_amount
		FROM main m
		CROSS JOIN prev_saldo_raw p
	),
	total_count_cte AS (
		SELECT COUNT(*) AS total_count FROM main
	),
	-- paginated rows with sequential id for table_data
	paginated AS (
		SELECT
			row_number() OVER (ORDER BY created_date) AS id,
			created_date,
			document,
			analytic_debit,
			analytic_credit,
			debit,
			credit,
			amount,
			ABS(saldo_amount) AS saldo_amount,
			(saldo_amount >= 0) AS saldo_debit
		FROM ordered_with_saldo
		ORDER BY created_date
		LIMIT _limit
		OFFSET _offset
	),
	-- account_data: prev_saldo and next_saldo (opening and closing for period)
	account_data_cte AS (
		SELECT
			_account AS account,
			pt.debit_amount,
			pt.credit_amount,
			p.balance AS prev_balance,
			p.balance + pt.debit_amount - pt.credit_amount AS next_balance
		FROM prev_saldo_raw p
		CROSS JOIN period_totals pt
	),
	table_data_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'id', id,
					'created_date', created_date,
					'document', document,
					'analytic_debit', analytic_debit,
					'analytic_credit', analytic_credit,
					'debit', debit,
					'credit', credit,
					'amount', amount,
					'saldo_amount', saldo_amount,
					'saldo_debit', saldo_debit
				) ORDER BY id
			),
			'[]'::jsonb
		) AS table_data
		FROM paginated
	)
	SELECT jsonb_build_object(
		'type', 'staff',
		'status', 200,
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'table_data', (SELECT table_data FROM table_data_json),
		'total_count', (SELECT total_count FROM total_count_cte),
		'account_data', (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'prev_saldo_debit', (prev_balance >= 0),
				'next_saldo_amount', ABS(next_balance),
				'prev_saldo_amount', ABS(prev_balance)
			)
			FROM account_data_cte
		)
	) INTO _result;

	 return _result;
end;
$BODY$;














/* -------------------------------------- */
--          products
/* -------------------------------------- */
		

select * from accounting.ledger l
join accounting.accounts a
	on l.debit = a.account
where related_selector = 'products';

			select * from accounting.warehouse_services

			select contract_id from accounting.warehouse_incoming wi
		
			select contract_id from accounting.warehouse_outgoing wo
			
			select contract_id from accounting.product_transfer pt
		
			select contract_id from accounting.advance_report_tmzos art
				
			select contract_id from accounting.warehouse_services ws

			select concat_ws(', ', c.name, cc.contract) as name
			from commons.counterparty_contracts cc
			join accounting.counterparty c
			on cc.counterparty_id = c.id

			select * from accounting.counterparty

select reports.account_card_products (
	'budget',
	131230,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);



CREATE OR REPLACE FUNCTION reports.account_card_products (
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

	/* QUERY */
	WITH main AS (
		SELECT
			l.created_date::date AS created_date,
			l.debit,
			l.credit,
			l.amount,
			
			-- METADATAs
			case
				when wi.name_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Поступление ТМЗ', wi.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Қабули ММВ', wi.id, 'аз', l.created_date)
				) 
				when wo.name_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Списание ОС, МБП', wo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Аз ҳисоб барорӣ ВА, МКА', wo.id, 'аз', l.created_date)
				) 
				when pt.name_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Перемещение ОС', pt.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Интиқоли ВА', pt.id, 'аз', l.created_date)
				) 
				when art.name_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Авансовый отчёт ТМЗ/ОС', art.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ПЕШПАРДОХТҲO ВА/МКА', art.id, 'аз', l.created_date)
				)
				when ws.service_nomenclature_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Поступление Услуг', ws.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Қабули Хизматрасониҳо', ws.id, 'аз', l.created_date)
				)
				else jsonb_build_object(
					'ru', null,
					'tj', null
				)
			end as document,
			'' as analytic_debit,
			'' as analytic_credit
		FROM accounting.ledger l
		left join accounting.warehouse_incoming wi
			on l.id = wi.ledger_id
		left join accounting.warehouse_outgoing wo
			on l.id = wo.ledger_id
		left join accounting.product_transfer pt
			on l.id = pt.ledger_id
		left join accounting.advance_report_tmzos art
			on l.id = art.ledger_id
		left join accounting.warehouse_services ws
			on l.id = ws.ledger_id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date BETWEEN _date_from AND _date_to
		  AND coalesce(
			  wi.name_id, 
			  wo.name_id, 
			  pt.name_id, 
			  art.name_id, 
			  ws.service_nomenclature_id
		  ) is not null
	),
	-- opening balance for _account before _date_from
	prev_saldo_raw AS (
		SELECT
			COALESCE(SUM(l.amount) FILTER (WHERE l.debit = _account), 0)
				- COALESCE(SUM(l.amount) FILTER (WHERE l.credit = _account), 0) AS balance
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date < _date_from
	),
	-- totals in period for the main account
	period_totals AS (
		SELECT
			COALESCE(SUM(m.amount) FILTER (WHERE m.debit = _account), 0) AS debit_amount,
			COALESCE(SUM(m.amount) FILTER (WHERE m.credit = _account), 0) AS credit_amount
		FROM main m
	),
	-- ordered rows with running saldo (saldo after each row, including that row)
	ordered_with_saldo AS (
		SELECT
			m.*,
			(p.balance + SUM(
				CASE WHEN m.debit = _account THEN m.amount WHEN m.credit = _account THEN -m.amount ELSE 0 END
			) OVER (ORDER BY m.created_date ROWS UNBOUNDED PRECEDING)) AS saldo_amount
		FROM main m
		CROSS JOIN prev_saldo_raw p
	),
	total_count_cte AS (
		SELECT COUNT(*) AS total_count FROM main
	),
	-- paginated rows with sequential id for table_data
	paginated AS (
		SELECT
			row_number() OVER (ORDER BY created_date) AS id,
			created_date,
			document,
			analytic_debit,
			analytic_credit,
			debit,
			credit,
			amount,
			ABS(saldo_amount) AS saldo_amount,
			(saldo_amount >= 0) AS saldo_debit
		FROM ordered_with_saldo
		ORDER BY created_date
		LIMIT _limit
		OFFSET _offset
	),
	-- account_data: prev_saldo and next_saldo (opening and closing for period)
	account_data_cte AS (
		SELECT
			_account AS account,
			pt.debit_amount,
			pt.credit_amount,
			p.balance AS prev_balance,
			p.balance + pt.debit_amount - pt.credit_amount AS next_balance
		FROM prev_saldo_raw p
		CROSS JOIN period_totals pt
	),
	table_data_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'id', id,
					'created_date', created_date,
					'document', document,
					'analytic_debit', analytic_debit,
					'analytic_credit', analytic_credit,
					'debit', debit,
					'credit', credit,
					'amount', amount,
					'saldo_amount', saldo_amount,
					'saldo_debit', saldo_debit
				) ORDER BY id
			),
			'[]'::jsonb
		) AS table_data
		FROM paginated
	)
	SELECT jsonb_build_object(
		'type', 'products',
		'status', 200,
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'table_data', (SELECT table_data FROM table_data_json),
		'total_count', (SELECT total_count FROM total_count_cte),
		'account_data', (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'prev_saldo_debit', (prev_balance >= 0),
				'next_saldo_amount', ABS(next_balance),
				'prev_saldo_amount', ABS(prev_balance)
			)
			FROM account_data_cte
		)
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

		
-- select staff_id, ledger_id from accounting.advance_report_tmzos

-- select staff_id, ledger_id from accounting.advance_report_oplata

-- select staff_id, ledger_id from accounting.advance_report_prochee

-- select staff_id from accounting.payment_order_incoming por

-- select staff_id from accounting.payment_order_outgoing pou

-- select staff_id from accounting.cash_payment_order cpo

-- select staff_id from accounting.cash_receipt_order cro

-- select staff_id from accounting.warehouse_outgoing

		

select reports.account_card_estimates (
	'budget',
	510100,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);




CREATE OR REPLACE FUNCTION reports.account_card_estimates (
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

	/* QUERY */
	WITH main AS (
		SELECT
			l.created_date::date AS created_date,
			l.debit,
			l.credit,
			l.amount,
			
			-- METADATAs
			case
				when poo.estimate_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Платежное поручение исходящее', poo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти содиротӣ', poo.id, 'аз', l.created_date)
				) 
				when cpo.estimate_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Расходный кассовый ордер', cpo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ СОДИРОТИ ХАЗИНАВӢ', cpo.id, 'аз', l.created_date)
				)
				when mo.estimate_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Операции, введенные вручную', mo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Амалиётҳои дастӣ воридшуда', mo.id, 'аз', l.created_date)
				)
				else jsonb_build_object(
					'ru', null,
					'tj', null
				)
			end as document,
			'' as analytic_debit,
			'' as analytic_credit
		FROM accounting.ledger l
		left join accounting.manual_operations mo
			on l.id = mo.ledger_id
		left join accounting.payment_order_outgoing poo
			on l.id = poo.ledger_id
		left join accounting.cash_payment_order cpo
			on l.id = cpo.ledger_id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date BETWEEN _date_from AND _date_to
		  and coalesce(mo.estimate_id, poo.estimate_id, cpo.estimate_id) is not null
	),
	-- opening balance for _account before _date_from
	prev_saldo_raw AS (
		SELECT
			COALESCE(SUM(l.amount) FILTER (WHERE l.debit = _account), 0)
				- COALESCE(SUM(l.amount) FILTER (WHERE l.credit = _account), 0) AS balance
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date < _date_from
	),
	-- totals in period for the main account
	period_totals AS (
		SELECT
			COALESCE(SUM(m.amount) FILTER (WHERE m.debit = _account), 0) AS debit_amount,
			COALESCE(SUM(m.amount) FILTER (WHERE m.credit = _account), 0) AS credit_amount
		FROM main m
	),
	-- ordered rows with running saldo (saldo after each row, including that row)
	ordered_with_saldo AS (
		SELECT
			m.*,
			(p.balance + SUM(
				CASE WHEN m.debit = _account THEN m.amount WHEN m.credit = _account THEN -m.amount ELSE 0 END
			) OVER (ORDER BY m.created_date ROWS UNBOUNDED PRECEDING)) AS saldo_amount
		FROM main m
		CROSS JOIN prev_saldo_raw p
	),
	total_count_cte AS (
		SELECT COUNT(*) AS total_count FROM main
	),
	-- paginated rows with sequential id for table_data
	paginated AS (
		SELECT
			row_number() OVER (ORDER BY created_date) AS id,
			created_date,
			document,
			analytic_debit,
			analytic_credit,
			debit,
			credit,
			amount,
			ABS(saldo_amount) AS saldo_amount,
			(saldo_amount >= 0) AS saldo_debit
		FROM ordered_with_saldo
		ORDER BY created_date
		LIMIT _limit
		OFFSET _offset
	),
	-- account_data: prev_saldo and next_saldo (opening and closing for period)
	account_data_cte AS (
		SELECT
			_account AS account,
			pt.debit_amount,
			pt.credit_amount,
			p.balance AS prev_balance,
			p.balance + pt.debit_amount - pt.credit_amount AS next_balance
		FROM prev_saldo_raw p
		CROSS JOIN period_totals pt
	),
	table_data_json AS (
		SELECT COALESCE(
			jsonb_agg(
				jsonb_build_object(
					'id', id,
					'created_date', created_date,
					'document', document,
					'analytic_debit', analytic_debit,
					'analytic_credit', analytic_credit,
					'debit', debit,
					'credit', credit,
					'amount', amount,
					'saldo_amount', saldo_amount,
					'saldo_debit', saldo_debit
				) ORDER BY id
			),
			'[]'::jsonb
		) AS table_data
		FROM paginated
	)
	SELECT jsonb_build_object(
		'type', 'estimates',
		'status', 200,
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'table_data', (SELECT table_data FROM table_data_json),
		'total_count', (SELECT total_count FROM total_count_cte),
		'account_data', (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'prev_saldo_debit', (prev_balance >= 0),
				'next_saldo_amount', ABS(next_balance),
				'prev_saldo_amount', ABS(prev_balance)
			)
			FROM account_data_cte
		)
	) INTO _result;

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

		
-- select staff_id, ledger_id from accounting.advance_report_tmzos

-- select staff_id, ledger_id from accounting.advance_report_oplata

-- select staff_id, ledger_id from accounting.advance_report_prochee

-- select staff_id from accounting.payment_order_incoming por

-- select staff_id from accounting.payment_order_outgoing pou

-- select staff_id from accounting.cash_payment_order cpo

-- select staff_id from accounting.cash_receipt_order cro

-- select staff_id from accounting.warehouse_outgoing

		

select reports.account_card_not_content (
	'budget',
	111200,
	'2026-01-01',
	'2026-03-03',
	1000,
	0
);


CREATE OR REPLACE FUNCTION reports.account_card_not_content (
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

	/* QUERY */
	WITH main AS (
		SELECT
			l.created_date::date AS created_date,
			l.debit,
			l.credit,
			l.amount,
			
			-- METADATAs
			case
				when poo.estimate_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Платежное поручение исходящее', poo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Фармони пардохти содиротӣ', poo.id, 'аз', l.created_date)
				) 
				when cpo.estimate_id is not null then jsonb_build_object(
					'ru', concat_ws(' ', 'Расходный кассовый ордер', cpo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'ОРДЕРИ СОДИРОТИ ХАЗИНАВӢ', cpo.id, 'аз', l.created_date)
				)
				when mo.estimate_id is not null then jsonb_build_object (
					'ru', concat_ws(' ', 'Операции, введенные вручную', mo.id, 'от', l.created_date),
					'tj', concat_ws(' ', 'Амалиётҳои дастӣ воридшуда', mo.id, 'аз', l.created_date)
				)
				else jsonb_build_object(
					'ru', null,
					'tj', null
				)
			end as document,
			'' as analytic_debit,
			'' as analytic_credit
		FROM accounting.ledger l
		left join accounting.manual_operations mo
			on l.id = mo.ledger_id
		left join accounting.payment_order_outgoing poo
			on l.id = poo.ledger_id
		left join accounting.cash_payment_order cpo
			on l.id = cpo.ledger_id
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date BETWEEN _date_from AND _date_to
		  and coalesce(mo.estimate_id, poo.estimate_id, cpo.estimate_id) is not null
	),
	-- opening balance for _account before _date_from
	prev_saldo_raw AS (
		SELECT
			COALESCE(SUM(l.amount) FILTER (WHERE l.debit = _account), 0)
				- COALESCE(SUM(l.amount) FILTER (WHERE l.credit = _account), 0) AS balance
		FROM accounting.ledger l
		WHERE l.draft IS NOT TRUE
		  AND l.financing = _financing
		  AND (l.debit = _account OR l.credit = _account)
		  AND l.created_date::date < _date_from
	),
	-- totals in period for the main account
	period_totals AS (
		SELECT
			COALESCE(SUM(m.amount) FILTER (WHERE m.debit = _account), 0) AS debit_amount,
			COALESCE(SUM(m.amount) FILTER (WHERE m.credit = _account), 0) AS credit_amount
		FROM main m
	),
	-- account_data: prev_saldo and next_saldo (opening and closing for period)
	account_data_cte AS (
		SELECT
			_account AS account,
			pt.debit_amount,
			pt.credit_amount,
			p.balance AS prev_balance,
			p.balance + pt.debit_amount - pt.credit_amount AS next_balance
		FROM prev_saldo_raw p
		CROSS JOIN period_totals pt
	)
	SELECT jsonb_build_object(
		'type', 'account',
		'status', 200,
		'department', (
			SELECT department
			FROM commons.department
			WHERE id = 12
		),
		'account_data', (
			SELECT jsonb_build_object(
				'account', account,
				'debit_amount', debit_amount,
				'credit_amount', credit_amount,
				'next_saldo_debit', (next_balance >= 0),
				'prev_saldo_debit', (prev_balance >= 0),
				'next_saldo_amount', ABS(next_balance),
				'prev_saldo_amount', ABS(prev_balance)
			)
			FROM account_data_cte
		)
	) INTO _result;

	 return _result;
end;
$BODY$;







