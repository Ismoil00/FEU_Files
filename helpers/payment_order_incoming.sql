


select * from accounting.payment_order_incoming;


select * from accounting.ledger 
where id > 104
order by id;



CREATE OR REPLACE FUNCTION accounting.upsert_payment_order_incoming(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_ledger_id bigint = (jdata->>'ledger_id')::bigint;
		_credit integer = (jdata->>'credit')::integer;
		_amount numeric = (jdata->>'amount')::numeric;
		_contract_id bigint = (jdata->>'contract_id')::bigint;
		_staff_id bigint = (jdata->>'staff_id')::bigint;
	BEGIN

		/* we fill ledger with the accountingentry */
		SELECT accounting.upsert_ledger(
			111254,
			_credit,
			_amount,
			_contract_id,
			_staff_id,
			_ledger_id
		) INTO _ledger_id;

		-- insertion
		if _id is null then
			insert into accounting.payment_order_incoming (
				financing,
				bank_account_id,
				counterparty_id,
				contract_id,
				contract_text,
				cash_flow_article_id,
				amount,
				credit,
				description,
				ledger_id,
				staff_id,
				staff_id_document,
				department_id,
				received_from,
				created
			) values (
				_financing,
				(jdata->>'bank_account_id')::bigint,
				(jdata->>'counterparty_id')::bigint,
				_contract_id,
				jdata->>'contract_text',
				(jdata->>'cash_flow_article_id')::bigint,
				_amount,
				_credit,
				jdata->>'description',
				_ledger_id,
				_staff_id,
				jdata->>'staff_id_document',
				(jdata->>'department_id')::bigint,
				jdata->>'received_from',
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;

		-- update
		else
			update accounting.payment_order_incoming poi SET
				financing = _financing,
				bank_account_id = (jdata->>'bank_account_id')::bigint,
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				contract_id = _contract_id,
				contract_text = jdata->>'contract_text',
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = _amount,
				credit = _credit,
				description = jdata->>'description',
				ledger_id = _ledger_id,
				staff_id = _staff_id,
				staff_id_document = jdata->>'staff_id_document',
				department_id = (jdata->>'department_id')::bigint,
				received_from = jdata->>'received_from',
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             poi.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE poi.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;

		return json_build_object(
			'msg', case when _id is null then 'created' else 'updated' end,
			'status', 200,
			'id', _id
		);
	end;
$BODY$;



select * from accounting.payment_order_incoming



select accounting.get_cash_payment_order (
	'budget',
	null,
	null,
	null,
	null,
	null,
	100,
	0
)



CREATE OR REPLACE FUNCTION accounting.get_payment_order_incoming(
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_bank_account_id bigint DEFAULT NULL::bigint,
	_counterparty_id bigint DEFAULT NULL::bigint,
	_staff_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN

		with main as (
			select *
			from accounting.payment_order_incoming
			where financing = _financing 
			and (_id is null or id = _id)
			and (_date_from is null or _date_from::date <= (created->>'date')::date)
			and (_date_to is null or _date_to::date >= (created->>'date')::date)
			and (_bank_account_id is null or _bank_account_id = bank_account_id)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
			and (_staff_id is null or _staff_id = staff_id)
		),
		total_count as (
			select count(*) total from main
		),
		paginated as (
			select jsonb_build_object(
				'key', row_number() over(order by id),
				'id', id,
				'financing', financing,
				'bank_account_id', bank_account_id,
				'counterparty_id', counterparty_id,
				'contract_id', contract_id,
				'contract_text', contract_text,
				'ledger_id', ledger_id,
				'staff_id', staff_id,
				'staff_id_document', staff_id_document,
				'department_id', department_id,
				'cash_flow_article_id', cash_flow_article_id,
				'amount', amount,
				'credit', credit,
				'description', description,
				'received_from', received_from,
				'created_date', (created->>'date')::date
			) aggregated
			from main
			order by id limit _limit offset _offset
		)
		select jsonb_build_object(
			'results', jsonb_agg(p.aggregated),
			'total', (select total from total_count),
			'status', 200
		) into _result from paginated p;

		return _result;
	end;
$BODY$;


select accounting.get_payment_order_incoming_id();



CREATE OR REPLACE FUNCTION accounting.get_payment_order_incoming_id(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select id 
		into _last_id
		from accounting.payment_order_incoming
		order by id desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;



