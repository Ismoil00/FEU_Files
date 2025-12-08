create table if not exists accounting.payment_order_outgoing
(
	id bigserial primary key,
	bank_account_id bigint not null references commons.accouting_bank_accounts(id),
	counterparty_id bigint not null REFERENCES accounting.counterparty(id),
	counterparty_contract text not null,
	cash_flow_article_id bigint not null REFERENCES commons.accouting_cash_flow_articles(id),
	amount numeric not null,
	credit integer not null default 111254,
	debit integer not null,
	advance_account_debit integer not null,
	description text not null,
	payment_date date default current_date,
	created jsonb not null,
	updated jsonb
);


CREATE OR REPLACE FUNCTION accounting.get_payment_order_outgoing (
	_id bigint DEFAULT NULL::bigint,
	_created_date text DEFAULT NULL::text,
	_payment_date text DEFAULT NULL::text,
	_bank_account_id bigint DEFAULT NULL::bigint,
	_counterparty_id bigint DEFAULT NULL::bigint,
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
			select jsonb_build_object(
				'id', id,
				'bank_account_id', bank_account_id,
				'counterparty_id', counterparty_id,
				'cash_flow_article_id', cash_flow_article_id,
				'amount', amount,
				'debit', debit,
				'advance_account_debit', advance_account_debit,
				'description', description,
				'counterparty_contract', counterparty_contract,
				'created_date', (created->>'date')::date,
				'payment_date', payment_date
			) aggregated
			from accounting.payment_order_outgoing
			where (_id is null or id = _id)
			and (_created_date is null or _created_date::date = (created->>'date')::date)
			and (_payment_date is null or _payment_date::date = payment_date)
			and (_bank_account_id is null or _bank_account_id = bank_account_id)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
			order by id limit _limit offset _offset
		) select jsonb_agg(m.aggregated) into _result from main m;

		return _result;
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.upsert_payment_order_outgoing(
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
	BEGIN

		if _id is null then
			insert into accounting.payment_order_outgoing (
				bank_account_id,
				counterparty_id,
				counterparty_contract,
				cash_flow_article_id,
				amount,
				debit,
				advance_account_debit,
				description,
				payment_date,
				created
			) values (
				(jdata->>'bank_account_id')::bigint,
				(jdata->>'counterparty_id')::bigint,
				(jdata->>'counterparty_contract')::text,
				(jdata->>'cash_flow_article_id')::bigint,
				(jdata->>'amount')::numeric,
				(jdata->>'debit')::integer,
				(jdata->>'advance_account_debit')::integer,
				(jdata->>'description')::text,
				(jdata->>'payment_date')::date,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.payment_order_outgoing poo SET
				bank_account_id = (jdata->>'bank_account_id')::bigint,
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				counterparty_contract = (jdata->>'counterparty_contract')::text,
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = (jdata->>'amount')::numeric,
				debit = (jdata->>'debit')::integer,
				advance_account_debit = (jdata->>'advance_account_debit')::integer,
				description = (jdata->>'description')::text,
				payment_date = (jdata->>'payment_date')::date,
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             poo.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE poo.created
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



CREATE OR REPLACE FUNCTION accounting.get_payment_order_outgoing_id(
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
		from accounting.payment_order_outgoing
		order by id desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;