

create table if not exists accounting.payment_order_incoming (
	id bigserial primary key,
	bank_account_id bigint not null references commons.accouting_bank_accounts(id),
	counterparty_id bigint not null REFERENCES accounting.counterparty(id),
	cash_flow_article_id bigint not null REFERENCES commons.accouting_cash_flow_articles(id),
	amount numeric not null,
	debit integer not null default 111254,
	credit integer not null,
	advance_account_credit integer not null,
	description text not null,
	created jsonb not null,
	updated jsonb
);


select * from accounting.payment_order_incoming;



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
	BEGIN

		if _id is null then
			insert into accounting.payment_order_incoming (
				financing,
				bank_account_id,
				counterparty_id,
				counterparty_contract,
				cash_flow_article_id,
				amount,
				credit,
				advance_account_credit,
				description,
				created
			) values (
				_financing,
				(jdata->>'bank_account_id')::bigint,
				(jdata->>'counterparty_id')::bigint,
				(jdata->>'counterparty_contract')::text,
				(jdata->>'cash_flow_article_id')::bigint,
				(jdata->>'amount')::numeric,
				(jdata->>'credit')::integer,
				(jdata->>'advance_account_credit')::integer,
				(jdata->>'description')::text,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.payment_order_incoming poi SET
				financing = _financing,
				bank_account_id = (jdata->>'bank_account_id')::bigint,
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				counterparty_contract = (jdata->>'counterparty_contract')::text,
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = (jdata->>'amount')::numeric,
				credit = (jdata->>'credit')::integer,
				advance_account_credit = (jdata->>'advance_account_credit')::integer,
				description = (jdata->>'description')::text,
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



CREATE OR REPLACE FUNCTION accounting.get_payment_order_incoming(
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
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
				'key', row_number() over(order by id),
				'id', id,
				'financing', financing,
				'bank_account_id', bank_account_id,
				'counterparty_id', counterparty_id,
				'cash_flow_article_id', cash_flow_article_id,
				'amount', amount,
				'credit', credit,
				'advance_account_credit', advance_account_credit,
				'description', description,
				'counterparty_contract', counterparty_contract,
				'created_date', (created->>'date')::date
			) aggregated
			from accounting.payment_order_incoming
			where financing = _financing 
			and (_id is null or id = _id)
			and (_date_from is null or _date_from::date <= (created->>'date')::date)
			and (_date_to is null or _date_to::date >= (created->>'date')::date)
			and (_bank_account_id is null or _bank_account_id = bank_account_id)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
			order by id limit _limit offset _offset
		) select jsonb_agg(m.aggregated) into _result from main m;

		return _result;
	end;
$BODY$;


select accounting.get_payment_order_incoming_id();



CREATE OR REPLACE FUNCTION accounting.get_payment_order_incoming_id()
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



