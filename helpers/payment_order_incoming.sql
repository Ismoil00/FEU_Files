

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


CREATE OR REPLACE FUNCTION accounting.upsert_payment_order_incoming(jdata json)
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
			insert into accounting.payment_order_incoming (
				bank_account_id,
				counterparty_id,
				cash_flow_article_id,
				amount,
				credit,
				advance_account_credit,
				description,
				created
			) values (
				(_entry->>'bank_account_id')::bigint,
				(_entry->>'counterparty_id')::bigint,
				(_entry->>'cash_flow_article_id')::bigint,
				(_entry->>'amount')::numeric,
				(_entry->>'credit')::integer,
				(_entry->>'advance_account_credit')::integer,
				(_entry->>'description')::text,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			);
		else
			update accounting.payment_order_incoming poi SET
				bank_account_id = (_entry->>'bank_account_id')::bigint,
				counterparty_id = (_entry->>'counterparty_id')::bigint,
				cash_flow_article_id = (_entry->>'cash_flow_article_id')::bigint,
				amount = (_entry->>'amount')::numeric,
				credit = (_entry->>'credit')::integer,
				advance_account_credit = (_entry->>'advance_account_credit')::integer,
				description = (_entry->>'description')::text,
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
			'status', 200
		);
	end;
$BODY$;


select accounting.get_payment_order_incoming();


CREATE OR REPLACE FUNCTION accounting.get_payment_order_incoming(
	_id bigint default null,
	_created_date text default null,
	_bank_account_id bigint default null,
	_counterparty_id bigint default null,
	_limit int default 100,
	_offset int default 100
)
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
				'credit', credit,
				'advance_account_credit', advance_account_credit,
				'description', description,
				'created_date', (created->>'date')::date
			) aggregated
			from accounting.payment_order_incoming
			where (_id is null or id = _id)
			and (_created_date is null or _created_date = (created->>date)::date)
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



