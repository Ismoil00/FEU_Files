create table if not exists accounting.cash_receipt_order
(
	id bigserial primary key,
	cash_flow_article_id bigint not null REFERENCES commons.accouting_cash_flow_articles(id),
	amount numeric not null,
	debit integer not null default 111110,
	credit integer not null,
	advance_account_credit integer not null,
	description text not null,
	
	excepted_from text not null,
	based_on text not null,
	
	created jsonb not null,
	updated jsonb
);



CREATE OR REPLACE FUNCTION accounting.get_cash_receipt_order (
	_id bigint DEFAULT NULL::bigint,
	_created_date text DEFAULT NULL::text,
	_cash_flow_article_id bigint DEFAULT NULL::bigint,
	_credit integer DEFAULT NULL::integer,
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
				'cash_flow_article_id', cash_flow_article_id,
				'amount', amount,
				'credit', credit,
				'advance_account_credit', advance_account_credit,
				'description', description,
				'created_date', (created->>'date')::date,
				
				'excepted_from', excepted_from,
				'based_on', based_on
			) aggregated
			from accounting.cash_receipt_order
			where (_id is null or id = _id)
			and (_created_date is null or _created_date::date = (created->>'date')::date)
			and (_cash_flow_article_id is null or _cash_flow_article_id = cash_flow_article_id)
			and (_credit is null or _credit = credit)
			order by id limit _limit offset _offset
		) select jsonb_agg(m.aggregated) into _result from main m;

		return _result;
	end;
$BODY$;



-- select * from accounting.cash_receipt_order



CREATE OR REPLACE FUNCTION accounting.upsert_cash_receipt_order(
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
			insert into accounting.cash_receipt_order (
				cash_flow_article_id,
				amount,
				credit,
				advance_account_credit,
				description,
				
				excepted_from,
				based_on,
				
				created
			) values (
				(jdata->>'cash_flow_article_id')::bigint,
				(jdata->>'amount')::numeric,
				(jdata->>'credit')::integer,
				(jdata->>'advance_account_credit')::integer,
				(jdata->>'description')::text,
				
				(jdata->>'excepted_from')::text,
				(jdata->>'based_on')::text,
				
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.cash_receipt_order cro SET
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = (jdata->>'amount')::numeric,
				credit = (jdata->>'credit')::integer,
				advance_account_credit = (jdata->>'advance_account_credit')::integer,
				description = (jdata->>'description')::text,
				
				excepted_from = (jdata->>'excepted_from')::text,
				based_on = (jdata->>'based_on')::text,
				
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             cro.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE cro.created
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



CREATE OR REPLACE FUNCTION accounting.get_cash_receipt_order_id(
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
		from accounting.cash_receipt_order
		order by id desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;