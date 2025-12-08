create table if not exists accounting.cash_payment_order
(
	id bigserial primary key,
	cash_flow_article_id bigint not null REFERENCES commons.accouting_cash_flow_articles(id),
	amount numeric not null,
	credit integer not null default 111110,
	debit integer not null,
	advance_account_debit integer not null,
	description text not null,
	
	given_to text not null,
	givent_to_document text not null,
	based_on text not null,
	
	created jsonb not null,
	updated jsonb
);


CREATE OR REPLACE FUNCTION accounting.get_cash_payment_order (
	_id bigint DEFAULT NULL::bigint,
	_created_date text DEFAULT NULL::text,
	_cash_flow_article_id bigint DEFAULT NULL::bigint,
	_debit integer DEFAULT NULL::integer,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0
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
				'cash_flow_article_id', cash_flow_article_id,
				'amount', amount,
				'debit', debit,
				'advance_account_debit', advance_account_debit,
				'description', description,
				'created_date', (created->>'date')::date,
				
				'given_to', given_to,
				'given_to_document', given_to_document,
				'based_on', based_on
			) aggregated
			from accounting.cash_payment_order
			where (_id is null or id = _id)
			and (_created_date is null or _created_date::date = (created->>'date')::date)
			and (_cash_flow_article_id is null or _cash_flow_article_id = cash_flow_article_id)
			and (_debit is null or _debit = debit)
			order by id limit _limit offset _offset
		) select jsonb_agg(m.aggregated) into _result from main m;

		return _result;
	end;
$BODY$;



select * from accounting.cash_payment_order



CREATE OR REPLACE FUNCTION accounting.upsert_get_cash_payment_order(jdata json)
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
			insert into accounting.cash_payment_order (
				cash_flow_article_id,
				amount,
				debit,
				advance_account_debit,
				description,
				
				given_to,
				given_to_document,
				based_on,
				
				created
			) values (
				(jdata->>'cash_flow_article_id')::bigint,
				(jdata->>'amount')::numeric,
				(jdata->>'debit')::integer,
				(jdata->>'advance_account_debit')::integer,
				(jdata->>'description')::text,
				
				(jdata->>'given_to')::text,
				(jdata->>'given_to_document')::text,
				(jdata->>'based_on')::text,
				
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.cash_payment_order cpo SET
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = (jdata->>'amount')::numeric,
				debit = (jdata->>'debit')::integer,
				advance_account_debit = (jdata->>'advance_account_debit')::integer,
				description = (jdata->>'description')::text,
				
				given_to = (jdata->>'given_to')::text,
				given_to_document = (jdata->>'given_to_document')::text,
				based_on = (jdata->>'based_on')::text,
				
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             cpo.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE cpo.created
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



CREATE OR REPLACE FUNCTION accounting.get_cash_payment_order_id(
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
		from accounting.cash_payment_order
		order by id desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;