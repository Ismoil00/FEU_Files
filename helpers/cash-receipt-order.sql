


select * from accounting.cash_receipt_order;


		select accounting.get_cash_receipt_order (
			'special',
			null,
			null,
			null,
			null,
			null,
			100,
			0
		);


CREATE OR REPLACE FUNCTION accounting.get_cash_receipt_order (
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
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
			select *
			from accounting.cash_receipt_order
			where financing = _financing 
			and (_id is null or id = _id)
			and (_date_from is null or _date_from::date <= (created->>'date')::date)
			and (_date_to is null or _date_to::date >= (created->>'date')::date)
			and (_cash_flow_article_id is null or _cash_flow_article_id = cash_flow_article_id)
			and (_credit is null or _credit = credit)
		),
		total_count as (
			select count(*) total from main
		),
		paginated as (
			select jsonb_build_object(
				'key', row_number() over(order by id),
				'id', id,
				'financing', financing,
				'cash_flow_article_id', cash_flow_article_id,
				'amount', amount,
				'credit', credit,
				'advance_credit', advance_credit,
				'description', description,
				'based_on', based_on,
				'created_date', (created->>'date')::date,
				
				'received_from', received_from,
				'staff_id', staff_id,
				'staff_id_document', staff_id_document,
				'counterparty_id', counterparty_id,
				'contract', contract
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
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
	BEGIN

		if _id is null then
			insert into accounting.cash_receipt_order (
				financing,
				cash_flow_article_id,
				amount,
				credit,
				advance_credit,
				description,
				based_on,
				
				received_from,
				staff_id,
				staff_id_document,
				counterparty_id,
				contract,
				
				created
			) values (
				_financing,
				(jdata->>'cash_flow_article_id')::bigint,
				(jdata->>'amount')::numeric,
				(jdata->>'credit')::integer,
				(jdata->>'advance_credit')::integer,
				(jdata->>'description')::text,
				(jdata->>'based_on')::text,
				
				(jdata->>'received_from')::text,
				(jdata->>'staff_id')::bigint,
				(jdata->>'staff_id_document')::text,
				(jdata->>'counterparty_id')::bigint,
				(jdata->>'contract')::text,
				
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.cash_receipt_order cro SET
				financing = _financing,
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = (jdata->>'amount')::numeric,
				credit = (jdata->>'credit')::integer,
				advance_credit = (jdata->>'advance_credit')::integer,
				description = (jdata->>'description')::text,
				based_on = (jdata->>'based_on')::text,
				
				received_from = (jdata->>'received_from')::text,
				staff_id = (jdata->>'staff_id')::bigint,
				staff_id_document = (jdata->>'staff_id_document')::text,
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				contract = (jdata->>'contract')::text,
				
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

select * from accounting.advance_report

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