create type accounting.advance_type as enum ('advances', 'tmz', 'oplata', 'prochee');

create table if not exists accounting.advance_report (
	id bigserial primary key,
	staff_id bigint not null references hr.staff(id),
	purpose text not null,
	credit integer not null default 114610,
	advance_type accounting.advance_type not null,
	table_data jsonb not null,
	created jsonb not null,
	updated json
);


select * from accounting.advance_report;

CREATE OR REPLACE FUNCTION accounting.upsert_advance_report(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id uuid = (jdata->>'user_id')::uuid;
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		isUpdate boolean = false;
		
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_purpose text = (jdata->>'purpose')::text;
		_advance_type accounting.advance_type = (jdata->>'advance_type')::accounting.advance_type;
		_counterparty_id bigint = (jdata->>'counterparty_id')::bigint;
		_table_data jsonb = (jdata->>'table_data')::jsonb;
	BEGIN

		if _id is null then
			insert into accounting.advance_report (
				staff_id,
				purpose,
				credit,
				advance_type,
				table_data,
				created
			) values (
				(jdata->>'staff_id')::bigint,
				(jdata->>'purpose')::text,
				(jdata->>'credit')::integer,
				(jdata->>'advance_type')::accounting.advance_type,
				(jdata->>'table_data')::jsonb,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			);
		else
			isUpdate = true;
			update accounting.advance_report ar SET
				staff_id = (jdata->>'staff_id')::bigint,
				purpose = (jdata->>'purpose')::text,
				credit = case when jdata->>'credit' is not null 
					then (jdata->>'credit')::integer else ar.credit end,
				advance_type = (jdata->>'advance_type')::accounting.advance_type,
				table_data = (jdata->>'table_data')::jsonb,
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             ar.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE ar.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'status', 200
		);
	end;
$BODY$;


select accounting.get_advance_report();


CREATE OR REPLACE FUNCTION accounting.get_advance_report(
	_id bigint DEFAULT NULL::bigint,
	_created_date text DEFAULT NULL::text,
	_staff_id bigint DEFAULT NULL::bigint,
	_credit bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 1000,
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
				'staff_id', staff_id,
				'purpose', purpose,
				'credit', credit,
				'advance_type', advance_type,
				'table_data', table_data,
				'created_date', (created->>'date')::date
			) aggregated
			from accounting.advance_report
			where (_id is null or id = _id)
			and (_created_date is null or _created_date::date = (created->>'date')::date)
			and (_staff_id is null or _staff_id = staff_id)
			and (_credit is null or _credit = credit)
			order by id limit _limit offset _offset
		) select jsonb_agg(m.aggregated) into _result from main m;

		return _result;
	end;
$BODY$;



select accounting.get_advance_report_report_number();

CREATE OR REPLACE FUNCTION accounting.get_advance_report_id()
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select id into _last_id
		from accounting.advance_report
		order by id desc limit 1;
		
		return _last_id;
	end;
$BODY$;

