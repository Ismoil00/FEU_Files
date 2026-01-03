create type accounting.advance_type as enum ('advances', 'tmz', 'oplata', 'prochee');

create table if not exists accounting.advance_report (
	id bigserial primary key,
	staff_id bigint not null references hr.staff(id),
	purpose text not null,
	credit integer not null default 114610,
	debit integer not null,
	advance_type accounting.advance_type not null,
	table_data jsonb not null,
	created jsonb not null,
	updated json
);


select * from accounting.advance_report;





CREATE OR REPLACE FUNCTION accounting.upsert_advance_report(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id uuid = (jdata->>'user_id')::uuid;
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		isUpdate boolean = false;
		
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_purpose text = (jdata->>'purpose')::text;
		_advance_type accounting.advance_type = (jdata->>'advance_type')::accounting.advance_type;
		_counterparty_id bigint = (jdata->>'counterparty_id')::bigint;
		_table_data jsonb = (jdata->>'table_data')::jsonb;
	BEGIN

		if _id is null then
			insert into accounting.advance_report (
				financing,
				staff_id,
				purpose,
				debit,
				advance_type,
				table_data,
				created
			) values (
				_financing,
				(jdata->>'staff_id')::bigint,
				(jdata->>'purpose')::text,
				(jdata->>'debit')::integer,
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
				financing = _financing,
				staff_id = (jdata->>'staff_id')::bigint,
				purpose = (jdata->>'purpose')::text,
				debit = (jdata->>'debit')::integer,
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
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_staff_id bigint DEFAULT NULL::bigint,
	_debit integer DEFAULT NULL::integer,
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
			select *
			from accounting.advance_report
			where financing = _financing 
			and (_id is null or id = _id)
			and (_date_from is null or _date_from::date <= (created->>'date')::date)
			and (_date_to is null or _date_to::date >= (created->>'date')::date)
			and (_staff_id is null or _staff_id = staff_id)
			and (_debit is null or _debit = debit)
		),
		total_count as (
			select count(*) total from main
		),
		paginated as (
			select jsonb_build_object(
				'key', row_number() over(order by id),
				'id', id,
				'financing', financing,
				'staff_id', staff_id,
				'purpose', purpose,
				'debit', debit,
				'advance_type', advance_type,
				'table_data', table_data,
				'created_date', (created->>'date')::date
			) aggregated
			from accounting.advance_report
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

