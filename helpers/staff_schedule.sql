



select * from hr.staff_schedule_comments;


select hr.get_schedules(
	'
		{
			"year": 2025,
			"month": 12,
			"staffs": [11, 70] 
		}
	'
);


select * from hr.staff_schedule
where month_closed is true

update hr.staff_schedule set
	month_closed = false
where month_closed is true

-- truncate hr.staff_schedule



CREATE OR REPLACE FUNCTION hr.get_schedules(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_year int = (jdata->>'year')::integer;
	_month int = (jdata->>'month')::integer;
	_staffs jsonb = (jdata->>'staffs')::jsonb;
	_result json;
BEGIN

	with m1 as (
		select
			staff_id,
			month_closed,
			json_object_agg(
				date,
				marker_id
			) date_and_marker
		from hr.staff_schedule ss
		where extract(year from ss.date) = _year
		and extract(month from ss.date) = _month
		and staff_id in (
			SELECT jsonb_array_elements_text(_staffs)::bigint
		)
		group by staff_id, month_closed
	),
	m2 as (
		select
			month_closed,
			json_object_agg(
				staff_id,
				date_and_marker
			) paginated
		from m1 group by month_closed
	)
	select jsonb_build_object(
		'year', _year, 
		'month', _month,
		'closed', m2.month_closed,
		'staffs', m2.paginated
	) into _result from m2;
	 
	return _result;
END;
$BODY$;



select * from hr.staff_schedule;



CREATE OR REPLACE FUNCTION hr.upsert_schedules (jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_year int = (jdata->>'year')::int;
	_month int = (jdata->>'month')::int;
	_user_id uuid = (jdata->>'user_id')::uuid;
	staffs jsonb = (jdata->>'staffs')::jsonb;
	
	_staff jsonb;
	_created jsonb = jsonb_build_object (
		'date', localtimestamp(0),
		'user_id', _user_id
	);
BEGIN

	-- || checking if the month is closed ||
	if exists (
		SELECT 1 FROM hr.staff_schedule
		WHERE EXTRACT(YEAR FROM date) = _year
		  AND EXTRACT(MONTH FROM date) = _month
		  AND staff_id IN (
		    SELECT (key::bigint) FROM jsonb_each(staffs) AS e(key, value)
		 )
		 group by month_closed
		 having month_closed is true
	)
		then 
			RAISE EXCEPTION 'Месяц закрыт и не подлежит изменению' USING ERRCODE = 'P0001';
	end if;

	/* UPSERT */
	FOR _staff IN SELECT * FROM jsonb_array_elements((jdata->>'staffs')::jsonb) LOOP
		INSERT INTO hr.staff_schedule (
		    staff_id, 
		   	date,
		    marker_id,
			hours,
		    created
		) VALUES (
		    (_staff->>'staff_id')::bigint,
		    (_staff->>'date')::date,
		    (_staff->>'marker_id')::bigint,
		    (_staff->>'hours')::integer,
			_created
		)
		ON CONFLICT (staff_id, date)
		DO UPDATE SET
		    marker_id = EXCLUDED.marker_id,
		    updated = _created;
	end loop;
	
	return jsonb_build_object(
		'status', 200,
		'msg', 'updated'
	);
END;
$BODY$;










CREATE OR REPLACE FUNCTION hr.close_month(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_year int;
	_month int;
	_staffs json;
	logs jsonb;
	month_is_closed boolean;
BEGIN
    _year := jdata->>'year';
    _month := jdata->>'month';
	_staffs := jdata->>'staffs';
	logs := (jdata->'log')::jsonb;

	-- || checking if the month is closed ||
	SELECT month_closed into month_is_closed
	FROM hr.staff_schedule
	WHERE EXTRACT(YEAR FROM date) = _year
	  AND EXTRACT(MONTH FROM date) = _month
	  AND staff_id IN (
		SELECT json_array_elements_text(_staffs)::bigint
	)
	group by month_closed
	having month_closed is true;
	
	if month_is_closed then 
		RAISE EXCEPTION 'Отчётный месяц уже закрыт и не подлежит изменению' USING ERRCODE = 'P0001';
	end if;
	-- || -------------------------------- ||

	-- || we close month ||:
	update hr.staff_schedule ss set 
		month_closed = true,
		updated = (logs || jsonb_build_object('date', CURRENT_TIMESTAMP))
	where extract(year from ss.date) = _year
	and extract(month from ss.date) = _month
	and staff_id in (
		SELECT json_array_elements_text(_staffs)::bigint
	);
	-- || --------------------------------- ||
	
	return jsonb_build_object(
		'status', 200,
		'msg', 'success'
	);
END;
$BODY$;



