



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



select * from hr.staff_schedule_comments;


select * from hr.staff_schedule;


CREATE OR REPLACE FUNCTION hr.get_schedules(json_data json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	year_v int = (json_data->>'year')::int;
	month_v int = (json_data->>'month')::int;
	staffs_v json = (json_data->>'staffs')::json;
	_result json;
BEGIN
	
	with staff_schedule as (
		select
			ss.staff_id id,
			json_object_agg(
				ss.date, 
				ss.marker_id
			) data
		from hr.staff_schedule ss
		where extract(year from ss.date) = year_v
		and extract(month from ss.date) = month_v
		and ss.staff_id in (
			SELECT json_array_elements_text(staffs_v)::bigint
		)
		group by ss.staff_id
	) select json_object_agg(id, data)
	from staff_schedule into _result;
	 
	return json_build_object(
		'year', year_v, 
		'month', month_v, 
		'staffs', _result
	);
END;
$BODY$;














CREATE OR REPLACE FUNCTION hr.upsert_schedules (json_data json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	year_v int = (json_data->>'year')::int;
	month_v int = (json_data->>'month')::int;
	staffs json = (json_data->>'staffs')::json;
	logs jsonb = (json_data->>'log')::jsonb;
	staffs_ids json;
	staff_id_v bigint;
	date_v date;
	marker_id_v bigint;
BEGIN

	-- || checking if the month is closed ||
	if exists (
		SELECT 1 FROM hr.staff_schedule
		WHERE EXTRACT(YEAR FROM date) = year_v
		  AND EXTRACT(MONTH FROM date) = month_v
		  AND staff_id IN (
		    SELECT (key::bigint)
		    FROM json_each(staffs) AS e(key, value)
		 )
		 group by month_closed
		 having month_closed = true
	)
		then 
			-- return '{"status": 400, "msg": 9"}'::json;
			RAISE EXCEPTION '9' USING ERRCODE = 'P0001';
	end if;

	-- || we create/update each staff data ||:
	FOR staff_id_v IN SELECT json_object_keys(staffs) LOOP
		
		FOR date_v IN SELECT json_object_keys(staffs->staff_id_v::text) LOOP
			marker_id_v := staffs->staff_id_v::text->date_v::text;
			
            INSERT INTO hr.staff_schedule (
                staff_id, 
               	date, 
                marker_id, 
                created
            ) VALUES (
                staff_id_v,
                date_v,
                marker_id_v,
                (logs || jsonb_build_object('date', localtimestamp(0)))
            )
            ON CONFLICT (staff_id, date)
            DO UPDATE SET
                marker_id = EXCLUDED.marker_id,
                updated = (logs || jsonb_build_object('date', localtimestamp(0)));
			
		end loop;
	end loop;
	
	return '{"status": 200, "msg": 1}'::json;
END;
$BODY$;
















CREATE OR REPLACE FUNCTION hr.close_month(json_data json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	year_v int;
	month_v int;
	staffs_v json;
	logs jsonb;
	month_closed_v boolean;
BEGIN
    year_v := json_data->>'year';
    month_v := json_data->>'month';
	staffs_v := json_data->>'staffs';
	logs := (json_data->'log')::jsonb;

	-- || checking if the month is closed ||
	SELECT month_closed into month_closed_v
	FROM hr.staff_schedule
	WHERE EXTRACT(YEAR FROM date) = year_v
	  AND EXTRACT(MONTH FROM date) = month_v
	  AND staff_id IN (
		SELECT json_array_elements_text(staffs_v)::bigint
	)
	group by month_closed
	having month_closed = true;
	
	if month_closed_v then 
		RAISE EXCEPTION '10' USING ERRCODE = 'P0001';
	end if;
	-- || -------------------------------- ||

	-- || we close month ||:
	update hr.staff_schedule ss set 
		month_closed = true,
		updated = (logs || jsonb_build_object('date', CURRENT_TIMESTAMP))
	where extract(year from ss.date) = year_v
	and extract(month from ss.date) = month_v
	and staff_id in (
		SELECT json_array_elements_text(staffs_v)::bigint
	);
	-- || --------------------------------- ||
	
	return '{"status": 200, "msg": 11}'::json;
END;
$BODY$;















select commons.get_current_month (
	4, 2025
)

CREATE OR REPLACE FUNCTION commons.get_current_month(
	month_param integer DEFAULT NULL::integer,
	year_param integer DEFAULT NULL::integer)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE 
    cur_month json;
BEGIN
    
	IF month_param IS NULL AND year_param IS NULL THEN
		select json_array(
			select generate_series(
    	       (date (date_trunc('month', now())::date))::timestamp,
    	       (date ((date_trunc('month', now()) + interval '1 month - 1 day')::date))::timestamp,
    	       interval '1 day'
    	     )::date
		) into cur_month;
		
		RETURN json_build_object('current_date', current_date, 'whole_month', cur_month);
	ELSE
		IF month_param IS NULL OR year_param IS NULL THEN
		    RAISE EXCEPTION 'Both month and year parameters must be provided if any one is provided.' USING ERRCODE = 'P0001';

        END IF;
        
        IF month_param < 1 OR month_param > 12 THEN
			RAISE EXCEPTION 'Invalid month value. Month should be between 1 and 12.' USING ERRCODE = 'P0001';
        END IF;

  		cur_month := (
            SELECT json_agg(d::date)
            FROM generate_series(
                make_date(year_param, month_param, 1),
                CASE
                    WHEN month_param = 12 
                        THEN make_date(year_param + 1, 1, 1) - interval '1 day'
                    ELSE 
                        make_date(year_param, month_param + 1, 1) - interval '1 day'
                END,
                interval '1 day'
            ) d
        );
		
		RETURN json_build_object(
			case when month_param = extract(month from current_date) 
			and year_param = extract(year from current_date)
			then 'current_date' else 'first_date' end,
			case when month_param = extract(month from current_date)
			and year_param = extract(year from current_date)
			then current_date else make_date(year_param, month_param, 1) end,
			'whole_month', cur_month
		);
	END IF;
END;
$BODY$;




