


SELECT * FROM commons.work_calendar_day_type

alter table commons.work_calendar_day_type
add column basic bool default false;


CREATE OR REPLACE FUNCTION commons.work_calendar_get_day_type(
	_type_id bigint DEFAULT NULL::bigint,
	_code text DEFAULT NULL::text,
	_default_hours integer DEFAULT NULL::integer
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _result json;
BEGIN

	SELECT json_agg(row_to_json(dt))
	FROM (
		SELECT 
			id, 
			code, 
			label, 
			default_hours,
			color,
			basic
		FROM commons.work_calendar_day_type
		WHERE (_type_id is null or id = _type_id) 
		and (_code is null or code = _code) 
		and (_default_hours is null or default_hours = _default_hours)
	) dt into _result;

    
    RETURN _result;
END;
$BODY$;



SELECT * FROM commons.work_calendar_day_type



CREATE OR REPLACE FUNCTION commons.upsert_work_calendar_day_type(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _result json;
	_id bigint = (jdata->>'id')::bigint;
	_label text = (jdata->>'label')::text;
BEGIN

	/* ------------------------------
		Validation: duplicate label
	------------------------------ */
    IF EXISTS (
        SELECT 1
        FROM commons.work_calendar_day_type t
        WHERE t.label = _label
          AND (_id IS NULL OR t.id <> _id)
    ) THEN
		RAISE EXCEPTION
        'Тип рабочего дня с меткой "%" уже существует.', _label USING ERRCODE = '23505';
    END IF;

	/* ------------------------------
		Create or Update
	------------------------------ */
	if _id is null THEN
		insert into commons.work_calendar_day_type (
			code, 
			label, 
			default_hours,
			color
		) values (
			(jdata->>'code')::text,
			(jdata->>'label')::text,
			(jdata->>'default_hours')::integer,
			COALESCE((jdata->>'color')::text, '#dff2fe')
		);
		
		return json_build_object('status', 200, 'msg', 'created');
	else
		update commons.work_calendar_day_type set
			code = (jdata->>'code')::text,
			label = (jdata->>'label')::text,
			default_hours = (jdata->>'default_hours')::integer,
			color = (jdata->>'color')::text,
			updated_at = LOCALTIMESTAMP(0)
		where id = _id;

		return json_build_object('status', 200, 'msg', 'updated');
	end if;
    
    RETURN _result;
END;
$BODY$;




