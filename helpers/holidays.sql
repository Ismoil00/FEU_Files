

select * FROM commons.work_calendar_fixed_holiday

alter table commons.work_calendar_fixed_holiday
add column basic boolean default false

CREATE OR REPLACE FUNCTION commons.work_calendar_get_holidays(
	_holiday_id bigint DEFAULT NULL::bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _result json;
BEGIN

	SELECT json_agg(row_to_json(dt))
	FROM
	(
		SELECT 
			id, 
			day,
			end_day,
			month, 
			label, 
			is_day_off,
			basic
		FROM commons.work_calendar_fixed_holiday
		WHERE (id = _holiday_id or _holiday_id is null)
	)dt
	into _result;

    
    RETURN _result;
END;
$BODY$;

















CREATE OR REPLACE FUNCTION commons.work_calendar_holiday_upsert(jdata jsonb)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _id bigint;
	_day smallint = (jdata->>'day')::smallint;
	_month smallint = (jdata->>'month')::smallint;
	_label text = (jdata->>'label')::text;
	_holiday_id bigint = (jdata->>'holiday_id')::bigint;
	_is_day_off boolean = (jdata->>'is_day_off')::boolean;
	_end_day smallint = (jdata->>'end_day')::smallint;
BEGIN
    -- Валидация даты
    IF _day < 1 OR _day > 31 THEN
        RAISE EXCEPTION 'Invalid day: %', _day;
    END IF;

    IF _month < 1 OR _month > 12 THEN
        RAISE EXCEPTION 'Invalid month: %', _month;
    END IF;

    -- INSERT
    IF _holiday_id IS NULL THEN

        -- Проверка на дубликат
        IF EXISTS (
            SELECT 1
            FROM commons.work_calendar_fixed_holiday
            WHERE day = _day AND month = _month
        ) THEN
            RAISE EXCEPTION
                'Holiday for day % and month % already exists',
                _day, _month;
        END IF;

        INSERT INTO commons.work_calendar_fixed_holiday
            (day, month, label, is_day_off, end_day)
        VALUES
            (_day, _month, _label, _is_day_off, _end_day)
        RETURNING id INTO _id;

        RETURN json_build_object(
            'status', 'inserted',
            'id', _id
        );

    -- UPDATE
    ELSE
        UPDATE commons.work_calendar_fixed_holiday
        SET
            day = _day,
            month = _month,
            label = _label,
            is_day_off = _is_day_off,
            updated_at = now(),
			end_day = _end_day
        WHERE id = _holiday_id
        RETURNING id INTO _id;

        IF _id IS NULL THEN
            RAISE EXCEPTION
                'Holiday with id % not found',
                _holiday_id;
        END IF;

        RETURN json_build_object(
            'status', 'updated',
            'id', _id
        );
    END IF;
END;
$BODY$;