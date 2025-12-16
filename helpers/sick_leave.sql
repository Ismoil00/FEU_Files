





-- =====================================================
-- 2. UPSERT FUNCTION
-- =====================================================
CREATE OR REPLACE FUNCTION accounting.upsert_sick_leave(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
    DECLARE
        _user_id text = jdata->>'user_id';
        _created_date date = (jdata->>'created_date')::date;
        _id bigint = (jdata->>'id')::bigint;
        _paid_month integer = (jdata->>'paid_month')::integer;
        _date_from date = (jdata->>'date_from')::date;
        _date_to date = (jdata->>'date_to')::date;
        _department_id bigint = (jdata->>'department_id')::bigint;
        _staff_id bigint = (jdata->>'staff_id')::bigint;
        _sick_leave_type accounting.sick_leave_type_enum = (jdata->>'sick_leave_type')::accounting.sick_leave_type_enum;
        _days integer = (jdata->>'days')::integer;
        _percent integer = (jdata->>'percent')::integer;
        _salary_record jsonb = (jdata->>'salary_record')::jsonb;
        _comment text = jdata->>'comment';
        _daily_average_earning numeric(15, 2) = (jdata->>'daily_average_earning')::numeric(15, 2);
        _total_benefit_amount numeric(15, 2) = (jdata->>'total_benefit_amount')::numeric(15, 2);
        _overlapping_count integer;
    BEGIN
        -- Validate date range
        IF _date_to < _date_from THEN
            RAISE EXCEPTION 'Дата окончания не может быть раньше даты начала';
        END IF;

        -- Check for date overlap with existing records for the same staff
        -- Exclude the current record if updating
        SELECT COUNT(*)
        INTO _overlapping_count
        FROM accounting.sick_leave
        WHERE staff_id = _staff_id
            AND (_id IS NULL OR id != _id)
            AND (
                -- New range starts within existing range
                (_date_from >= date_from AND _date_from <= date_to)
                OR
                -- New range ends within existing range
                (_date_to >= date_from AND _date_to <= date_to)
                OR
                -- New range completely contains existing range
                (_date_from <= date_from AND _date_to >= date_to)
                OR
                -- Existing range completely contains new range
                (date_from <= _date_from AND date_to >= _date_to)
            );

        IF _overlapping_count > 0 THEN
            RAISE EXCEPTION 'Период больничного листа пересекается с существующей записью для данного сотрудника';
        END IF;

        -- UPSERT
        IF _id IS NULL THEN
            -- INSERT
            INSERT INTO accounting.sick_leave (
                paid_month,
                date_from,
                date_to,
                department_id,
                staff_id,
                sick_leave_type,
                days,
                percent,
                comment,
                daily_average_earning,
                total_benefit_amount,
				salary_record,
                created
            ) VALUES (
                _paid_month,
                _date_from,
                _date_to,
                _department_id,
                _staff_id,
                _sick_leave_type,
                _days,
                _percent,
                _comment,
                _daily_average_earning,
                _total_benefit_amount,
				_salary_record,
                jsonb_build_object(
                    'user_id', _user_id,
                    'date', COALESCE(_created_date::timestamp, LOCALTIMESTAMP(0))
                )
            ) RETURNING id INTO _id;
        ELSE
            -- UPDATE
            UPDATE accounting.sick_leave sl SET
                paid_month = _paid_month,
                date_from = _date_from,
                date_to = _date_to,
                department_id = _department_id,
                staff_id = _staff_id,
                sick_leave_type = _sick_leave_type,
                days = _days,
                percent = _percent,
                comment = _comment,
                daily_average_earning = _daily_average_earning,
                total_benefit_amount = _total_benefit_amount,
				salary_record = _salary_record,
                created = CASE
                    WHEN _created_date IS NOT NULL THEN
                        jsonb_set(
                            sl.created,
                            '{date}',
                            to_jsonb(_created_date::timestamp)
                        )
                    ELSE sl.created
                END,
                updated = jsonb_build_object(
                    'user_id', _user_id,
                    'date', LOCALTIMESTAMP(0)
                )
            WHERE id = _id;
        END IF;

        RETURN json_build_object(
            'msg', CASE WHEN _id IS NULL THEN 'created' ELSE 'updated' END,
            'status', 200,
            'id', _id
        );
    END;
$BODY$;






-- =====================================================
-- 3. GET/SEARCH FUNCTION
-- =====================================================
CREATE OR REPLACE FUNCTION accounting.get_sick_leave(
    _paid_month integer DEFAULT NULL::integer,
    _date_from text DEFAULT NULL::text,
    _date_to text DEFAULT NULL::text,
    _department_id bigint DEFAULT NULL::bigint,
    _staff_id bigint DEFAULT NULL::bigint,
    _sick_leave_type accounting.sick_leave_type_enum DEFAULT NULL::accounting.sick_leave_type_enum,
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
        _total_count bigint;
    BEGIN
        -- Get total count for pagination
        SELECT COUNT(*)
        INTO _total_count
        FROM accounting.sick_leave
        WHERE (_paid_month IS NULL OR paid_month = _paid_month)
            AND (_date_from IS NULL OR _date_from::date <= date_from)
            AND (_date_to IS NULL OR _date_to::date >= date_to)
            AND (_department_id IS NULL OR department_id = _department_id)
            AND (_staff_id IS NULL OR staff_id = _staff_id)
            AND (_sick_leave_type IS NULL OR sick_leave_type = _sick_leave_type);

        -- Get paginated results
        WITH main AS (
            SELECT jsonb_build_object(
                'key', row_number() OVER (ORDER BY id),
                'id', id,
                'paid_month', paid_month,
                'date_from', date_from,
                'date_to', date_to,
                'department_id', department_id,
                'staff_id', staff_id,
                'sick_leave_type', sick_leave_type,
                'days', days,
                'percent', percent,
                'comment', comment,
                'daily_average_earning', daily_average_earning,
                'total_benefit_amount', total_benefit_amount,
				'salary_record', salary_record,
                'created_date', (created->>'date')::date
            ) AS aggregated
            FROM accounting.sick_leave
            WHERE (_paid_month IS NULL OR paid_month = _paid_month)
                AND (_date_from IS NULL OR _date_from::date <= date_from)
                AND (_date_to IS NULL OR _date_to::date >= date_to)
                AND (_department_id IS NULL OR department_id = _department_id)
                AND (_staff_id IS NULL OR staff_id = _staff_id)
                AND (_sick_leave_type IS NULL OR sick_leave_type = _sick_leave_type)
            ORDER BY id DESC
            LIMIT _limit
            OFFSET _offset
        )
        SELECT jsonb_build_object(
            'results', jsonb_agg(m.aggregated),
            'total', _total_count
        )::json
        INTO _result
        FROM main m;

        RETURN _result;
    END;
$BODY$;


-- =====================================================
-- 4. GET LAST ID FUNCTION
-- =====================================================
CREATE OR REPLACE FUNCTION accounting.get_sick_leave_id()
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
    DECLARE
        _last_id bigint;
    BEGIN
	
        SELECT id
        INTO _last_id
        FROM accounting.sick_leave
        ORDER BY id DESC
        LIMIT 1;

        RETURN COALESCE(_last_id, 0);
    END;
$BODY$;

