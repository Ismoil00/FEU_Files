


/*
	UPSERT
*/


SELECT * FROM hr.trip_type;



CREATE OR REPLACE FUNCTION hr.upsert_business_trip(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_id bigint := (jdata->>'id')::bigint; 
	_isLocal boolean := (jdata->>'isLocal')::boolean;
	_travel_to_id bigint := (jdata->>'travel_to_id')::bigint;
	_fare bigint := CASE WHEN (jdata->>'isLocal')::boolean THEN (
			SELECT fare 
			FROM commons.local_trip_prices
			WHERE table_id = (jdata->>'travel_to_id')::bigint)
		ELSE COALESCE((jdata->>'fare')::bigint, 0) END;
	_trip_type_id bigint;
	_start_date date := (jdata->>'start_date')::date;
	_end_date date := (jdata->>'end_date')::date;
	_daily_payment bigint = case when (jdata->>'isLocal')::boolean then (
			select amount
			from commons.trip_daily_pay
			where record_id = 1 and
			end_date is null
		) else round((
			select amount
			from commons.trip_daily_pay
			where record_id = 2 and
			end_date is null
		) * (
			select value from commons.usd_exchange
		)) end;
	_house_rent bigint = case when (jdata->>'isLocal')::boolean then (
			select amount
			from commons.trip_daily_pay
			where record_id = 3 and
			end_date is null
		) else COALESCE((jdata->>'house_rent')::bigint, 0) end;
	_payment bigint;
	_days int = ((jdata->>'end_date')::date - (jdata->>'start_date')::date)::int;
BEGIN

-- 	raise notice 'fare %', _fare;
-- 	raise notice 'house rent %', _house_rent;
	
	-- we check the OVERLAPS:
	if exists (
		select 1 from hr.business_trip
		where _id is null 
		and department_id = (jdata->>'department_id')::int
		and jobtitle_id = (jdata->>'jobtitle_id')::bigint
		and (
			(start_date <= _start_date and end_date >= _start_date) or
			(start_date <= _end_date and end_date >= _end_date) or 
			(start_date >= _start_date and end_date <= _end_date)
		)                  
	) THEN
		-- return json_build_object('status', 409, 'msg', 8);
		RAISE EXCEPTION '8' USING ERRCODE = 'P0001';

	end if;

   	if _id is null then
		-- first we insert data to trip type table:
		insert into hr.trip_type (
			isLocal, 
			local_travel,
			foreign_travel,
			fare,
			house_rent,
			daily_payment,
			return_ticket,
			otherside_pay,
			created_at
		) values (
			_isLocal,
			case when _isLocal then _travel_to_id else null end,
			case when _isLocal then null else _travel_to_id end,
			_fare,
			_house_rent,
			_daily_payment,
			(jdata->>'return_ticket')::boolean,
			(jdata->>'otherside_pay')::boolean,
			(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)
		) RETURNING id into _trip_type_id;

		-- then we calculate the payment amount:
		_payment = hr.calculate_business_trip_payment(
			(jdata->>'staff_id')::bigint,
			_isLocal,
			_fare,
			_house_rent,
			_daily_payment,
			(jdata->>'return_ticket')::boolean,
			(jdata->>'otherside_pay')::boolean,
			_days
		);

		-- then we insert data to business trip table:
		insert into hr.business_trip (
			staff_id,
			department_id,
			jobtitle_id,
			trip_type_id,
			doc_id,
			payment,
			start_date,
			end_date,
			created
		) values (
			(jdata->>'staff_id')::bigint,
			(jdata->>'department_id')::int,
			(jdata->>'jobtitle_id')::bigint,
			_trip_type_id,
			(jdata->>'doc_id')::bigint,
			_payment,
			_start_date,
			_end_date,
			jsonb_build_object(
				'user_id', (jdata->>'user_id')::uuid,
				'date', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)
			)	
		);
			
		return json_build_object('status', 200, 'msg', 1);
	else
		-- first we get the trip type table id:
		select trip_type_id into _trip_type_id from hr.business_trip where id = _id;

		-- first we update trip type table:
		update hr.trip_type SET
			isLocal = _isLocal, 
			local_travel = case when _isLocal then _travel_to_id else null end,
			foreign_travel = case when _isLocal then null else _travel_to_id end,
			fare = _fare,
			house_rent = _house_rent,
			daily_payment = _daily_payment,
			return_ticket = (jdata->>'return_ticket')::boolean,
			otherside_pay = (jdata->>'otherside_pay')::boolean,
			updated_at = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)
		where id = _trip_type_id;

		-- then we calculate the payment amount:
		_payment = hr.calculate_business_trip_payment(
			(jdata->>'staff_id')::bigint,
			_isLocal,
			_fare,
			_house_rent,
			_daily_payment,
			(jdata->>'return_ticket')::boolean,
			(jdata->>'otherside_pay')::boolean,
			_days
		);

		-- then we update the business trip table:
		update hr.business_trip SET
			department_id = (jdata->>'department_id')::int,
			jobtitle_id = (jdata->>'jobtitle_id')::bigint,
			doc_id = (jdata->>'doc_id')::bigint,
			payment = _payment,
			start_date = _start_date,
			end_date = _end_date,
			updated = jsonb_build_object(
				'user_id', (jdata->>'user_id')::uuid,
				'date', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)
			)
		where id = _id;
			
		return json_build_object('status', 200, 'msg', 2);
	end if;
	
END;
$BODY$;












/*
	CALCULATION
*/
CREATE OR REPLACE FUNCTION hr.calculate_business_trip_payment(
	_staff_id bigint,
	islocal boolean,
	fare bigint,
	house_rent bigint,
	_daily_payment bigint,
	_return_ticket boolean,
	_otherside_pays boolean,
	days integer)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	salary bigint = (payments.calculate_salary(_staff_id, days)->>'salary')::bigint;
	daily_payment bigint = coalesce(_daily_payment, 0);
	return_ticket int = case when _return_ticket then 2 else 1 end;
	otherside_pays int = case when _otherside_pays then 0 else 1 end;
	payment bigint;
BEGIN
	-- we calculate the payment:
	if isLocal THEN
		payment = salary + fare * return_ticket + (daily_payment + house_rent) * days;
	ELSE
		payment = salary + (fare + (daily_payment + house_rent) * days) * otherside_pays;		
	end if;

	-- raise notice 'fare %', fare;
	-- raise notice 'salary %', salary;
	-- raise notice 'daily paymant amount %', daily_payment;
	-- raise notice 'return_tickett %', return_ticket;
	-- raise notice 'otherside_pays %', otherside_pays;
	-- raise notice 'payment %', payment;

	return payment;
END;
$BODY$;










/*
	STAFF BUSINESS TRIP
*/
CREATE OR REPLACE FUNCTION hr.get_staff_business_trip(
	_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	rec record;
	_result json;
BEGIN

	-- check the record existance:
	if not exists (select 1 from hr.business_trip where id = _id) then
		return null::json;
	end if;

	WITH bt AS (
	    SELECT id, staff_id, trip_type_id, start_date, end_date
	    FROM hr.business_trip
	    WHERE id = _id
	),
	s AS (
	    SELECT id, firstname, lastname, middlename FROM hr.staff
	),
	tt as (
		select 
			id, isLocal, 
			local_travel, 
			foreign_travel, 
			fare, daily_payment, 
			house_rent,
			return_ticket
		from hr.trip_type
	),
	lt as (
		select table_id, intercity 
		from commons.local_trip_prices
		where end_date is null
	),
	ft as (
		select id, country
		from commons.countries
	)
	SELECT
		bt.id, 
		bt.staff_id, 
		format('%s %s %s', s.firstname, s.lastname, s.middlename) as fullname,
		case when tt.isLocal then lt.intercity else ft.country end as travels_to,
		bt.end_date - bt.start_date as days,
		case when tt.return_ticket then tt.fare/100*2 else tt.fare/100 end as fare,
		tt.daily_payment/100*((bt.end_date - bt.start_date)::int) as daily_payment,
		tt.house_rent/100*((bt.end_date - bt.start_date)::int) as house_rent,
		(tt.house_rent/100 + tt.daily_payment/100)*((bt.end_date - bt.start_date)::int) + (
			case when tt.return_ticket then tt.fare/100*2 else tt.fare/100 end
		) as total,
		bt.start_date,
		bt.end_date
	INTO rec
	FROM bt
	JOIN s ON bt.staff_id = s.id
	JOIN tt ON bt.trip_type_id = tt.id
	LEFT JOIN lt ON tt.local_travel = lt.table_id
	LEFT JOIN ft ON tt.foreign_travel = ft.id;

	return row_to_json(rec);
END
$BODY$;










/*
	DEPARTMENT BUSINESS TRIP
*/
CREATE OR REPLACE FUNCTION hr.get_department_business_trip (
	_department_id integer,
	_year integer)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result json;
BEGIN
	select json_agg(
	json_build_object(
		'id', bt.id,
		'staff_id', bt.staff_id,
		'firstname', s.firstname,
		'lastname', s.lastname,
		'middlename', s.middlename,
		'department_id', bt.department_id,
		'jobtitle_id', bt.jobtitle_id,
		'doc_id', bt.doc_id,
		'isLocal', tt.isLocal,
		'travel_to', case when tt.isLocal then ltp.intercity else con.country end,
		'travel_to_id', case when tt.isLocal then tt.local_travel else tt.foreign_travel end,
		'payment', bt.payment ,
		'daily_payment', tt.daily_payment,
		'fare', tt.fare,
		'house_rent', tt.house_rent,
		'return_ticket', tt.return_ticket,
		'otherside_pay', tt.otherside_pay,
		'start_date', bt.start_date,
		'end_date', bt.end_date
	)) into _result
	from (
		select 
			id, staff_id, 
			department_id,
			doc_id,
			jobtitle_id, 
			trip_type_id,
			payment,
			start_date,
			end_date
		from hr.business_trip
		where department_id = _department_id
		and (
			extract(year from start_date) = _year
			or
			extract(year from end_date) = _year
		)
	) bt
	inner join hr.trip_type tt
	on bt.trip_type_id = tt.id
	inner join (
		select id, firstname, lastname, middlename 
		from hr.staff where status = 1	
	) s
	on bt.staff_id = s.id
	left join (
		select id, country, cities from commons.countries
	) con 
	on tt.foreign_travel = con.id
	left join (
		select table_id, intercity, fare from commons.local_trip_prices
	) ltp
	on tt.local_travel = ltp.table_id;

	return _result;
END;
$BODY$;












/*
	ALL BUSINESS TRIP
*/
CREATE OR REPLACE FUNCTION accounting.get_staff_all_business_trip(
	_staff_id bigint,
	_department_id bigint,
	_year integer)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result json;
begin

	select json_agg(row_to_json(bt)) 
	into _result
	from hr.business_trip bt
	where staff_id = _staff_id 
	and department_id = _department_id
	and (
		extract(year from start_date) = coalesce(_year, extract(year from current_date))
		or
		extract(year from end_date) = coalesce(_year, extract(year from current_date))
	);

	return _result;
end;
$BODY$;









/*
	CERTIFICATE
*/
CREATE OR REPLACE FUNCTION accounting.download_business_trip_certificate(
	_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result json;
BEGIN

	-- check if the user exists:
	if not EXISTS (
		select 1 from hr.business_trip
		where id = _id
	) then
		-- return json_build_object('status', 400, 'msg', 'Корманд вуҷуд надорад!');
		RAISE EXCEPTION 'Корманд вуҷуд надорад!' USING ERRCODE = 'P0001';
	end if;
	
	-- check if the payment is valid;
	if EXISTS (
		select 1 from hr.business_trip
		where id = _id and (
			payment is null
			or payment < 1
		)
	) then
		-- return json_build_object('status', 400, 'msg', 'Пардохти сафари хизматӣ ҳисоб карда нашудааст!');
		RAISE EXCEPTION 'Пардохти сафари хизматӣ ҳисоб карда нашудааст!' USING ERRCODE = 'P0001';
	end if;

	-- check if the record is approved already;
	if exists (
		select 1 from hr.business_trip
		where id = _id and approved is false
	) then
		-- return json_build_object('status', 400, 'msg', 'Сабт тасдиқ карда нашудааст!');
		RAISE EXCEPTION 'Сабт тасдиқ карда нашудааст!' USING ERRCODE = 'P0001';
	end if;

	_result = hr.get_staff_business_trip(_id);
	return json_build_object('status', 200, 'data', _result);
END;	
$BODY$;













/*
	SAVE CERTIFICATE
*/
CREATE OR REPLACE FUNCTION accounting.save_business_trip_certificate(
	_id bigint,
	_doc_id bigint,
	user_id text)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
begin
	update hr.business_trip set
		cirtificate_doc_id = _doc_id,
		updated = jsonb_build_object(
			'user_id', user_id::uuid,
			'date', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)
		)
	where id = _id;

	return json_build_object('status', 200, 'msg', 'UPDATED!');
end;
$BODY$;