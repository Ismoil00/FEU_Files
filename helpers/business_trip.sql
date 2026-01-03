


/*
	UPSERT
*/


SELECT * FROM hr.trip_type;

SELECT * FROM commons.local_trip_prices;

select * from commons.trip_daily_pay;

select * from  hr.business_trip;


select hr.upsert_business_trip(
	'
		{
			"user_id": "b325f38d-f247-462e-adf1-40bce7806302",
			"destination": 10,
			"abroad_trip": true,
			"created_date": "2025-12-26",
			"table_rows": [
				{
					"personnel_number": "E-44444445555",
					"department_id": 1,
					"staff_id": 3667,
					"date_from": "2025-12-10",
					"date_to": "2025-12-31",
					"purpose": "",
					"fare": "",
					"house_rent": 1119153.2
				}
			]
		}
	'
);


CREATE OR REPLACE FUNCTION hr.upsert_business_trip(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_operation_number bigint = (jdata->>'operation_number')::bigint;
	_abroad_trip boolean = (jdata->>'abroad_trip')::boolean;
	_destination bigint = (jdata->>'destination')::bigint;
	_visiting_organization text = (jdata->>'visiting_organization')::text;
	_description text = (jdata->>'description')::text;
	_doc_id bigint = (jdata->>'doc_id')::bigint;
	_user_id text = jdata->>'user_id';
	_created_date date = (jdata->>'created_date')::date;
	
	_staff jsonb;
	_date_from date;
	_date_to date;
	_id bigint;
	isUpdate boolean = false;
BEGIN

	/* GENERATING NEW OPERATION-NUMBER INSERTION */
	if _operation_number is null then
		SELECT coalesce(max(sub.operation_number), 0) + 1
		into _operation_number from (
			SELECT operation_number
	    	FROM hr.business_trip
	    	GROUP BY operation_number
	    	ORDER BY operation_number DESC
	    	LIMIT 1
		) sub;
	end if;

	FOR _staff IN SELECT * FROM jsonb_array_elements((jdata->>'table_rows')::jsonb) LOOP
		_date_from = (_staff->>'date_from')::date;
		_date_to = (_staff->>'date_to')::date;
		_id = (_staff->>'id')::bigint;
		
	   	if _id is null then
		   	insert into hr.business_trip (
				operation_number,
				abroad_trip,
				destination,
				visiting_organization,
				description,
				doc_id,
				
				personnel_number,
				department_id,
				staff_id,
				date_from,
				date_to,
				purpose,
				fare,
				house_rent,
				
				created
		   	) values (
			   	_operation_number,
				_abroad_trip,
				_destination,
				_visiting_organization,
				_description,
				_doc_id,

				(_staff->>'personnel_number')::text,
				(_staff->>'department_id')::integer,
				(_staff->>'staff_id')::bigint,
				_date_from,
				_date_to,
				(_staff->>'purpose')::text,
				(_staff->>'fare')::numeric,
				(_staff->>'house_rent')::numeric,

				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
		   	);
		else
			isUpdate = true;

			update hr.business_trip bt SET
				operation_number = _operation_number,
				abroad_trip = _abroad_trip,
				destination = _destination,
				visiting_organization = _visiting_organization,
				description = _description,
				doc_id = _doc_id,
				
				personnel_number = (_staff->>'personnel_number')::text,
				department_id = (_staff->>'department_id')::integer,
				staff_id = (_staff->>'staff_id')::bigint,
				date_from = _date_from,
				date_to = _date_to,
				purpose = (_staff->>'purpose')::text,
				fare = (_staff->>'fare')::numeric,
				house_rent = (_staff->>'house_rent')::numeric,

				created = CASE
				    WHEN _created_date IS NOT NULL THEN 
						jsonb_set(
				             bt.created,
				             '{date}',
				             to_jsonb(_created_date)
				         )
				-- 		-- jsonb_build_object(
				-- 		--     'date', _created_date::timestamp,
				-- 		-- 	'user_id', bt.created->>'user_id'
				-- 		-- )
				    ELSE bt.created
				END,
				updated = jsonb_build_object(
					'date', LOCALTIMESTAMP(0),
					'user_id', _user_id
				)
			where id = _id;
		end if;
	END LOOP;

	return json_build_object(
		'msg', case when isUpdate then 'updated' else 'created' end,
		'operation_number', _operation_number,
		'status', 200
	);
END;
$BODY$;







/*
	STAFF BUSINESS TRIP
*/


select hr.get_business_trip (
	false,
	null,
	null,
	null,
	null
);

select * from  hr.business_trip;

CREATE OR REPLACE FUNCTION hr.get_business_trip(
	_abroad_trip boolean,
	_destination bigint default null,
	_visiting_organization text default null,
	_date_from text default null,
	_date_to text default null,
	_limit integer default 100,
	_offset integer default 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result json;
BEGIN

	WITH bt_parent AS (
	    SELECT DISTINCT ON (operation_number)
	        operation_number,
	        doc_id,
	        abroad_trip,
	        destination,
	        visiting_organization,
	        description,
	        (created->>'date')::date AS created_date
	    FROM hr.business_trip
	    WHERE abroad_trip = _abroad_trip
		and (_destination is null or _destination = destination)
		and (_visiting_organization is null or _visiting_organization = visiting_organization)
		and (_date_from is null or _date_from::date <= date_from)
		and (_date_to is null or _date_to::date >= date_to)
	    ORDER BY operation_number, id
	), bt_table_rows AS (
	    SELECT 
	        operation_number,
	        jsonb_agg(
	            jsonb_build_object(
					'key', rn,
	                'id', id,
	                'staff_id', staff_id,
	                'department_id', department_id,
	                'date_from', date_from,
	                'date_to', date_to,
	                'personnel_number', personnel_number,
	                'purpose', purpose,
	                'fare', fare,
	                'house_rent', house_rent
	            ) ORDER BY id desc
	        ) AS table_rows
	    FROM (
			select
				*,
				row_number() OVER (
	                PARTITION BY operation_number
	                ORDER BY id DESC
	            ) AS rn
			from hr.business_trip
			WHERE abroad_trip = _abroad_trip
			and (_destination is null or _destination = destination)
			and (_visiting_organization is null or _visiting_organization = visiting_organization)
			and (_date_from is null or _date_from::date <= date_from)
			and (_date_to is null or _date_to::date >= date_to)
		)
	    GROUP BY operation_number
	),
	joined as (
		SELECT *
		FROM bt_parent
		JOIN bt_table_rows 
		USING (operation_number)
	),
	total_count as (
		select count(*) as total from joined
	),
	bt as (
		SELECT jsonb_build_object(
			'key', row_number() OVER (order by created_date desc),
			'operation_number', operation_number,
	        'doc_id', doc_id,
	        'abroad_trip', abroad_trip,
	        'destination', destination,
	        'visiting_organization', visiting_organization,
	        'description', description,
	        'created_date', created_date,
			'table_rows', table_rows
		) as paginated
		FROM joined
		order by created_date desc
		limit _limit offset _offset
	)
	select jsonb_build_object(
		'status', 200,
		'results', jsonb_agg(bt.paginated),
		'total', (select total from total_count)
	) into _result from bt;

	return _result;
END
$BODY$;




select hr.get_business_trip_by_operation_number(1)

/*
	STAFF BUSINESS TRIP BY OPERATION NUMBER
*/

CREATE OR REPLACE FUNCTION hr.get_business_trip_by_operation_number(
	_operation_number bigint
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result json;
BEGIN

	WITH bt_parent AS (
	    SELECT DISTINCT ON (operation_number)
	        operation_number,
	        doc_id,
	        abroad_trip,
	        destination,
	        visiting_organization,
	        description,
	        (created->>'date')::date AS created_date
	    FROM hr.business_trip
	    WHERE operation_number = _operation_number
	    ORDER BY operation_number, id
	), bt_table_rows AS (
	    SELECT 
	        operation_number,
	        jsonb_agg(
	            jsonb_build_object(
					'key', rn,
	                'id', id,
	                'staff_id', staff_id,
	                'department_id', department_id,
	                'date_from', date_from,
	                'date_to', date_to,
	                'personnel_number', personnel_number,
	                'purpose', purpose,
	                'fare', fare,
	                'house_rent', house_rent
	            ) ORDER BY id desc
	        ) AS table_rows
	    FROM (
			select
				*,
				row_number() OVER (
	                PARTITION BY operation_number
	                ORDER BY id DESC
	            ) AS rn
			from hr.business_trip
		)
	    GROUP BY operation_number
	),
	bt as (
		SELECT jsonb_build_object(
			'operation_number', btp.operation_number,
	        'doc_id', btp.doc_id,
	        'abroad_trip', btp.abroad_trip,
	        'destination', btp.destination,
	        'visiting_organization', btp.visiting_organization,
	        'description', btp.description,
	        'created_date', btp.created_date,
			'table_rows', bttr.table_rows
		) as paginated
		FROM bt_parent btp
		JOIN bt_table_rows bttr 
		USING (operation_number)
	)
	select bt.paginated into _result from bt;

	return json_build_object(
		'result', _result,
		'status', 200
	);
END
$BODY$;




/*
	LAST NUMBER
*/
CREATE OR REPLACE FUNCTION hr.get_business_trip_last_operation_number()
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_operation_number bigint;
	BEGIN

		select operation_number
		into _operation_number
		from hr.business_trip
		group by operation_number
		order by operation_number desc
		limit 1;
		
		return _operation_number;
	end;
$BODY$;






/*
	CERTIFICATE
*/
CREATE OR REPLACE FUNCTION hr.download_business_trip_certificate(
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









