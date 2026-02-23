select * from commons.accouting_expenses;



create table if not exists commons.individuals (
	id bigserial primary key,
	firstname varchar(100) not null,
	lastname varchar(100) not null,
	middlename varchar(100),
	passport_data jsonb not null,
	details jsonb,
	disabled boolean default false,
	created_at timestamp(0) without time zone default localtimestamp(0),
	updated_at timestamp(0) without time zone
);



select * from commons.individuals


select commons.get_individuals()

CREATE OR REPLACE FUNCTION commons.get_individuals ()
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE _result json;
BEGIN

	select json_agg(row_to_json(dt))
	from (
		select
			row_number() over (order by created_at) as key, 
			id,
			firstname,
			lastname,
			middlename,
			passport_data,
			details,
			created_at
		from commons.individuals
		where disabled is not true
		order by created_at
	)	dt into _result;
	
	return _result;
END;
$BODY$;










CREATE OR REPLACE FUNCTION commons.upsert_individuals (jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _id bigint = (jdata->>'id')::bigint;
	_firstname varchar(100) = (jdata->>'firstname')::varchar(100);
	_lastname varchar(100) = (jdata->>'lastname')::varchar(100);
	_middlename varchar(100) = (jdata->>'middlename')::varchar(100);
	_passport_data jsonb = (jdata->>'passport_data')::jsonb;
	_details jsonb = (jdata->>'details')::jsonb;
BEGIN

	-- we check for dublicates:
	if exists (
		select 1 from commons.individuals i
		where 
		(
			i.id <> _id 
			and i.firstname = _firstname 
			and i.lastname = _lastname 
		) OR (
			_id IS NULL 
			and i.firstname = _firstname 
			and i.lastname = _lastname 
		)
	) THEN
		RAISE EXCEPTION 'Такой запись уже сушествует!' USING ERRCODE = 'P0001';
	end if;

	-- update:
	if _id is not null then
		update commons.individuals set
			firstname = _firstname,
			lastname = _lastname,
			middlename = _middlename,
			passport_data = _passport_data,
			details = _details,
		 	updated_at = localtimestamp(0)
		where id = _id;
		
		return json_build_object('status', 200, 'msg', 'UPDATED!')::json;
	else
	-- create:
		insert into commons.individuals (
			firstname,
			lastname,
			middlename,
			passport_data,
			details
		) values (
			_firstname,
			_lastname,
			_middlename,
			_passport_data,
			_details
		);

		return json_build_object('status', 200, 'msg', 'CREATED!')::json;
	end if;
END;
$BODY$;




