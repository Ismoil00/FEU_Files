create table if not exists accounting.manual_operations (
	id bigserial primary key,
	operation_number bigint not null,
	debit integer not null,
	credit integer not null,
	amount numeric not null,
	description text,
	created jsonb not null,
	updated jsonb
);


select * from accounting.manual_operations;


CREATE OR REPLACE FUNCTION accounting.upsert_manual_operations(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id uuid = (jdata->>'user_id')::uuid;
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		_created_date date = (jdata->>'created_date')::date;
		
		_product jsonb;
		isUpdate boolean = false;
	BEGIN

		-- loop
		FOR _product IN SELECT * FROM json_array_elements(jdata->'products') LOOP
			if (_product->>'id')::bigint is null then
				insert into accounting.manual_operations (
					operation_number,
					debit,
					credit,
					amount,
					description,
					created
				) values (
					_operation_number,
					(_product->>'debit')::integer,
					(_product->>'credit')::integer,
					(_product->>'amount')::numeric,
					(_product->>'description')::text,
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);
			else
				isUpdate = true;
				update accounting.manual_operations mo SET
					debit = (_product->>'debit')::integer,
					credit = (_product->>'credit')::integer,
					amount = (_product->>'amount')::numeric,
					description = (_product->>'description')::text,
					created = CASE
    				    WHEN _created_date IS NOT NULL
    				    THEN jsonb_set(
    				             mo.created,
    				             '{date}',
    				             to_jsonb(_created_date)
    				         )
    				    ELSE mo.created
    				END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', LOCALTIMESTAMP(0)
					)
				where id = (_product->>'id')::bigint;
			end if;
    	END LOOP;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'status', 200
		);
	end;
$BODY$;




CREATE OR REPLACE FUNCTION accounting.get_manual_operations(
	_operation_number bigint default null,
	_created_date text default null,
	_user_id text default null,
	_limit int default 100,
	_offset int default 100
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN

		select 
			mo.operation_number,
			us.fullname,
			mo.created->>'user_id' user_id,
			mo.created->>'date' date,

			jsonb_build_object(
				'id', mo.id,
				'debit', mo.debit,
				'credit', mo.credit,
				'amount', mo.amount,
				'description', mo.description
			)
		from accounting.manual_operationsc mo
		left join (
			select 
				a.id,
				concat_ws(' ', s.lastname, s.firstname, s.middlename) fullname
			from auth.user a
			left join hr.staff s
			on a.staff_id = s.id
		) us on (mo.created->>'user_id')::uuid = us.id
		order by mo.operation_number;

		return _result;
	end;
$BODY$;