








CREATE OR REPLACE FUNCTION accounting.get_manual_operations(
	_financing accounting.budget_distribution_type,
	_operation_number bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_user_id text DEFAULT NULL::text,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
		_total_count integer;
	BEGIN

		-- total
		with filtered as (
			select distinct mo.operation_number
			from accounting.manual_operations mo
			left join (
				select 
					a.id,
					concat_ws(' ', s.lastname, s.firstname, s.middlename) fullname
				from auth.user a
				left join hr.staff s
				on a.staff_id = s.id
			) us on (mo.created->>'user_id')::uuid = us.id
			where financing = _financing
			and (_operation_number is null or _operation_number = mo.operation_number)
			and (_date_from is null or _date_from::date <= (mo.created->>'date')::date)
			and (_date_to is null or _date_to::date >= (mo.created->>'date')::date)
			and (_user_id is null or _user_id::uuid = us.id)
		)
		select count(*) into _total_count from filtered;

		-- main
		with main as (
			select
				mo.operation_number,
				us.fullname,
				mo.created->>'user_id' user_id,
				(mo.created->>'date')::date created_date,
				mo.amount,
				mo.financing,
				mo.subconto_type,
				jsonb_build_object(
					'id', mo.id,
					'subconto_name', mo.subconto_name,
					'debit', mo.debit,
					'credit', mo.credit,
					'amount', mo.amount,
					'description', mo.description
				) jsnb
			from accounting.manual_operations mo
			left join (
				select 
					a.id,
					concat_ws(' ', s.lastname, s.firstname, s.middlename) fullname
				from auth.user a
				left join hr.staff s
				on a.staff_id = s.id
			) us on (mo.created->>'user_id')::uuid = us.id
			where financing = _financing
			and (_operation_number is null or _operation_number = mo.operation_number)
			and (_date_from is null or _date_from::date <= (mo.created->>'date')::date)
			and (_date_to is null or _date_to::date >= (mo.created->>'date')::date)
			and (_user_id is null or _user_id::uuid = us.id)
			order by mo.operation_number
			limit _limit offset _offset
		),
		grouped as (
			select jsonb_build_object(
				'financing', min(financing),
				'operation_number', operation_number, 
				'fullname', min(fullname), 
				'user_id', min(user_id), 
				'created_date', min(created_date),
				'total_amount', sum(amount),
				'entries', jsonb_agg(jsnb)
			) aggregated from main
			group by operation_number
		) select jsonb_build_object(
			'results', jsonb_agg(g.aggregated),
			'total', _total_count,
			'status', 200
		) into _result from grouped g;

		return _result;
	end;
$BODY$;





select * from accounting.manual_operations







CREATE OR REPLACE FUNCTION accounting.upsert_manual_operations(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id uuid = (jdata->>'user_id')::uuid;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		_created_date date = (jdata->>'created_date')::date;
		_subconto_type text = jdata->>'subconto_type';
		
		_entry jsonb;
		isUpdate boolean = false;
	BEGIN

		/* GENERATING _operation_number for INSERTION */
		if _operation_number is null then
			SELECT coalesce(max(sub.operation_number), 0) + 1
			into _operation_number from (
				SELECT operation_number
	        	FROM accounting.manual_operations
	        	GROUP BY operation_number
	        	ORDER BY operation_number DESC
	        	LIMIT 1
			) sub;
		end if;

		-- loop
		FOR _entry IN SELECT * FROM jsonb_array_elements((jdata->>'entries')::jsonb) LOOP
			if (_entry->>'id')::bigint is null then
				insert into accounting.manual_operations (
					financing,
					operation_number,
					subconto_type,
					subconto_name,
					debit,
					credit,
					amount,
					description,
					created
				) values (
					_financing,
					_operation_number,
					_subconto_type,
					_entry->>'subconto_name',
					(_entry->>'debit')::integer,
					(_entry->>'credit')::integer,
					(_entry->>'amount')::numeric,
					_entry->>'description',
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);
			else
				isUpdate = true;
				update accounting.manual_operations mo SET
					financing = _financing,
					subconto_type = _subconto_type,
					subconto_name = _entry->>'subconto_name',
					debit = (_entry->>'debit')::integer,
					credit = (_entry->>'credit')::integer,
					amount = (_entry->>'amount')::numeric,
					description = _entry->>'description',
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
				where id = (_entry->>'id')::bigint;
			end if;
    	END LOOP;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'status', 200
		);
	end;
$BODY$;










