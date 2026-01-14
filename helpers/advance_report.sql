create type accounting.advance_type as enum ('advances', 'tmz', 'oplata', 'prochee');





select * from accounting.advance_report;










drop function accounting.insert_advance_report_ledgers;



create or replace function accounting.insert_advance_report_ledgers (
	_financing accounting.budget_distribution_type,
	_credit integer,
	_table_data jsonb,
	_staff_id bigint,
	_advance_type accounting.advance_type
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_updated_table_data jsonb = '[]'::jsonb;
		_entry jsonb;
		_ledger_id bigint;
		_debit integer;
		_amount numeric;
		_contract_id bigint;
	BEGIN

		FOR _entry IN SELECT * FROM json_array_elements(_table_data) LOOP
			_ledger_id = (_entry->>'ledger_id')::bigint;
			_debit = (_entry->>'debit')::integer;
			_amount = case when _advance_type = 'tmz' then 
				coalesce((_entry->>'quantity')::numeric, 0) * 
					coalesce((_entry->>'price')::numeric, 0)
				else (_entry->>'amount')::numeric end;
			_contract_id = (_entry->>'contract_id')::bigint;
			
			/* we fill ledger with the accountingentry */
			SELECT accounting.upsert_ledger(
				_financing,
				_debit,
				_credit,
				_amount,
				_contract_id,
				_staff_id,
				_ledger_id
			) INTO _ledger_id;

			-- we set new ledger_id
			_updated_table_data = _updated_table_data || jsonb_build_array(
				_entry || jsonb_build_object(
					'ledger_id', 
					_ledger_id
				)
			);
		END LOOP;

		RETURN _updated_table_data;
	END;
$BODY$;





		
CREATE OR REPLACE FUNCTION accounting.upsert_advance_report(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id uuid = (jdata->>'user_id')::uuid;
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		isUpdate boolean = false;
		
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_purpose text = jdata->>'purpose';
		_advance_type accounting.advance_type = (jdata->>'advance_type')::accounting.advance_type;
		_table_data jsonb = (jdata->>'table_data')::jsonb;
	BEGIN

		/* setting on ledgers the entries */
		select accounting.insert_advance_report_ledgers (
			_financing,
			114610,
			_table_data,
			_staff_id,
			_advance_type
		) into _table_data;

		-- insertion
		if _id is null then
			insert into accounting.advance_report (
				financing,
				staff_id,
				purpose,
				advance_type,
				table_data,
				created
			) values (
				_financing,
				_staff_id,
				_purpose,
				_advance_type,
				_table_data,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			);

		-- update
		else
			isUpdate = true;
			update accounting.advance_report ar SET
				financing = _financing,
				staff_id = _staff_id,
				purpose = _purpose,
				advance_type = _advance_type,
				table_data = _table_data,
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             ar.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE ar.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'status', 200
		);
	end;
$BODY$;














select accounting.get_advance_report();




CREATE OR REPLACE FUNCTION accounting.get_advance_report(
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_staff_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN

		with main as (
			select *
			from accounting.advance_report
			where financing = _financing 
			and (_id is null or id = _id)
			and (_date_from is null or _date_from::date <= (created->>'date')::date)
			and (_date_to is null or _date_to::date >= (created->>'date')::date)
			and (_staff_id is null or _staff_id = staff_id)
		),
		total_count as (
			select count(*) total from main
		),
		paginated as (
			select jsonb_build_object(
				'key', row_number() over(order by id),
				'id', id,
				'financing', financing,
				'staff_id', staff_id,
				'purpose', purpose,
				'advance_type', advance_type,
				'table_data', table_data,
				'created_date', (created->>'date')::date
			) aggregated
			from accounting.advance_report
			order by id limit _limit offset _offset
		)
		select jsonb_build_object(
			'results', jsonb_agg(p.aggregated),
			'total', (select total from total_count),
			'status', 200
		) into _result from paginated p;

		return _result;
	end;
$BODY$;
















select accounting.get_advance_report_report_number();

CREATE OR REPLACE FUNCTION accounting.get_advance_report_id(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select id into _last_id
		from accounting.advance_report
		order by id desc limit 1;
		
		return _last_id;
	end;
$BODY$;

