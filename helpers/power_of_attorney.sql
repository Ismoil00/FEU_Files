create table if not exists accounting.attorney_power (
	id bigserial primary key,
	staff_id bigint not null references hr.staff (id),
	staff_passport_data jsonb not null,
	counterparty_id bigint not null REFERENCES accounting.counterparty (id),
	contract text not null,
	expire_date TIMESTAMP(0) without time zone not null,
	bank_account_id bigint not null references commons.accouting_bank_accounts (id),
	table_date jsonb not null,
	created jsonb not null,
	updated jsonb
);


select * from accounting.attorney_power;


		select accounting.get_attorney_power (
			'budget',
			null,
			null,
			null,
			null,
			null,
			100,
			0
		)
		
CREATE OR REPLACE FUNCTION accounting.get_attorney_power(
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_staff_id bigint DEFAULT NULL::bigint,
	_counterparty_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
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
			from accounting.attorney_power
			where financing = _financing 
			and (_id is null or id = _id)		
			and (_date_from is null or _date_from::date <= (created->>'date')::date)
			and (_date_to is null or _date_to::date >= (created->>'date')::date)
			and (_staff_id is null or _staff_id = staff_id)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
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
				'staff_passport_data', staff_passport_data,
				'counterparty_id', counterparty_id,
				'contract', contract,
				'expire_date', expire_date,
				'bank_account_id', bank_account_id,
				'table_data', table_data,
				'created_date', (created->>'date')::date
			) aggregated
			from main
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






CREATE OR REPLACE FUNCTION accounting.upsert_attorney_power(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
	BEGIN

		if _id is null then
			insert into accounting.attorney_power (
				staff_id,
				staff_passport_data,
				counterparty_id,
				contract,
				expire_date,
				bank_account_id,
				table_data,
				financing,
				created
			) values (
				(jdata->>'staff_id')::bigint,
				(jdata->>'staff_passport_data')::jsonb,
				(jdata->>'counterparty_id')::bigint,
				(jdata->>'contract')::text,
				(jdata->>'expire_date')::date,
				(jdata->>'bank_account_id')::bigint,
				(jdata->>'table_data')::jsonb,
				_financing,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.attorney_power ap SET
				staff_id = (jdata->>'staff_id')::bigint,
				staff_passport_data = (jdata->>'staff_passport_data')::jsonb,
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				contract = (jdata->>'contract')::text,
				expire_date = (jdata->>'expire_date')::date,
				bank_account_id = (jdata->>'bank_account_id')::bigint,
				table_data = (jdata->>'table_data')::jsonb,
				financing = _financing,
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             ap.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE ap.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;

		return json_build_object(
			'msg', case when _id is null then 'created' else 'updated' end,
			'status', 200,
			'id', _id
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_attorney_power_id(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select id 
		into _last_id
		from accounting.attorney_power
		order by id desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;







