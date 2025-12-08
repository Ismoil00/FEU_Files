---- removed bank_details;
---- add new fields: 

truncate accounting.counterparty CASCADE

select * from accounting.counterparty

where name = 'TEST';

select accounting.get_all_counterparty()

CREATE OR REPLACE FUNCTION accounting.get_all_counterparty(
	)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result json;
BEGIN

	SELECT json_agg(row_to_json(ac))
	into _result
	FROM (
		select 
			id, 
			name, 
			itn, 
			details,
			doc_id,
			contracts,
			bank_details,
			(created->>'date')::date created_date
		from accounting.counterparty
	) ac;
	
	return _result;
end;
$BODY$;

-----------------------------------------------------------


CREATE OR REPLACE FUNCTION accounting.get_counterparties(
	_name text DEFAULT NULL::text,
	_itn text DEFAULT NULL::text,
	_bank_name text DEFAULT NULL::text,
	_bank_account text DEFAULT NULL::text,
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
	_total integer;
BEGIN

	SELECT count(*) INTO _total
	FROM accounting.counterparty ac
	WHERE (_name IS NULL OR ac.name = _name) 
	AND (_itn IS NULL OR ac.itn = _itn)
	AND (
		_bank_name IS NULL 
		OR EXISTS (
			SELECT 1 
			FROM jsonb_array_elements(ac.bank_details) AS bank
			WHERE bank->>'bank_name' = _bank_name
		)
	)
	AND (
		_bank_account IS NULL 
		OR EXISTS (
			SELECT 1 
			FROM jsonb_array_elements(ac.bank_details) AS bank
			WHERE bank->>'bank_account' = _bank_account
		)
	);

	SELECT jsonb_agg(ac.aggregated) INTO _result
	FROM (
		SELECT jsonb_build_object(
			'id', ac.id,
			'name', ac.name,
			'itn', ac.itn,
			'details', ac.details,
			'doc_id', ac.doc_id,
			'contracts', ac.contracts,
			'bank_details', ac.bank_details,
			'created_date', (ac.created->>'date')::date
		) aggregated
		FROM accounting.counterparty ac
		WHERE (_name IS NULL OR ac.name = _name)
		AND (_itn IS NULL OR ac.itn = _itn)
		AND (
			_bank_name IS NULL 
			OR EXISTS (
				SELECT 1 
				FROM jsonb_array_elements(ac.bank_details) AS bank
				WHERE bank->>'bank_name' = _bank_name
			)
		)
		AND (
			_bank_account IS NULL 
			OR EXISTS (
				SELECT 1 
				FROM jsonb_array_elements(ac.bank_details) AS bank
				WHERE bank->>'bank_account' = _bank_account
			)
		)
		order by id
		LIMIT _limit OFFSET _offset
	) ac;

	RETURN jsonb_build_object('total', _total, 'results', _result);
END;
$BODY$;

--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION accounting.get_counterparty_contracts_by_id(
	_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE 
    _result json; 
BEGIN
    
	select contracts
	into _result
	from accounting.counterparty
	where id = _id;
    
    RETURN _result;
END;
$BODY$;

--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION accounting.upsert_counterparty(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_id BIGINT = (jdata->>'id')::bigint;
	_itn text = (jdata->>'itn')::text;
	isUpdated boolean = false;
BEGIN

	-- dublicate check!
	if exists (
		select 1 from accounting.counterparty
		where itn = _itn and _id is null
	) THEN
		RAISE EXCEPTION 'Вы не можете сохранить 2 контрагента с одним ИНН.' USING ERRCODE = 'P0001';
	end if;

	-- upsert!
	if _id is null THEN
		insert into accounting.counterparty (
			name, 
			itn, 
			details, 
			doc_id, 
			contracts,
			bank_details, 
			created
		) values (
			(jdata->>'name')::text,
			_itn,
			(jdata->>'details')::jsonb,
			(jdata->>'doc_id')::bigint,
			(jdata->>'contracts')::jsonb,
			(jdata->>'bank_details')::jsonb,
			jsonb_build_object(
				'user_id', (jdata->>'user_id')::uuid,
				'date', localtimestamp(0)
			)
		) RETURNING id into _id;
	ELSE
		isUpdated = true;
		update accounting.counterparty SET
			name = (jdata->>'name')::text, 
			itn = _itn, 
			details = (jdata->>'details')::jsonb,
			doc_id = (jdata->>'doc_id')::bigint, 
			contracts = (jdata->>'contracts')::jsonb,
			bank_details = (jdata->>'bank_details')::jsonb, 
			updated = 	jsonb_build_object(
				'user_id', (jdata->>'user_id')::uuid,
				'date', localtimestamp(0)
			)
		where id = _id;
	end if;

	return json_build_object(
		'msg', case when isUpdated then 'updated' else 'created' end,
		'status', 200,
		'id', _id
	);
end;
$BODY$;

--------------------------------------------------------------------