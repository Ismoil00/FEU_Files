


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
			bank_details, 
			created
		) values (
			(jdata->>'name')::text,
			_itn,
			(jdata->>'details')::jsonb,
			(jdata->>'doc_id')::bigint,
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




select * FROM accounting.counterparty ac


CREATE OR REPLACE FUNCTION accounting.get_counterparties(
	_name text DEFAULT NULL::text,
	_itn text DEFAULT NULL::text,
	_bank_name text DEFAULT NULL::text,
	_bank_account text DEFAULT NULL::text,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
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
			'bank_details', ac.bank_details,
			'contracts', c.contracts,
			'created_date', (ac.created->>'date')::date
		) aggregated
		FROM accounting.counterparty ac
		left join (
			select 
				counterparty_id,
				jsonb_agg(
					jsonb_build_object(
						'key', id,
						'id', id,
						'contract', contract,
						'created_date', created_date
					) order by id desc
				) as contracts
			from commons.counterparty_contracts
			group by counterparty_id
		) c on ac.id = c.counterparty_id
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





drop function accounting.get_counterparty_contracts_by_id

/* DELETED */
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
			bank_details,
			(created->>'date')::date created_date
		from accounting.counterparty
	) ac;
	
	return _result;
end;
$BODY$;


/*
	CONTRACTS
*/

create table if not exists commons.counterparty_contracts (
	id bigserial primary key,
	contract text not null,
	counterparty_id bigint references accounting.counterparty (id),
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT localtimestamp(0),
    updated_date TIMESTAMP WITHOUT TIME ZONE
);



select * from commons.counterparty_contracts



select commons.upsert_counterparty_contract (
	149,
	'TEXT 3',
	6
);





CREATE OR REPLACE FUNCTION commons.upsert_counterparty_contract(
	_counterparty_id bigint,
	_contract text,
	contract_id bigint default null
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_id BIGINT = contract_id;
BEGIN

	-- upsert!
	if _id is null THEN
		insert into commons.counterparty_contracts (
			contract,
			counterparty_id
		) values (
			_contract,
			_counterparty_id
		) returning id into _id;
	ELSE
		update commons.counterparty_contracts SET
			contract = _contract,
			updated_date = localtimestamp(0)
		where id = _id;
	end if;

	return jsonb_build_object (
		'status', 200,
		'msg', case when _id is not null then 'updated' else 'created' end,
		'id', _id
	);
end;
$BODY$;






select commons.get_counterparty_contract(149)

CREATE OR REPLACE FUNCTION commons.get_counterparty_contract(
		_counterparty_id bigint
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
			id as key, 
			id, 
			contract, 
			counterparty_id, 
			created_date
		from commons.counterparty_contracts
		where counterparty_id = _counterparty_id
	) ac;
	
	return _result;
end;
$BODY$;




