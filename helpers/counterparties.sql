


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
			(created->>'date')::date created_date
		from accounting.counterparty
	) ac;
	
	return _result;
end;
$BODY$;





select * from accounting.counterparty


	


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

	SELECT COUNT(DISTINCT ac.id)
	INTO _total
	FROM accounting.counterparty ac
	LEFT JOIN commons.counterparty_banks_details cbd
	    ON ac.id = cbd.counterparty_id
	WHERE (_name IS NULL OR ac.name = _name)
		AND (_itn IS NULL OR ac.itn = _itn)
		AND (_bank_name IS NULL OR cbd.name = _bank_name)
		AND (_bank_account IS NULL OR cbd.bank_account = _bank_account);

	SELECT jsonb_agg(ac.aggregated) INTO _result
		FROM (
		SELECT jsonb_build_object(
			'id', ac.id,
			'name', ac.name,
			'itn', ac.itn,
			'details', ac.details,
			'doc_id', ac.doc_id,
			'bank_details', cbd.bank_details,
			'contracts', c.contracts,
			'created_date', (ac.created->>'date')::date
		) aggregated
		FROM accounting.counterparty ac
		LEFT JOIN (
			select 
				counterparty_id,
				jsonb_agg(
					jsonb_build_object(
						'key', id,
						'id', id,
						'name', name,
						'bank_account', bank_account,
						'bik', bik,
						'corr_account', corr_account
					) order by created_date desc
				) as bank_details
			from commons.counterparty_banks_details
			where (_bank_name IS NULL or _bank_name = name)
			AND (_bank_account IS NULL or _bank_account = bank_account)
			group by counterparty_id
		) cbd on ac.id = cbd.counterparty_id
		left join (
			select 
				counterparty_id,
				jsonb_agg(
					jsonb_build_object(
						'key', id,
						'id', id,
						'contract', contract,
						'created_date', created_date
					) order by created_date desc
				) as contracts
			from commons.counterparty_contracts
			group by counterparty_id
		) c on ac.id = c.counterparty_id
		WHERE (_name IS NULL OR ac.name = _name)
		AND (_itn IS NULL OR ac.itn = _itn)
		order by ac.id
		LIMIT _limit OFFSET _offset
	) ac;

	RETURN jsonb_build_object('total', _total, 'results', _result);
END;
$BODY$;











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
			created
		) values (
			(jdata->>'name')::text,
			_itn,
			(jdata->>'details')::jsonb,
			(jdata->>'doc_id')::bigint,
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














