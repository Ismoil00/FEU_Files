select * from commons.accouting_bank_accounts;

select commons.get_accouting_bank_accounts()

CREATE OR REPLACE FUNCTION commons.get_accouting_bank_accounts(
	)
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
			id, 
			bank_account,
			bank_name,
			currency,
			bik,
			corr_account,
			"createdAt"
		from commons.accouting_bank_accounts
		where disabled is not true
	)	dt into _result;
	
	return _result;
END;
$BODY$;


CREATE OR REPLACE FUNCTION commons.upsert_accouting_bank_accounts(
	request json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _id bigint = (request->>'id')::bigint;
	_bank_account text = (request->>'bank_account')::text;
	_bank_name text = (request->>'bank_name')::text;
	_currency text = (request->>'currency')::text;
	_bik text = (request->>'bik')::text;
	_corr_account text = (request->>'corr_account')::text;
	-- _disabled bool = (request->>'disabled')::bool;
BEGIN

	-- we check for dublicates:
	if exists (
		select 1 from commons.accouting_bank_accounts aba
	    where (
	        (id <> _id and aba.bank_account = _bank_account and aba.bank_name = _bank_name)
	        or 
	        (_id is null and aba.bank_account = _bank_account and aba.bank_name = _bank_name)
	    )
	) THEN
		RAISE EXCEPTION 'Такой Аккаунт уже сушествует в таком Банке!' USING ERRCODE = 'P0001';
	end if;

	if _id is not null then
	-- update:
		update commons.accouting_bank_accounts
		set
			bank_account = _bank_account,
			bank_name = _bank_name,
			currency = _currency,
			bik = _bik,
			corr_account = _corr_account,
			-- disabled = _disabled,
		 	"updatedAt" = localtimestamp(0)
		where id = _id;
		
		return json_build_object('status', 200, 'msg', 'UPDATED!')::json;
	else
	-- create:
		insert into commons.accouting_bank_accounts (
			bank_account,
			bank_name,
			currency,
			bik,
			corr_account
		) values (
			_bank_account,
			_bank_name,
			_currency,
			_bik,
			_corr_account
		);

		return json_build_object('status', 200, 'msg', 'CREATED!')::json;
	end if;

END;
$BODY$;