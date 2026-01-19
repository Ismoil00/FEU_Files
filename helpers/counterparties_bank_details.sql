-- select * from accounting.counterparty;



create table if not exists commons.counterparty_banks_details (
	id bigserial primary key,
	counterparty_id bigint not null references accounting.counterparty(id),
	name text not null,
	bank_account text not null,
	bik text not null,
	corr_account text not null,
	disabled boolean default false,
	created_date timestamp(0) without time zone default localtimestamp(0),
	updated_date timestamp(0) without time zone
);


CREATE OR REPLACE FUNCTION commons.upsert_counterparty_banks_details(jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   _id bigint = (jdata->'id')::bigint;
   _result jsonb;
   is_update boolean;
   _counterparty_id bigint = (jdata->>'counterparty_id')::bigint;
   _name text = (jdata->>'name')::text;
BEGIN

	if exists (
		select 1 from commons.counterparty_banks_details
		where counterparty_id = _counterparty_id
		and name = _name
		and _id is null
	) then
		RAISE EXCEPTION 'У данного Контрагента есть банк, зарегистрированный с таким именим.' USING ERRCODE = 'P0001';
	end if;

   if _id is null then

		insert into commons.counterparty_banks_details (
			counterparty_id,
			name,
			bank_account,
			bik,
			corr_account
		) values (
			_counterparty_id,
			_name,
			jdata->>'bank_account',
			jdata->>'bik',
			jdata->>'corr_account'
		) returning id into _id;
		   
   else
		is_update = true;

		update commons.counterparty_banks_details set
			name = _name,
			bank_account = jdata->>'bank_account',
			bik = jdata->>'bik',
			corr_account = jdata->>'corr_account'
		where id = _id;
		
   end if;

   return jsonb_build_object(
		'id', _id,
		'status', 200,
		'msg', case when is_update is true then 'updated' else 'created' end
   );
END;
$BODY$;






CREATE OR REPLACE FUNCTION commons.get_counterparty_banks_details(_id bigint)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   _result jsonb;
BEGIN

	select jsonb_agg(
		jsonb_build_object(
			'key', id,
			'id', id,
			'name', name,
			'bank_account', bank_account,
			'bik', bik,
			'corr_account', corr_account
		) order by created_date
	) into _result
	from commons.counterparty_banks_details
	where counterparty_id = _id
	and disabled is not true;

   return jsonb_build_object(
		'status', 200,
		'results', _result
   );
END;
$BODY$;










