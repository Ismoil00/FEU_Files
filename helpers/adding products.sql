select * from commons.tmp111 (
	'
	'
);

select * from commons.nomenclature
where low_cost;

CREATE OR REPLACE FUNCTION commons.tmp111(
	jdata jsonb)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_row jsonb;
BEGIN

	for _row in select * from jsonb_array_elements(jdata) loop
		-- raise notice 'row %', _row;

		INSERT into commons.nomenclature (
			name,
			unit_id,
			low_cost
		) values (
			(_row->>'name')::jsonb,
			(_row->>'unit_id')::bigint,
			(_row->>'low_cost')::boolean
		);
	end loop;

END;
$BODY$;