


	select * from commons.counterparty_contracts
	
	select * from accounting.counterparty
	
	select * from accounting.manual_operations





select accounting.download_manual_operation_file (1)





CREATE OR REPLACE FUNCTION accounting.download_manual_operation_file(_id bigint)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result jsonb;
begin

	/* VALIDATION */
	if not exists (
		select 1
		from accounting.manual_operations
		where operation_number = _id
	) then 
		raise exception 'Файл не найден';
	end if;

	/* QUERY */
	with main_parent as (
		select DISTINCT ON (operation_number)
			(
				select department->>'tj' 
				from commons.department
				where id = 12
			) as organization,
			operation_number,
			(created->>'date')::date as created_date,
			content
		from accounting.manual_operations
		where operation_number = _id
		ORDER BY operation_number
	),
	main_child as (
	  SELECT
	    operation_number,
	    jsonb_agg(
	      jsonb_build_object(
	        'key', rn,
	        'debit', debit,
	        'credit', credit,
	        'quantity', quantity,
	        'amount', amount,
	        'description', description,
	        'total_amount', round(amount * quantity, 2),
	        'subconto', subconto
	      )
	      ORDER BY id
	    ) AS table_data
	  FROM (
	    SELECT
	      mo.*,
	      row_number() OVER (PARTITION BY operation_number ORDER BY mo.id) AS rn,
	      CASE
	        WHEN mo.subconto_type IS NULL THEN mo.subconto_name
	        WHEN mo.subconto_type = 'counterparties' THEN (
	          SELECT concat_ws(', ', c.name, cc.contract)
	          FROM commons.counterparty_contracts cc
	          JOIN accounting.counterparty c
	            ON cc.counterparty_id = c.id
	          WHERE cc.id = coalesce(mo.contract_id, mo.subconto_name::bigint)
	        )
	        WHEN mo.subconto_type = 'products' THEN (
	          SELECT name->>'tj'
	          FROM commons.nomenclature
	          WHERE id = mo.subconto_name::bigint
	        )
	        WHEN mo.subconto_type = 'services' THEN (
	          SELECT name->>'tj'
	          FROM commons.services_nomenclature
	          WHERE id = mo.subconto_name::bigint
	        )
	      END AS subconto
	    FROM accounting.manual_operations mo
	    WHERE operation_number = _id
	  ) x
	  GROUP BY operation_number
	),
	main as (
		SELECT 
		    p.*,
		    c.table_data
		FROM main_parent p
		JOIN main_child c 
		USING (operation_number)
	)
	select jsonb_build_object(
		'operation_number', operation_number,
		'organization', organization,
		'created_date', created_date,
		'content', content,
		'table_data', table_data
	) into _result from main;

	/* RETURN */
	return _result;
end;
$BODY$;













