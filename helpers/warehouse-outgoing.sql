CREATE OR REPLACE FUNCTION accounting.upsert_warehouse_outgoing(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		/* general columns */
		_unique_outgoing_number bigint = (jdata->>'unique_outgoing_number')::bigint;
		_order_number bigint = (jdata->>'order_number')::bigint;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_department_id integer = (jdata->>'department_id')::integer;
		_doc_id bigint = (jdata->>'doc_id')::bigint;
		_description text = (jdata->>'description')::text;
		_issued_date date = (jdata->>'issued_date')::date;
		_temp_staff_fullname text = (jdata->>'temp_staff_fullname')::text;
		_temp_staff_rank text = (jdata->>'temp_staff_rank')::text;

		_product jsonb;
		newQuantity numeric;
		leftQuantity numeric;
		isUpdate boolean = false;
		_updated jsonb = jsonb_build_object(
			'user_id', (jdata->>'user_id')::uuid,
			'date', LOCALTIMESTAMP(0)
		);
		_name_id BIGINT;
		_import_id BIGINT;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.warehouse_outgoing
			where unique_outgoing_number = _unique_outgoing_number 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;

		/* GENERATING NEW UNIQUE-OUTGOING-NUMBER and ORDER-NUMBER for INSERTION */
		if _unique_outgoing_number is null then
			SELECT coalesce(max(sub.unique_outgoing_number), 0) + 1
			into _unique_outgoing_number from (
				SELECT unique_outgoing_number
	        	FROM accounting.warehouse_outgoing
	        	GROUP BY unique_outgoing_number
	        	ORDER BY unique_outgoing_number DESC
	        	LIMIT 1
			) sub;

			SELECT coalesce(max(sub.order_number), 0) + 1
			into _order_number from (
				SELECT order_number
	        	FROM accounting.warehouse_outgoing
				where financing = _financing
				and extract(year from (created->>'date')::date) = extract(
					year from coalesce(_issued_date, current_date)
				)
	        	GROUP BY order_number
	        	ORDER BY order_number DESC
	        	LIMIT 1
			) sub;
		end if;

		-- when date is changed in the scale of year, then we have to change the order-nunber as well;
		if exists (
			select 1 from accounting.warehouse_outgoing
			where unique_outgoing_number = _unique_outgoing_number
			and extract(year from (created->>'date')::date) <> extract(year from _issued_date) 
		) and then
			SELECT coalesce(max(sub.order_number), 0) + 1
			into _order_number from (
				SELECT order_number
	        	FROM accounting.warehouse_outgoing
				where financing = _financing
				and extract(year from (created->>'date')::date) = extract(
					year from coalesce(_issued_date, current_date)
				)
	        	GROUP BY order_number
	        	ORDER BY order_number DESC
	        	LIMIT 1
			) sub;
		end if;

		FOR _product IN SELECT * FROM json_array_elements(jdata->'products') LOOP
			if (_product->>'id')::bigint is null then
				/* PRODUCT AMOUNT CHECK VALIDATION */
				perform accounting.warehouse_product_amount_validation(
					(_product->>'name_id')::bigint,
					(_product->>'import_id')::bigint,
					(_product->>'quantity')::numeric
				);

				/* CURR-TABLE INSERTION */
				insert into accounting.warehouse_outgoing (
					unique_outgoing_number,
					order_number,
					financing,
					staff_id,
					department_id,
					doc_id,
					description,
					issued_date,
					temp_staff_fullname,
					temp_staff_rank,
					
					name_id,
					quantity,
					unit_price,
					import_id,
					created
				) values (
					_unique_outgoing_number,
					_order_number,
					_financing,
					_staff_id,
					_department_id,
					_doc_id,
					_description,
					coalesce(_issued_date, localtimestamp(0)),
					_temp_staff_fullname,
					_temp_staff_rank,

					(_product->>'name_id')::bigint,
					(_product->>'quantity')::numeric,
					(_product->>'unit_price')::numeric,
					(_product->>'import_id')::bigint,
					jsonb_build_object(
						'user_id', jdata->>'user_id',
						'date', coalesce(_issued_date, LOCALTIMESTAMP(0))
					)
				);
			else
				/* FETCH VALUES FOR DRY */
				isUpdate = true;
				newQuantity = (_product->>'quantity')::numeric;
				_name_id = (_product->>'name_id')::bigint;
				_import_id = (_product->>'import_id')::bigint;
				select newQuantity - o.quantity 
				into leftQuantity
				from accounting.warehouse_outgoing o
				where id = (_product->>'id')::bigint;

				/* WE ONLY CHECK IF NEW QUANTITY IS ADDED */
				if leftQuantity >= 0 then
					perform accounting.warehouse_product_amount_validation(
						_name_id,
						_import_id,
						leftQuantity
					);
				elseif leftQuantity < 0 then
					/* RESET CLOSE FOR DECREASED PRODUCT IN BOTH TABLES */
					update accounting.warehouse_incoming SET
						closed = false,
						updated = _updated
					where name_id = _name_id 
					and unique_import_number = _import_id;
						
					update accounting.warehouse_outgoing SET
						closed = false,
						updated = _updated
					where name_id = _name_id 
					and import_id = _import_id;
				end if;

				/* CURR-TABLE UPDATE */
				update accounting.warehouse_outgoing wo SET
					order_number = _order_number,
					financing = _financing,
					staff_id = _staff_id,
					department_id = _department_id,
					description = _description,
					issued_date = coalesce(_issued_date, wo.issued_date),
					created = CASE
    				    WHEN _issued_date IS NOT NULL
    				    THEN jsonb_set(
    				             wo.created,
    				             '{date}',
    				             to_jsonb(_issued_date)
    				         )
    				    ELSE wo.created
    				END,
					temp_staff_fullname = _temp_staff_fullname,
					temp_staff_rank = _temp_staff_rank,

					name_id = _name_id,
					quantity = (_product->>'quantity')::numeric,
					unit_price = (_product->>'unit_price')::numeric,
					import_id = _import_id,
					updated = _updated
				where id = (_product->>'id')::bigint;
			end if;		
    	END LOOP;

		/* RIST ROUTING - DONE INSPECTORS */
		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _unique_outgoing_number)
		);

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'unique_outgoing_number', _unique_outgoing_number,
			'order_number', _order_number,
			'status', 200
		);
	end;
$BODY$;










CREATE OR REPLACE FUNCTION accounting.warehouse_product_amount_validation(
	_name_id bigint,
	_import_id bigint,
	_amount numeric)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	lef_quantity numeric;
	_product_name text;
begin

	select
	    coalesce(i.quantity, 0) - coalesce(e.quantity, 0)
	into lef_quantity
	from (
	    select sum(quantity) as quantity
	    from accounting.warehouse_incoming
	    where name_id = _name_id
	      and closed is not true
	      and unique_import_number = _import_id
	      and status = 'approved'
	) i
	cross join (
	    select sum(quantity) as quantity
	    from accounting.warehouse_outgoing
	    where name_id = _name_id
	      and closed is not true
	      and import_id = _import_id
	      -- and status = 'approved'
	) e;

	if lef_quantity < _amount then
		select name->>'ru'
		into _product_name
		from commons.nomenclature
		where id = _name_id;
		
	    raise exception 'Запрошенное количество % превышает доступный % для данного товара "%"', _amount, lef_quantity, _product_name;
	end if;
end;
$BODY$;