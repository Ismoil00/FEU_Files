



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
		_basis jsonb = (jdata->>'basis')::jsonb;
		_created_date date = (jdata->>'created_date')::date;
		_through text = (jdata->>'through')::text;
		_comment text = jdata->>'comment';
		_storage_location_id bigint = (jdata->>'storage_location_id')::bigint;

		/* table records */
		_id bigint;
		_product jsonb;
		_quantity numeric;
		_old_quantity numeric;
		isUpdate boolean = false;
		_updated jsonb = jsonb_build_object(
			'user_id', (jdata->>'user_id')::uuid,
			'date', LOCALTIMESTAMP(0)
		);
		_name_id BIGINT;
		_import_id BIGINT;
		_unit_price NUMERIC;
		_debit integer;
		_credit integer;
		_ledger_id bigint;
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
					year from coalesce(_created_date, current_date)
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
			and extract(year from (created->>'date')::date) <> extract(year from _created_date) 
		) and then
			SELECT coalesce(max(sub.order_number), 0) + 1
			into _order_number from (
				SELECT order_number
	        	FROM accounting.warehouse_outgoing
				where financing = _financing
				and extract(year from (created->>'date')::date) = extract(
					year from coalesce(_created_date, current_date)
				)
	        	GROUP BY order_number
	        	ORDER BY order_number DESC
	        	LIMIT 1
			) sub;
		end if;

		FOR _product IN SELECT * FROM json_array_elements(jdata->'products') LOOP
			/* FETCHING VALUES FOR DRY */
			_id = (_product->>'id')::bigint;
			_quantity = (_product->>'quantity')::numeric;
			_name_id = (_product->>'name_id')::bigint;
			_import_id = (_product->>'import_id')::bigint;
			_unit_price = (_product->>'unit_price')::numeric;
			_debit = (_product->>'debit')::integer;
			_credit = (_product->>'credit')::integer;
			_ledger_id = (_product->>'ledger_id')::bigint;
				
			if _id is null then
				/* PRODUCT AMOUNT CHECK VALIDATION */
				perform accounting.warehouse_product_amount_validation(
					_storage_location_id,
					_name_id,
					_import_id,
					_quantity,
					_unit_price,
					_financing
				);

				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					null,
					null,
					null
				) INTO _ledger_id;

				/* CURR-TABLE INSERTION */
				insert into accounting.warehouse_outgoing (
					unique_outgoing_number,
					order_number,
					financing,
					staff_id,
					department_id,
					doc_id,
					basis,
					through,
					comment,
					storage_location_id,
					
					name_id,
					quantity,
					unit_price,
					import_id,
					credit,
					debit,
					ledger_id,
					created
				) values (
					_unique_outgoing_number,
					_order_number,
					_financing,
					_staff_id,
					_department_id,
					_doc_id,
					_basis,
					_through,
					_comment,
					_storage_location_id,

					_name_id,
					_quantity,
					_unit_price,
					_import_id,
					_credit,
					_debit,
					_ledger_id,
					jsonb_build_object(
						'user_id', jdata->>'user_id',
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);
			else
				isUpdate = true;
				/* we get old quantity */
				select sum(quantity) into _old_quantity 
				from accounting.warehouse_outgoing
				where name_id = _name_id
				and import_id = _import_id
				and unit_price = _unit_price
				and storage_location_id = _storage_location_id;

				/* WE ONLY CHECK IF NEW QUANTITY IS ADDED */
				if _old_quantity < _quantity then
					perform accounting.warehouse_product_amount_validation(
						_storage_location_id,
						_name_id,
						_import_id,
						_quantity - _old_quantity,
						_unit_price,
						_financing
					);
				end if;

				/* we update ledger with new accouting entry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					null,
					null,
					_ledger_id
				) INTO _ledger_id;

				/* CURR-TABLE UPDATE */
				update accounting.warehouse_outgoing wo SET
					order_number = _order_number,
					financing = _financing,
					staff_id = _staff_id,
					department_id = _department_id,
					basis = _basis,
					comment = _comment,
					created = CASE
    				    WHEN _created_date IS NOT NULL
    				    THEN jsonb_set(
    				             wo.created,
    				             '{date}',
    				             to_jsonb(_created_date)
    				         )
    				    ELSE wo.created
    				END,
					through = _through,
					name_id = _name_id,
					quantity = _quantity,
					unit_price = _unit_price,
					import_id = _import_id,
					credit = _credit,
					debit = _debit,
					ledger_id = _ledger_id,
					storage_location_id = _storage_location_id,
					updated = _updated
				where id = _id;
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








