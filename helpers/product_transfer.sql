


select * from accounting.product_transfer;


select * from accounting.ledger;


select * from accounting.warehouse_total_routing


CREATE OR REPLACE FUNCTION accounting.upsert_product_transfer(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_financing accounting.budget_distribution_type = 
			(jdata->>'financing')::accounting.budget_distribution_type;
		_transfer_number bigint = (jdata->>'transfer_number')::bigint;
		isUpdate bool = false;
		_from_storage_location_id bigint = (jdata->>'from_storage_location_id')::bigint;
		_to_storage_location_id bigint = (jdata->>'to_storage_location_id')::bigint;
		_main_department_id int = (jdata->>'main_department_id')::int;
		_comment text = jdata->>'comment';

		/* table variables */
		_product jsonb;
		_id bigint;
		_name_id bigint;
		_import_id bigint;
		_quantity numeric;
		_unit_price numeric;
		_debit integer;
		_credit integer;
		_ledger_id bigint;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.product_transfer
			where id = _transfer_number 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;

		/* GENERATING NEW UNIQUE-OUTGOING-NUMBER and ORDER-NUMBER for INSERTION */
		if _transfer_number is null then
			SELECT coalesce(max(sub.transfer_number), 0) + 1
			into _transfer_number from (
				SELECT transfer_number
	        	FROM accounting.product_transfer
	        	GROUP BY transfer_number
	        	ORDER BY transfer_number DESC
	        	LIMIT 1
			) sub;
		end if;

		/* UPSERT */
		FOR _product IN SELECT * FROM jsonb_array_elements((jdata->>'table_data')::jsonb) LOOP
				_id = (_product->>'id')::bigint;
				_name_id = (_product->>'name_id')::bigint;
				_import_id = (_product->>'import_id')::bigint;
				_quantity = (_product->>'quantity')::numeric;
				_unit_price = (_product->>'unit_price')::numeric;
				_debit = (_product->>'debit')::integer;
				_credit = (_product->>'credit')::integer;

			-- insertion
			if _id is null then
				/* we validate the required quantity to transfer */
				perform accounting.warehouse_product_amount_validation(
				   _from_storage_location_id,
				   _name_id,
				   _import_id,
				   _quantity,
				   _unit_price,
				   _financing
				);

				/* we fill ledger with the accounting entry */
				SELECT accounting.upsert_ledger(
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					null,
					null,
					null
				) INTO _ledger_id;

				-- insertion
				insert into accounting.product_transfer (
					transfer_number,
					from_storage_location_id,
					to_storage_location_id,
					main_department_id,
					moved_at,
					comment,
					financing,

					/* table data */
					debit,
					credit,
					name_id,
					quantity,
					unit_price,
					import_id,
					ledger_id,
					
					created
				) values (
					_transfer_number,
					_from_storage_location_id,
					_to_storage_location_id,
					_main_department_id,
					LOCALTIMESTAMP(0),
					_comment,
					_financing,
					
					/* table data */
					_debit,
					_credit,
					_name_id,
					_quantity,
					_unit_price,
					_import_id,
					_ledger_id,
					
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);

			-- update
			else
				isUpdate = true;
				update accounting.product_transfer ie SET				
					-- from_storage_location_id = _from_storage_location_id,
					-- to_storage_location_id = _to_storage_location_id,
					main_department_id  = _main_department_id,
					comment = _comment,
					financing = _financing,
					
					/* table data */
					-- debit = _debit,
					-- credit = _credit,
					-- name_id = _name_id,
					-- quantity = _quantity,
					-- unit_price = _unit_price,
					-- import_id = _import_id,
					-- ledger_id = _ledger_id,
					
					created = CASE
	    			    WHEN _created_date IS NOT NULL
	    			    THEN jsonb_set(
	    			             ie.created,
	    			             '{date}',
	    			             to_jsonb(_created_date)
	    			         )
	    			    ELSE ie.created
	    			END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', LOCALTIMESTAMP(0)
					)
				where id = _id;
			end if;
		END LOOP;

		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _transfer_number)
		);

		return json_build_object(
			'msg', case when isUpdate is true then 'updated' else 'created' end,
			'status', 200,
			'transfer_number', _transfer_number
		);
	end;
$BODY$;




 

CREATE OR REPLACE FUNCTION accounting.get_product_transfer(
	_department_id integer,
	_financing accounting.budget_distribution_type,
	_transfer_number bigint DEFAULT NULL::bigint,
	_created_from text DEFAULT NULL::text,
	_created_to text DEFAULT NULL::text,
	_from_location bigint DEFAULT NULL::bigint,
	_to_location bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
		total int;
	BEGIN

		/* TOTAL */
		select count(*) into total
		from accounting.product_transfer
		where financing = _financing 
		and (_transfer_number is null or transfer_number = _transfer_number)
		and (_created_from is null or _created_from::date <= moved_at::date)
		and (_created_to is null or _created_to::date >= moved_at::date)
		and (_from_location is null or _from_location = from_storage_location_id)
		and (_to_location is null or _to_location = to_storage_location_id);

		/* MAIN QUERY */
		WITH main1 AS (
		    SELECT DISTINCT ON (transfer_number)
				transfer_number,
				from_storage_location_id,
				to_storage_location_id,
				main_department_id,
				moved_at,
				comment,
				financing,
				status,
				(created->>'date')::date as created_date
			from accounting.product_transfer
			where financing = _financing 
			and (_transfer_number is null or transfer_number = _transfer_number)
			and (_created_from is null or _created_from::date <= moved_at::date)
			and (_created_to is null or _created_to::date >= moved_at::date)
			and (_from_location is null or _from_location = from_storage_location_id)
			and (_to_location is null or _to_location = to_storage_location_id)
		    ORDER BY transfer_number
		), 
		main2 AS (
		    SELECT 
		        transfer_number,
		        jsonb_agg(
		            jsonb_build_object(
		                'key', id,
		                'id', id,
		                'import_id', import_id,
		                'name_id', name_id,
		                'quantity',quantity,
		                'unit_price', unit_price,
		                'ledger_id', ledger_id,
		                'debit', debit,
		                'credit', credit
		            ) ORDER BY name_id
		        ) AS table_data
		    FROM accounting.product_transfer
		    GROUP BY transfer_number
		),
		main as (
			SELECT 
				row_number() over(order by m1.transfer_number) as key,
			    m1.*,
			    m2.table_data
			FROM main1 m1
			JOIN main2 m2 
			USING (transfer_number)
			limit _limit offset _offset
		),
		routing_1 as (
			select
			wtr.warehouse_id,
			row_number() over(
				partition by wtr.warehouse_id
				order by wtr.warehouse_id, wtr."createdAt"
			) as rownumber,
			jsonb_build_object(
				'jobposition_id', wtr.jobposition_id,
				'level', l.level,
				'fullname', d.fullname,
				'status', wtr.status,
				'declined_text', wtr.declined_text,
				'date', case when wtr."updatedAt" is not null
					then (wtr."updatedAt")::date 
					else (wtr."createdAt")::date end,
				'time', case when wtr."updatedAt" is not null
					then (wtr."updatedAt")::time 
					else (wtr."createdAt")::time end
			) routing_object from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'product_transfer' 
			) wtr
			left join (
				select 
			  		j.id, 
					concat_ws(' ', s.lastname, s.firstname, s.middlename ) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
			) d on wtr.jobposition_id = d.id
			left join (
				select level, unnest(jobpositions) as jobposition_id 
				from commons.department_routing_levels
				where department_id = _department_id
			) l on wtr.jobposition_id = l.jobposition_id
			order by wtr.warehouse_id, wtr."createdAt"
		), 
		routing_2 as (
			select
				warehouse_id,
				jsonb_object_agg(
					rownumber,
					routing_object
				) as routing
			from routing_1		
			group by warehouse_id
		)
		select jsonb_agg(
			jsonb_build_object(				
				'key', m.key,
				'transfer_number', m.transfer_number,
				'from_storage_location_id', m.from_storage_location_id,
				'to_storage_location_id', m.to_storage_location_id,
				'main_department_id', m.main_department_id,
				'moved_at', m.moved_at,
				'comment', m.comment,
				'financing', financing,
				'status', status,
				'table_data', m.table_data,
				'created_date', m.created_date,
				'routing', r.routing
			)
		) into _result
		from main m 
		left join routing_2 r 
		on m.transfer_number = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'total', total,
			'results', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_product_transfer_by_id(
	_department_id integer,
	_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN
	
		/* MAIN QUERY */
		WITH main1 AS (
		    SELECT DISTINCT ON (transfer_number)
				transfer_number,
				from_storage_location_id,
				to_storage_location_id,
				main_department_id,
				moved_at,
				comment,
				financing,
				status,
				(created->>'date')::date as created_date
			from accounting.product_transfer
			where transfer_number = _id 
		), 
		main2 AS (
		    SELECT 
		        transfer_number,
		        jsonb_agg(
		            jsonb_build_object(
						'key', id,
		                'id', id,
		                'import_id', import_id,
		                'name_id', name_id,
		                'quantity',quantity,
		                'unit_price', unit_price,
		                'ledger_id', ledger_id,
		                'debit', debit,
		                'credit', credit
		            ) ORDER BY name_id
		        ) AS table_data
		    FROM accounting.product_transfer
			where transfer_number = _id
		    GROUP BY transfer_number
		),
		main as (
			SELECT 
			    m1.*,
			    m2.table_data
			FROM main1 m1
			JOIN main2 m2 
			USING (transfer_number)
		),
		routing_1 as (
		select
			wtr.warehouse_id,
			row_number() over(
				partition by wtr.warehouse_id
				order by wtr.warehouse_id, wtr."createdAt"
			) as rownumber,
			jsonb_build_object(
				'jobposition_id', wtr.jobposition_id,
				'level', l.level,
				'fullname', d.fullname,
				'status', wtr.status,
				'declined_text', wtr.declined_text,
				'date', case when wtr."updatedAt" is not null
					then (wtr."updatedAt")::date 
					else (wtr."createdAt")::date end,
				'time', case when wtr."updatedAt" is not null
					then (wtr."updatedAt")::time 
					else (wtr."createdAt")::time end
			) routing_object from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'product_transfer' 
			) wtr
			left join (
				select 
			  		j.id, 
					concat_ws(' ', s.lastname, s.firstname, s.middlename ) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
			) d on wtr.jobposition_id = d.id
			left join (
				select level, unnest(jobpositions) as jobposition_id 
				from commons.department_routing_levels
				where department_id = _department_id
			) l on wtr.jobposition_id = l.jobposition_id
			order by wtr.warehouse_id, wtr."createdAt"
		), 
		routing_2 as (
			select
				warehouse_id,
				jsonb_object_agg(
					rownumber,
					routing_object
				) as routing
			from routing_1		
			group by warehouse_id
		)
		select jsonb_build_object(
			'transfer_number', m.transfer_number,
			'from_storage_location_id', m.from_storage_location_id,
			'to_storage_location_id', m.to_storage_location_id,
			'main_department_id', m.main_department_id,
			'moved_at', m.moved_at,
			'comment', m.comment,
			'financing', financing,
			'status', status,
			'table_data', m.table_data,
			'created_date', m.created_date,
			'routing', r.routing
		) into _result
		from main m 
		left join routing_2 r 
		on m.transfer_number = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'result', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_product_transfer_id(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select transfer_number 
		into _last_id
		from accounting.product_transfer
		GROUP by transfer_number
		order by transfer_number desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;



/*

	1. figuring out the logic for finding the product amount for location transfer;

	2. refactor the logic for warehouse-outgoing;

	3. refactor the logic for inventory-state;

	4. refactor the product_amount_check for inventory_entry & goods_return;

	5. recheck the logic for other sections like inventory_entry & goods_return;

*/



select * from commons.storage_location


select * from accounting.product_transfer;


select * from accounting.warehouse_total_routing
where warehouse_section = 'product_transfer';


select * from accounting.warehouse_incoming;


select * from accounting.warehouse_outgoing;

select * from accounting.inventory_entry;

select * from accounting.goods_return;


CREATE OR REPLACE FUNCTION accounting.warehouse_product_amount_validation(
	_location_id bigint,
	_name_id bigint,
	_import_id bigint,
	_amount numeric,
	_unit_price numeric,
	_financing accounting.budget_distribution_type)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _left_quantity numeric;
    product_name text;
BEGIN

    -- 1. Main warehouse
	with main_warehouse as (
		SELECT 
			name_id,
			unique_import_number as import_id,
			unit_price,
			storage_location_id as to_storage_location_id,
			null::bigint as from_storage_location_id,
			sum(quantity) as quantity
		FROM accounting.warehouse_incoming
		where name_id = _name_id
		and unique_import_number = _import_id
		and unit_price = _unit_price
		and financing = _financing
		-- and status = 'approved'
		group by name_id, unique_import_number, unit_price, storage_location_id
	),
	-- 2. Movements history
	movements as (
		select
			name_id,
	        import_id,
	        unit_price,
			to_storage_location_id,
			from_storage_location_id,
			quantity
		FROM accounting.product_transfer
		where name_id = _name_id
		and import_id = _import_id
		and unit_price = _unit_price
		and financing = _financing
		-- and status = 'approved'
	),
	-- 3. All the warehouses
	all_warehouses as (
		select * from main_warehouse
		UNION all
		select * from movements
	),
	-- 4. Warehouses' incoming movements
	movement_incoming AS (
	    SELECT
	        name_id,
	        import_id,
	        unit_price,
	        to_storage_location_id AS location_id,
	        SUM(quantity) AS quantity
	    FROM all_warehouses
	    GROUP BY name_id, import_id, unit_price, to_storage_location_id
	),
	-- 5. Warehouses' outgoing movements
	movement_outgoing AS (
	    SELECT
	        name_id,
	        import_id,
	        unit_price,
	        from_storage_location_id AS location_id,
	        SUM(quantity) AS quantity
	    FROM all_warehouses
		WHERE from_storage_location_id IS NOT NULL
	    GROUP BY name_id, import_id, unit_price, from_storage_location_id
	),
	-- 6. Real number calculation
	movement_combined AS (
	    SELECT
	        COALESCE(mi.name_id, mo.name_id) AS name_id,
	        COALESCE(mi.import_id, mo.import_id) AS import_id,
	        COALESCE(mi.unit_price, mo.unit_price) AS unit_price,
	        COALESCE(mi.location_id, mo.location_id) AS location_id,
	        COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) quantity
	    FROM movement_incoming mi
	    FULL JOIN movement_outgoing mo
	    ON mi.name_id = mo.name_id
	    AND mi.import_id = mo.import_id
	    AND mi.unit_price = mo.unit_price
	    AND mi.location_id = mo.location_id
		where COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) > 0
	),
	-- 7. Exports count
	warehouse_exports as (
		select 
			name_id,
			import_id,
			unit_price,
			storage_location_id as location_id,
			sum(quantity) quantity
		from accounting.warehouse_outgoing
		where name_id = _name_id
		and import_id = _import_id
		and unit_price = _unit_price
		and financing = _financing
		-- and status = 'approved'
		group by name_id, import_id, unit_price, storage_location_id
	),
	-- 8. We calculate final numbers
	left_quantities as (
		select 
			COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0) left_quantity
		from movement_combined mc
		left join warehouse_exports we
		ON mc.name_id = we.name_id
	    AND mc.import_id = we.import_id
	    AND mc.unit_price = we.unit_price
	    AND mc.location_id = we.location_id
		where mc.location_id = _location_id
	)
    SELECT left_quantity 
	INTO _left_quantity 
	FROM left_quantities;

    IF _left_quantity IS NULL THEN
        _left_quantity := 0;
    END IF;

    -- Actual validation
    IF _left_quantity < _amount THEN

        SELECT name->>'ru'
        INTO product_name
        FROM commons.nomenclature
        WHERE id = _name_id;

        RAISE EXCEPTION
            'Запрошенное количество % превышает доступное % для товара "%"',
            _amount, _left_quantity, product_name;
    END IF;

END;
$BODY$;