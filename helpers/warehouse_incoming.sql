

select * from accounting.warehouse_total_routing;


select * from accounting.warehouse_incoming 
order by id;


select * from accounting.ledger 
where id > 96
order by id;


CREATE OR REPLACE FUNCTION accounting.upsert_warehouse_incoming(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_doc_id bigint = (jdata->>'doc_id')::bigint;
		_counterparty_id bigint = (jdata->>'counterparty_id')::bigint;
		_user_id uuid = (jdata->>'user_id')::uuid;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_unique_import_number bigint = (jdata->>'unique_import_number')::bigint;
		_order_number bigint = (jdata->>'order_number')::bigint;
		created_date date = (jdata->>'created_date')::date;
		_comment text = (jdata->>'comment')::text;
		_contract_id bigint = (jdata->>'contract_id')::bigint;
		_product jsonb;
		isUpdate boolean = false;

		/* table variables */
		_id bigint;
		_name_id bigint;
		_unit_price numeric;
		_quantity numeric;
		_ledger_id bigint;
		_debit integer;
		_credit integer;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.warehouse_incoming
			where unique_import_number = _unique_import_number 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;

		/* GENERATING NEW UNIQUE-IMPORT-NUMBER and ORDER-NUMBER for INSERTION */
		if _unique_import_number is null then
			SELECT coalesce(max(sub.unique_import_number), 0) + 1
			into _unique_import_number from (
				SELECT unique_import_number
	        	FROM accounting.warehouse_incoming
	        	GROUP BY unique_import_number
	        	ORDER BY unique_import_number DESC
	        	LIMIT 1
			) sub;

			SELECT coalesce(max(sub.order_number), 0) + 1
			into _order_number from (
				SELECT order_number
	        	FROM accounting.warehouse_incoming
				where financing = _financing
				and extract(year from (created->>'date')::date) = extract(
					year from coalesce(created_date, current_date)
				)
	        	GROUP BY order_number
	        	ORDER BY order_number DESC
	        	LIMIT 1
			) sub;
		end if;

		-- when date is changed in the scale of year, then we have to change the order-nunber as well;
		if exists (
			select 1 from accounting.warehouse_incoming
			where unique_import_number = _unique_import_number
			and extract(year from (created->>'date')::date) <> extract(year from created_date) 
		) and then
			SELECT coalesce(max(sub.order_number), 0) + 1
			into _order_number from (
				SELECT order_number
	        	FROM accounting.warehouse_incoming
				where financing = _financing
				and extract(year from (created->>'date')::date) = extract(
					year from coalesce(created_date, current_date)
				)
	        	GROUP BY order_number
	        	ORDER BY order_number DESC
	        	LIMIT 1
			) sub;
		end if;

		-- loop
		FOR _product IN SELECT * FROM json_array_elements(jdata->'products') LOOP
			_id = (_product->>'id')::bigint;
			_name_id = (_product->>'name_id')::bigint;
			_quantity = (_product->>'quantity')::numeric;
			_unit_price = (_product->>'unit_price')::numeric;
			_debit = (_product->>'debit')::int;
			_credit = (_product->>'credit')::int;
			_ledger_id = (_product->>'ledger_id')::bigint;

			if _id is null then
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					_contract_id,
					null,
					null
				) INTO _ledger_id;

				/* warehouse-incoming new insertion */
				insert into accounting.warehouse_incoming (
					doc_id,
					counterparty_id, 
					financing,
					name_id, 
					quantity,
					unit_price, 
					debit,
					credit,
					vat,
					unique_import_number,
					order_number,
					comment,
					contract_id,
					storage_location_id,
					ledger_id,
					created
				) values (
					_doc_id,
					_counterparty_id,
					_financing,
					_name_id,
					_quantity,
					_unit_price,
					_debit,
					_credit,
					(_product->>'vat')::numeric,
					_unique_import_number,
					_order_number,
					_comment,
					_contract_id,
					1,
					_ledger_id,
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(created_date, LOCALTIMESTAMP(0))
					)
				);
			else
				isUpdate = true;
				/* update validations */
				perform accounting.warehouse_incoming_update_validation(
					_id,
				    1,
				    _name_id,
				    _unique_import_number,
				    _quantity,
				    _unit_price,
				    _financing
				);

				/* we update ledger with new accouting entry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					_contract_id,
					null,
					_ledger_id
				) INTO _ledger_id;
				
				/* we update rows */
				update accounting.warehouse_incoming wi SET
					order_number = _order_number,
					doc_id = _doc_id,
					counterparty_id = _counterparty_id,
					financing = _financing,
					name_id = _name_id,
					unit_price = _unit_price,
					debit = _debit,
					credit = _credit,
					vat = (_product->>'vat')::numeric,
					quantity = _quantity,
					comment = _comment,
					contract_id = _contract_id,
					ledger_id = _ledger_id,
					created = CASE
    				    WHEN created_date IS NOT NULL
    				    THEN jsonb_set(
    				             wi.created,
    				             '{date}',
    				             to_jsonb(created_date)
    				         )
    				    ELSE wi.created
    				END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)
					)
				where id = _id;
			end if;
    	END LOOP;

		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _unique_import_number)
		);

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'unique_import_number', _unique_import_number,
			'order_number', _order_number,
			'status', 200
		);
	end;
$BODY$;








CREATE OR REPLACE FUNCTION accounting.get_warehouse_incoming_by_unique_import_number(
	_unique_import_number bigint,
	_department_id integer)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result json;
	BEGIN

	with incomings_parent as (
	 	SELECT DISTINCT ON (wi.unique_import_number)
		 		wi.unique_import_number,
				wi.financing,
				wi.order_number,
	        	wi.doc_id,
	        	wi.counterparty_id,
	        	wi.status,
				(wi.created->>'date')::date created_date,
	        	wi.comment,
				wi.contract_id
		    FROM accounting.warehouse_incoming wi
		    where wi.unique_import_number = _unique_import_number
		    ORDER BY wi.unique_import_number, wi.id
	),
	incomings_child AS (
	    SELECT 
	        wi.unique_import_number,
	        jsonb_agg(
	            jsonb_build_object(
	                'id', wi.id,
	            	'name_id', wi.name_id,
	            	'unit_price', wi.unit_price,
	            	'quantity', wi.quantity,
					'debit', wi.debit,
					'credit', wi.credit,
					'vat', wi.vat,
					'ledger_id', wi.ledger_id
	            ) ORDER BY wi.name_id
	        ) AS products
	    FROM accounting.warehouse_incoming wi
	    GROUP BY wi.unique_import_number
	),
	incomings as (
		SELECT 
		    p.*,
		    c.products
		FROM incomings_parent p
		JOIN incomings_child c 
		USING (unique_import_number)
	),
	routing_1 as (
		select
			wtr.warehouse_id,
			row_number() over(
				order by wtr."createdAt"
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
			) routing_object
		from (
			select * from accounting.warehouse_total_routing
			where warehouse_section = 'incoming'
			and warehouse_id = _unique_import_number
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
		order by wtr."createdAt"
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
	) select jsonb_build_object(
		'unique_import_number', i.unique_import_number,
		'order_number', i.order_number,
		'financing', i.financing,
		'doc_id', i.doc_id,
		'counterparty_id', i.counterparty_id,
		'contract_id', i.contract_id,
		'contract', cc.contract,
		'status', i.status,
		'products', i.products,
		'created_date', i.created_date,
		'comment', i.comment,
		'routing', r2.routing
	) from incomings i into _result
	left join routing_2 r2
	on i.unique_import_number = r2.warehouse_id
	left join commons.counterparty_contracts cc
	on i.contract_id = cc.id;

		return jsonb_build_object(
			'statusCode', 200,
			'statusMessage', 'OK',
			'result', _result
		); 
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_warehouse_incoming(
	_department_id integer,
	_financing accounting.budget_distribution_type,
	_order_number bigint DEFAULT NULL::bigint,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_counterparty_id bigint DEFAULT NULL::bigint,
	_name_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

declare
	_result json;
	total int;
	_unique_import_numbers bigint[];
begin

	if _name_id is not null then
		select array_agg(distinct unique_import_number) 
		into _unique_import_numbers
		from accounting.warehouse_incoming
		where name_id = _name_id;
	end if;

	/* COUTING TOTAL */
	select count(distinct(wi.unique_import_number)) into total 
	from accounting.warehouse_incoming wi
	where financing = _financing 
	and (wi.order_number = _order_number or _order_number is null)
	and (wi.counterparty_id = _counterparty_id or _counterparty_id is null)
	and (_name_id is null or wi.unique_import_number = any(_unique_import_numbers))
	and (_date_from is null or (wi.created->>'date')::date >= _date_from::date)
	and (_date_to is null or (wi.created->>'date')::date <= _date_to::date);

	-- main query
	with incomings_parent as (
	 	SELECT DISTINCT ON (wi.unique_import_number)
		 		wi.unique_import_number,
				wi.financing,
				wi.order_number,
	        	wi.doc_id,
	        	wi.counterparty_id,
	        	wi.status,
				(wi.created->>'date')::date created_date,
	        	wi.comment,
				wi.contract_id
		    FROM accounting.warehouse_incoming wi
		    where financing = _financing
			and (wi.order_number = _order_number or _order_number is null)
			and (wi.counterparty_id = _counterparty_id or _counterparty_id is null)
			and (_name_id is null or wi.unique_import_number = any(_unique_import_numbers))
			and (_date_from is null or (wi.created->>'date')::date >= _date_from::date)
			and (_date_to is null or (wi.created->>'date')::date <= _date_to::date)
		    ORDER BY wi.unique_import_number, wi.id
	),
	incomings_child AS (
	    SELECT 
	        wi.unique_import_number,
	        jsonb_agg(
	            jsonb_build_object(
	                'id', wi.id,
	            	'name_id', wi.name_id,
	            	'unit_price', wi.unit_price,
	            	'quantity', wi.quantity,
					'debit', wi.debit,
					'credit', wi.credit,
					'vat', wi.vat,
					'ledger_id', wi.ledger_id
	            ) ORDER BY wi.name_id
	        ) AS products
	    FROM accounting.warehouse_incoming wi
	    GROUP BY wi.unique_import_number
	),
	incomings as (
		SELECT 
		    p.*,
		    c.products
		FROM incomings_parent p
		JOIN incomings_child c 
		USING (unique_import_number)
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
				where warehouse_section = 'incoming' 
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
		), final_join as (
			select
				row_number() over(order by i.order_number) as key,
				i.financing,
				i.order_number,
				i.unique_import_number,
	        	i.doc_id,
	        	i.counterparty_id,
	        	i.status,
	        	i.products,
	        	i.created_date,
	        	i.comment,
	        	i.contract_id,
	        	cc.contract,
				r2.routing
			from incomings i
			left join routing_2 r2
				on i.unique_import_number = r2.warehouse_id
			left join commons.counterparty_contracts cc
				on i.contract_id = cc.id
			-- order by i.order_number desc
			limit _limit offset _offset
		) select jsonb_agg(fj)
		into _result
		from final_join fj;

		return jsonb_build_object(
			'statusCode', 200,
			'statusMessage', 'OK',
			'total', total,
			'results', _result
		);
end;
$BODY$;





select * from accounting.warehouse_incoming;



select * from accounting.warehouse_outgoing;


select accounting.warehouse_incoming_update_validation(
	675, 1, 267, 2, 20, 
)


CREATE OR REPLACE FUNCTION accounting.warehouse_incoming_update_validation(
	_id bigint,
	_location_id bigint,
	_name_id bigint,
	_import_id bigint,
	_quantity numeric,
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
	_old_name_id bigint;
    _old_quantity numeric;
    _amount numeric;
BEGIN

	/* product name for validation messages */
	SELECT name->>'ru'
	INTO product_name
	FROM commons.nomenclature
	WHERE id = _name_id;

	/* name_id change validation */
	select name_id into _old_name_id
	from accounting.warehouse_incoming
	where id = _id;
	if _name_id <> _old_name_id and 
	(
		exists (
			select 1 from accounting.warehouse_outgoing
			where name_id = _old_name_id
			and unit_price = _unit_price
			and import_id = _import_id
		)
		OR
		exists (
			select 1 from accounting.product_transfer
			where name_id = _old_name_id
			and unit_price = _unit_price
			and import_id = _import_id
		)
	) then
		RAISE EXCEPTION
		'Вы не можете заменить этот товар на другой, поскольку он был экспортирован или перемещен.';
	end if;

	/* we validate the new quantity */
	select sum(quantity) into _old_quantity 
	from accounting.warehouse_incoming
	where name_id = _name_id
	and unique_import_number = _import_id
	and unit_price = _unit_price
	and storage_location_id = _location_id;
			
	if _old_quantity > _quantity then
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
		_amount = _old_quantity - _quantity;
	    IF _left_quantity < _amount THEN
	        RAISE EXCEPTION
				'Вы не можете уменьшить на % количество, так как больше этого (около %) уже экспортировано или перемещено для товара "%"',
	            _old_quantity - _amount, _old_quantity - _left_quantity, product_name;
	    END IF;
	END IF;
END;
$BODY$;








CREATE OR REPLACE FUNCTION accounting.download_warehouse_incoming_borkhat(
	_unique_import_number bigint,
	_department_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

	DECLARE
		_result json;
	BEGIN

		with products as (
			select 
				wi.unique_import_number id,
				min(wi.order_number) order_number,
				min(cp.name) counterparty_name,
				jsonb_agg(jsonb_build_object (
					'product_name', nm.product_name,
					'unit', nm.unit,
					'quantity', wi.quantity,
					'price', wi.unit_price,
					'total_price', wi.quantity * COALESCE(wi.unit_price, 0)
				) order by wi.name_id) products
			from accounting.warehouse_incoming wi
			left join (
				select 
					n.id, 
					n.name product_name, 
					gu.name->>'tj' unit 
				from commons.nomenclature n
				left join commons.global_units gu
				on n.unit_id = gu.id 
				and (gu.disabled = false or gu.disabled is null)
				where (n.disabled = false or n.disabled is null)
			) nm on wi.name_id = nm.id
      left join (
        select id, name from accounting.counterparty
      ) cp on wi.counterparty_id = cp.id
			where wi.unique_import_number = _unique_import_number
			group by wi.unique_import_number
		),
		routing_1 as (
			select
				wtr.warehouse_id,
				row_number() over(
					order by wtr."createdAt"
				) as rownumber,
				jsonb_build_object(
					'user_id', d.user_id,
					'fullname', d.fullname,
					'status', wtr.status,
					'date', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::date 
						else (wtr."createdAt")::date end,
					'time', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::time 
						else (wtr."createdAt")::time end
				) routing_object
			from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'incoming'
				and warehouse_id = _unique_import_number
			) wtr
			left join (
				select 
			  		j.id,
					u.id as user_id,
					concat_ws(' ', s.lastname, s.firstname, s.middlename) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
				left join auth.user u
				on j.staff_id = u.staff_id
			) d on wtr.jobposition_id = d.id
			left join (
				select level, unnest(jobpositions) as jobposition_id 
				from commons.department_routing_levels
				where department_id = _department_id
			) l on wtr.jobposition_id = l.jobposition_id
			order by wtr."createdAt"
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
			'id', p.id,
			'order_number', p.order_number,
			'products', p.products,
			'counterparty_name', p.counterparty_name,
			'routing', r2.routing
		) into _result
		from products p
		left join routing_2 r2
		on p.id = r2.warehouse_id;

		return jsonb_build_object(
			'statusCode', 200,
			'statusMessage', 'OK',
			'result', _result
		); 
	end;
$BODY$;