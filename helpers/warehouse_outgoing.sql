
select * from accounting.warehouse_outgoing;

select * from accounting.warehouse_total_routing;


select accounting.upsert_warehouse_outgoing (
	'
	{
		"routing": {
			"warehouse_id":2,
			"warehouse_section":"outgoing",
			"jobposition_id":109,
			"department_id":157,
			"status":"approved",
			"declined_text":""
		},
		"user_id": "98347360-c383-4979-9b21-c04d4808ce88",
		"financing": "budget",
		"products": [
			{
				"id":78,
				"debit":111000,
				"credit":54321,
				"name_id":267,
				"quantity":"15",
				"import_id":1,
				"unit_price":1000
			}
		],
		"department_id": 1,
		"storage_location_id": 3,
		"staff_id": 3674,
		"unique_outgoing_number": 2,
		"order_number": 2,
		"comment": "test3",
		"created_date": "2025-12-10"
	}
	'
);


NOTICE:  _old_quantity 95.0
NOTICE:  _quantity 15
NOTICE:  difference -80.0
NOTICE:  _storage_location_id 3


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
			_quantity = (_product->>'quantity')::numeric;
			_name_id = (_product->>'name_id')::bigint;
			_import_id = (_product->>'import_id')::bigint;
			_unit_price = (_product->>'unit_price')::numeric;
				
			if (_product->>'id')::bigint is null then
				/* PRODUCT AMOUNT CHECK VALIDATION */
				perform accounting.warehouse_product_amount_validation(
					_storage_location_id,
					_name_id,
					_import_id,
					_quantity,
					_unit_price,
					_financing
				);

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
					(_product->>'credit')::integer,
					(_product->>'debit')::integer,
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
					credit = (_product->>'credit')::integer,
					debit = (_product->>'debit')::integer,
					storage_location_id = _storage_location_id,
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









CREATE OR REPLACE FUNCTION accounting.get_warehouse_outgoing(
	routing_department_id integer,
	_financing accounting.budget_distribution_type,
	_order_number bigint DEFAULT NULL::bigint,
	date_from text DEFAULT NULL::text,
	date_to text DEFAULT NULL::text,
	_department_id integer DEFAULT NULL::integer,
	_staff_id bigint DEFAULT NULL::bigint,
	_name_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result json;
		total int;
		_unique_outgoing_numbers bigint[];
	BEGIN

		if _name_id is not null then
			select array_agg(distinct unique_outgoing_number) 
			into _unique_outgoing_numbers
			from accounting.warehouse_outgoing
			where name_id = _name_id;
		end if;

		/* COUTING TOTAL */
		select count(distinct wo.unique_outgoing_number) into total
		from accounting.warehouse_outgoing wo
		where wo.financing = _financing 
		and (wo.order_number = _order_number or _order_number is null)
		and (date_from is null or (wo.created->>'date')::date >= date_from::date)
		and (date_to is null or (wo.created->>'date')::date <= date_to::date)
		and (wo.department_id = _department_id or _department_id is null)
		and (wo.staff_id = _staff_id or _staff_id is null)
		and (_name_id is null or wo.unique_outgoing_number = any(_unique_outgoing_numbers));
		

		WITH outgoing_parent AS (
		    SELECT DISTINCT ON (wo.unique_outgoing_number)
		        wo.unique_outgoing_number,
		        wo.order_number,
		        wo.financing,
		        wo.staff_id,
		        wo.department_id,
		        wo.doc_id,
		        wo.status,
		        wo.through,
		        (wo.created->>'date')::date AS created_date,
		        wo.comment,
		        wo.storage_location_id,
		        wo.basis
		    FROM accounting.warehouse_outgoing wo
		    WHERE wo.financing = _financing
		      AND (_order_number IS NULL OR wo.order_number = _order_number)
		      AND (date_from IS NULL OR (wo.created->>'date')::date >= date_from::date)
		      AND (date_to   IS NULL OR (wo.created->>'date')::date <= date_to::date)
		      AND (_department_id IS NULL OR wo.department_id = _department_id)
		      AND (_staff_id IS NULL OR wo.staff_id = _staff_id)
		      AND (_name_id IS NULL OR wo.unique_outgoing_number = ANY(_unique_outgoing_numbers))
		    ORDER BY wo.unique_outgoing_number, wo.id
		), outgoing_products AS (
		    SELECT 
		        wo.unique_outgoing_number,
		        jsonb_agg(
		            jsonb_build_object(
		                'id', wo.id,
		                'import_id', wo.import_id,
		                'name_id', wo.name_id,
		                'quantity', wo.quantity,
		                'unit_price', wo.unit_price,
		                'debit', wo.debit,
		                'credit', wo.credit
		            ) ORDER BY wo.name_id
		        ) AS products
		    FROM accounting.warehouse_outgoing wo
		    GROUP BY wo.unique_outgoing_number
		),
		outgoing as (
			SELECT 
			    p.*,
			    pr.products
			FROM outgoing_parent p
			JOIN outgoing_products pr 
			USING (unique_outgoing_number)
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
				) routing_object
			from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'outgoing' 
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
				where department_id = routing_department_id
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
		), 
		final_join as (
			select
				row_number() over(order by og.order_number) as key,
				og.*,
				r2.routing
			from outgoing og
			left join routing_2 r2
			on og.unique_outgoing_number = r2.warehouse_id
			-- order by og.order_number desc 
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




















CREATE OR REPLACE FUNCTION accounting.get_warehouse_outgoing_by_unique_outgoing_number(
	_unique_outgoing_number bigint,
	_department_id integer)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result json;
	BEGIN
	
	WITH outgoing_parent AS (
	    SELECT DISTINCT ON (wo.unique_outgoing_number)
	        wo.unique_outgoing_number,
	        wo.order_number,
	        wo.financing,
	        wo.staff_id,
	        wo.department_id,
	        wo.doc_id,
	        wo.status,
	        wo.through,
	        (wo.created->>'date')::date AS created_date,
	        wo.comment,
	        wo.storage_location_id,
	        wo.basis
	    FROM accounting.warehouse_outgoing wo
	    WHERE wo.unique_outgoing_number = _unique_outgoing_number
	    ORDER BY wo.unique_outgoing_number, wo.id
	), outgoing_products AS (
	    SELECT 
	        wo.unique_outgoing_number,
	        jsonb_agg(
	            jsonb_build_object(
	                'id', wo.id,
	                'import_id', wo.import_id,
	                'name_id', wo.name_id,
	                'quantity', wo.quantity,
	                'unit_price', wo.unit_price,
	                'debit', wo.debit,
	                'credit', wo.credit
	            ) ORDER BY wo.name_id
	        ) AS products
	    FROM accounting.warehouse_outgoing wo
	    GROUP BY wo.unique_outgoing_number
	),
	outgoing as (
		SELECT 
		    p.*,
		    pr.products
		FROM outgoing_parent p
		JOIN outgoing_products pr 
		USING (unique_outgoing_number)
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
			where warehouse_section = 'outgoing'
			and warehouse_id = _unique_outgoing_number
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
		'unique_outgoing_number', og.unique_outgoing_number,
		'order_number', og.order_number,
		'financing', og.financing,
		'staff_id', og.staff_id,
		'department_id', og.department_id,
		'doc_id', og.doc_id,
		'basis', og.basis,
		'status', og.status,
		'created_date', og.created_date,
		'products', og.products,
		'through', og.through,
		'comment', og.comment,
		'storage_location_id', og.storage_location_id,
		'routing', r2.routing
	) from outgoing og into _result
	left join routing_2 r2
	on og.unique_outgoing_number = r2.warehouse_id;

		return jsonb_build_object(
			'statusCode', 200,
			'statusMessage', 'OK',
			'result', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_warehouse_outgoing_order_number(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_order_number bigint;
	BEGIN

		select order_number
		into _order_number
		from accounting.warehouse_outgoing
		group by order_number
		order by order_number desc
		limit 1;
		
		return _order_number;
	end;
$BODY$;





CREATE OR REPLACE FUNCTION accounting.download_warehouse_outgoing_borkhat(
	_unique_outgoing_number bigint,
	_department_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

	DECLARE
		_result json;
	BEGIN

		WITH products AS (
		    SELECT DISTINCT ON (wo.unique_outgoing_number)
		           wo.unique_outgoing_number AS id,
		           wo.order_number AS order_number,
				   (
					   (wo.basis->>'basis_type') || ' № ' ||
					   (wo.basis->>'basis_number') || ' аз ' ||
					   (wo.basis->>'basis_date')
				   ) AS based_on,
		           wo.through AS by_means_of,
		           (
		               SELECT jsonb_agg(
		                        jsonb_build_object(
		                            'product_name', nm2.product_name,
		                            'unit',        nm2.unit,
		                            'quantity',    wo2.quantity,
		                            'price',       wo2.unit_price,
		                            'total_price', wo2.quantity * COALESCE(wo2.unit_price, 0)
		                        )
		                        ORDER BY wo2.name_id
		                    )
		               FROM accounting.warehouse_outgoing wo2
		               LEFT JOIN (
		                    SELECT 
		                        n.id,
		                        n.name AS product_name,
		                        gu.name->>'tj' AS unit
		                    FROM commons.nomenclature n
		                    LEFT JOIN commons.global_units gu
		                        ON n.unit_id = gu.id 
		                       AND (gu.disabled is not true OR gu.disabled IS NULL)
		                    WHERE (n.disabled is not true OR n.disabled IS NULL)
		                ) nm2 
		               ON wo2.name_id = nm2.id
		               WHERE wo2.unique_outgoing_number = wo.unique_outgoing_number
		           ) AS products
		    FROM accounting.warehouse_outgoing wo
		    WHERE wo.unique_outgoing_number = _unique_outgoing_number
		    ORDER BY wo.unique_outgoing_number, wo.id
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
				where warehouse_section = 'outgoing'
				and warehouse_id = _unique_outgoing_number
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
			'based_on', p.based_on,
			'by_means_of', p.by_means_of,
			'products', p.products,
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









