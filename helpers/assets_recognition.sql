create table if not exists accounting.assets_recognition (
	id bigserial primary key,
	operation_number bigint not null,
	main_department_id integer not null REFERENCES commons.department (id),
	financing accounting.budget_distribution_type not null,

	/* table */
	name_id bigint not null,
	quantity numeric not null,
	unit_price numeric not null,
	credit integer not null,
	import_id bigint not null,
	inventory_number text,
	department_id integer REFERENCES commons.department (id),
	staff_id integer REFERENCES hr.staff (id),
	depreciation boolean default false,
	depreciation_percent numeric CHECK (depreciation_percent >= 0 AND depreciation_percent <= 100),
	depreciation_period integer CHECK (depreciation_period >= 0),
	/* table */
	
	committee jsonb,
	comment text,
	created jsonb not null,
	updated jsonb
);


select * from accounting.assets_recognition;


select * from accounting.warehouse_total_routing


CREATE OR REPLACE FUNCTION accounting.upsert_assets_recognition(
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
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		isUpdate bool = false;
		
		-- _storage_location_id bigint = (jdata->>'storage_location_id')::bigint;
		_main_department_id int = (jdata->>'main_department_id')::int;
		_comment text = (jdata->>'comment')::text;
		_committee jsonb = (jdata->>'committee')::jsonb;
		_product jsonb;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.assets_recognition
			where id = _operation_number 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;


		/* GENERATING NEW UNIQUE-OUTGOING-NUMBER and ORDER-NUMBER for INSERTION */
		if _operation_number is null then
			SELECT coalesce(max(sub.operation_number), 0) + 1
			into _operation_number from (
				SELECT operation_number
	        	FROM accounting.assets_recognition
	        	GROUP BY operation_number
	        	ORDER BY operation_number DESC
	        	LIMIT 1
			) sub;
		end if;

		/* UPSERT */
		FOR _product IN SELECT * FROM jsonb_array_elements((jdata->>'table_data')::jsonb) LOOP
			if (_product->>'id')::bigint is null then
				-- perform accounting.warehouse_product_amount_validation(
				--    _storage_location_id,
				--    (_product->>'name_id')::bigint,
				--    (_product->>'import_id')::bigint,
				--    (_product->>'quantity')::numeric,
				--    (_product->>'unit_price')::numeric,
				--    _financing
				-- );
			
				insert into accounting.assets_recognition (
					operation_number,
					main_department_id,
					financing,
				
					/* table */
					name_id,
					quantity,
					unit_price,
					credit,
					import_id,
					inventory_number,
					department_id,
					staff_id,
					depreciation,
					depreciation_percent,
					depreciation_period,
					/* table */
					
					committee,
					comment,
					created
				) values (
					_operation_number,
					_main_department_id,
					_financing,

					/* table */
					(_product->>'name_id')::bigint,
					(_product->>'quantity')::numeric,
					(_product->>'unit_price')::numeric,
					(_product->>'credit')::integer,
					(_product->>'import_id')::bigint,
					(_product->>'inventory_number')::text,
					(_product->>'department_id')::integer,
					(_product->>'staff_id')::bigint,
					(_product->>'depreciation')::boolean,
					(_product->>'depreciation_percent')::numeric,
					(_product->>'depreciation_period')::integer,
					/* table */

					_committee,
					_comment,
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);
			else
				isUpdate = true;
				update accounting.assets_recognition ar SET				
					-- storage_location_id = _storage_location_id,
					main_department_id  = _main_department_id,
					financing = _financing,
					comment = _comment,
					committee = _committee,
					
					/* table */
					name_id = (_product->>'name_id')::bigint,
					quantity = (_product->>'quantity')::numeric,
					unit_price = (_product->>'unit_price')::numeric,
					credit = (_product->>'credit')::integer,
					import_id = (_product->>'import_id')::bigint,
					inventory_number = (_product->>'inventory_number')::text,
					department_id = (_product->>'department_id')::integer,
					staff_id = (_product->>'staff_id')::bigint,
					depreciation = (_product->>'depreciation')::boolean,
					depreciation_percent = (_product->>'depreciation_percent')::numeric,
					depreciation_period = (_product->>'depreciation_period')::integer,
					/* table */
					
					created = CASE
	    			    WHEN _created_date IS NOT NULL
	    			    THEN jsonb_set(
	    			             ar.created,
	    			             '{date}',
	    			             to_jsonb(_created_date)
	    			         )
	    			    ELSE ar.created
	    			END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', LOCALTIMESTAMP(0)
					)
				where id = (_product->>'id')::bigint;
			end if;
		END LOOP;

		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _operation_number)
		);

		return json_build_object(
			'msg', case when isUpdate is true then 'updated' else 'created' end,
			'status', 200,
			'operation_number', _operation_number
		);
	end;
$BODY$;




 

CREATE OR REPLACE FUNCTION accounting.get_assets_recognition(
	_department_id integer,
	_financing accounting.budget_distribution_type,
	_operation_number bigint DEFAULT NULL::bigint,
	_created_from text DEFAULT NULL::text,
	_created_to text DEFAULT NULL::text,
	_accepted_department_id integer DEFAULT NULL::integer,
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
		from accounting.assets_recognition
		where financing = _financing 
		and (_operation_number is null or operation_number = _operation_number)
		and (_created_from is null or _created_from::date <= (created->>'date')::date)
		and (_created_to is null or _created_to::date >= (created->>'date')::date)
		and (_accepted_department_id is null or _accepted_department_id = department_id);

		/* MAIN QUERY */
		WITH main1 AS (
		    SELECT DISTINCT ON (operation_number)
				operation_number,
				main_department_id,
				financing,
				committee,
				comment,
				status,
				(created->>'date')::date as created_date
			from accounting.assets_recognition
			where financing = _financing 
			and (_operation_number is null or operation_number = _operation_number)
			and (_created_from is null or _created_from::date <= (created->>'date')::date)
			and (_created_to is null or _created_to::date >= (created->>'date')::date)
			and (_accepted_department_id is null or _accepted_department_id = department_id)
		    ORDER BY operation_number
		), 
		main2 AS (
		    SELECT 
		        operation_number,
		        jsonb_agg(
		            jsonb_build_object(
		                'key', id,
		                'id', id,
		                'name_id', name_id,
		                'quantity', quantity,
		                'unit_price', unit_price,
		                'import_id', import_id,
		                'credit', credit,
		                'inventory_number', inventory_number,
		                'department_id', department_id,
		                'staff_id', staff_id,
		                'depreciation', depreciation,
		                'depreciation_percent', depreciation_percent,
		                'depreciation_period', depreciation_period
		            ) ORDER BY name_id
		        ) AS table_data
		    FROM accounting.assets_recognition
		    GROUP BY operation_number
		),
		main as (
			SELECT 
				row_number() over(order by m1.operation_number) as key,
			    m1.*,
			    m2.table_data
			FROM main1 m1
			JOIN main2 m2 
			USING (operation_number)
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
				where warehouse_section = 'assets_recognition' 
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
				'operation_number', m.operation_number,
				'main_department_id', m.main_department_id,
				'committee', m.committee,
				'comment', m.comment,
				'financing', financing,
				'status', status,
				'created_date', m.created_date,				
				'table_data', m.table_data,
				'routing', r.routing
			)
		) into _result
		from main m 
		left join routing_2 r 
		on m.operation_number = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'total', total,
			'results', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_assets_recognition_by_id(
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
		    SELECT DISTINCT ON (operation_number)
				operation_number,
				main_department_id,
				financing,
				committee,
				comment,
				status,
				(created->>'date')::date as created_date
			from accounting.assets_recognition
			where operation_number = _id 
		), 
		main2 AS (
		    SELECT 
		        operation_number,
		        jsonb_agg(
		            jsonb_build_object(
						'key', id,
		                'id', id,
		                'name_id', name_id,
		                'quantity', quantity,
		                'unit_price', unit_price,
		                'import_id', import_id,
		                'credit', credit,
		                'inventory_number', inventory_number,
		                'department_id', department_id,
		                'staff_id', staff_id,
		                'depreciation', depreciation,
		                'depreciation_percent', depreciation_percent,
		                'depreciation_period', depreciation_period
		            ) ORDER BY name_id
		        ) AS table_data
		    FROM accounting.assets_recognition
			where operation_number = _id
		    GROUP BY operation_number
		),
		main as (
			SELECT 
			    m1.*,
			    m2.table_data
			FROM main1 m1
			JOIN main2 m2 
			USING (operation_number)
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
				where warehouse_section = 'assets_recognition' 
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
			'operation_number', m.operation_number,
			'main_department_id', m.main_department_id,
			'committee', m.committee,
			'comment', m.comment,
			'financing', financing,
			'status', status,
			'created_date', m.created_date,				
			'table_data', m.table_data,
			'routing', r.routing
		) into _result
		from main m 
		left join routing_2 r 
		on m.operation_number = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'result', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_assets_recognition_id()
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select operation_number 
		into _last_id
		from accounting.assets_recognition
		GROUP by operation_number
		order by operation_number desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;
