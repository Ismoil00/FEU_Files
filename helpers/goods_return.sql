create table if not exists accounting.goods_return (
	id bigserial primary key,
	counterparty_id bigint not null references accounting.counterparty (id),
	contract text not null,
	
	storage_location_id bigint not null references commons.storage_location (id),
	main_department_id integer not null references commons.department (id),
	
	comment text,
	status commons.routing_status default 'pending',
	financing accounting.budget_distribution_type not null,
	table_data jsonb not null,
	created jsonb not null,
	updated jsonb
);


select * from accounting.goods_return;


CREATE OR REPLACE FUNCTION accounting.upsert_goods_return(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = 
			(jdata->>'financing')::accounting.budget_distribution_type;
		_table_data jsonb;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.goods_return
			where id = _id 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;

		/* TABLE DATA VALIDATION */
    	SELECT accounting.goods_return_table_data_validation(
    	  _user_id,
    	  (jdata->>'table_data')::jsonb,
    	  _id
    	) INTO _table_data;

		/* UPSERT */
		if _id is null then
			insert into accounting.goods_return (
				main_department_id,
				counterparty_id,
				contract,
				storage_location_id,
				comment,
				financing,
				table_data,
				created
			) values (
				(jdata->>'main_department_id')::integer,
				(jdata->>'counterparty_id')::bigint,
				(jdata->>'contract')::text,
				(jdata->>'storage_location_id')::bigint,
				(jdata->>'comment')::text,
				_financing,
				_table_data,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.goods_return gr SET
				main_department_id  = (jdata->>'main_department_id')::integer,
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				contract = (jdata->>'contract')::text,
				storage_location_id = (jdata->>'storage_location_id')::bigint,
				comment = (jdata->>'comment')::text,
				financing = _financing,
				table_data = _table_data,
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             gr.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE gr.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;

		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _id)
		);

		return json_build_object(
			'msg', case when _id is null then 'created' else 'updated' end,
			'status', 200,
			'id', _id
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_goods_return(
	_department_id integer,
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_created_from text DEFAULT NULL::text,
	_created_to text DEFAULT NULL::text,
	_counterparty_id bigint DEFAULT NULL::bigint,
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
		from accounting.goods_return
		where financing = _financing 
		and (_id is null or id = _id)
		and (_created_from is null or _created_from::date <= (created->>'date')::date)
		and (_created_to is null or _created_to::date >= (created->>'date')::date)
		and (_counterparty_id is null or _counterparty_id = counterparty_id);

		/* MAIN QUERY */
		with main as (
			select
				row_number() over(order by id) as key,
				id,
				main_department_id,
				counterparty_id,
				contract,
				storage_location_id,
				comment,
				financing,
				status,
				table_data,
				(created->>'date')::date as created_date
			from accounting.goods_return
			where financing = _financing 
			and (_id is null or id = _id)
			and (_created_from is null or _created_from::date <= (created->>'date')::date)
			and (_created_to is null or _created_to::date >= (created->>'date')::date)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
			order by id limit _limit offset _offset
		), routing_1 as (
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
				where warehouse_section = 'goods_return' 
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
				'id', m.id,
				'main_department_id', m.main_department_id,
				'counterparty_id', m.counterparty_id,
				'contract', m.contract,
				'storage_location_id', m.storage_location_id,
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
		on m.id = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'total', total,
			'results', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_goods_return_by_id(
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
		with main as (
			select 
				id,
				main_department_id,
				counterparty_id,
				contract,
				storage_location_id,
				comment,
				financing,
				status,
				table_data,
				(created->>'date')::date as created_date
			from accounting.goods_return
			where id = _id 
		), routing_1 as (
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
				where warehouse_section = 'goods_return' 
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
			'id', m.id,
			'main_department_id', m.main_department_id,
			'counterparty_id', m.counterparty_id,
			'contract', m.contract,
			'storage_location_id', m.storage_location_id,
			'comment', m.comment,
			'financing', financing,
			'status', status,
			'table_data', m.table_data,
			'created_date', m.created_date,
			'routing', r.routing
		) into _result
		from main m 
		left join routing_2 r 
		on m.id = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'result', _result
		);
	end;
$BODY$;







CREATE OR REPLACE FUNCTION accounting.get_goods_return_id(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN

		select id 
		into _last_id
		from accounting.goods_return
		order by id desc
		limit 1;
		
		return _last_id;
	end;
$BODY$;






select * from accounting.warehouse_total_routing

select * from accounting.warehouse_outgoing;

select * from accounting.goods_return;

select * from accounting.warehouse_incoming;

create or replace function accounting.goods_return_table_data_validation(
	_user_id text,
	_table_data jsonb,
	row_id bigint default null::bigint
)
returns jsonb
language 'plpgsql'
cost 100
volatile parallel unsafe
as $BODY$
DECLARE
	_row jsonb;
	_old_table_data jsonb;
	_old_row jsonb;
	_new_quantity numeric;
	_quantity_diff numeric;
	_updated_table_data jsonb = '[]'::jsonb;
	_name_id bigint;
	_import_id bigint;
	_price numeric;
	_updated jsonb = jsonb_build_object(
	  'user_id', _user_id,
	  'date', LOCALTIMESTAMP(0)
	);
BEGIN

	if row_id is not null then
	    -- old table_data
		SELECT table_data 
	    INTO _old_table_data
		FROM accounting.goods_return
		WHERE id = row_id;
	end if;

	FOR _row IN SELECT * FROM jsonb_array_elements(_table_data) LOOP
       	-- Extract values
		_name_id = (_row->>'name_id')::bigint;
		_import_id = (_row->>'import_id')::bigint;
		_price = (_row->>'price')::numeric;
		_new_quantity = (_row->>'quantity')::numeric;

        IF (_row->>'minused')::boolean is not true OR row_id IS NULL THEN
            -- Validate the quantity
			PERFORM accounting.warehouse_product_amount_validation(
				_name_id,
				_import_id,
				_new_quantity,
				_price
			);

            -- we update the warehouse [incoming, outgoing] tables
            update accounting.warehouse_incoming wi set 
            	quantity = wi.quantity - _new_quantity,
              	closed = case when wi.quantity - _new_quantity = 0 then true else false end,
              	updated = _updated
            where unique_import_number = _import_id
            and name_id = _name_id
            and unit_price = _price;

            -- minused = true + updated table_data
			_updated_table_data = _updated_table_data || jsonb_build_array(
				_row || jsonb_build_object('minused', true)
			);
		ELSE
            -- This is an old row - find it in old_table_data by id or key
			SELECT jsonb_array_elements.value 
			INTO _old_row
			FROM jsonb_array_elements(_old_table_data)
			WHERE (jsonb_array_elements.value->>'id')::text = (_row->>'id')::text
			OR (jsonb_array_elements.value->>'key')::text = (_row->>'key')::text
			LIMIT 1;
    
			-- if old row was not found
			IF _old_row IS NULL THEN
				RAISE EXCEPTION 'Старая запись не найдена в table_data для id=% или key=%', _row->>'id', _row->>'key' USING ERRCODE = 'P0002';
			END IF;

            _quantity_diff = _new_quantity - (_old_row->>'quantity')::numeric;
			IF _quantity_diff > 0 THEN
				-- Validating the difference
				PERFORM accounting.warehouse_product_amount_validation(
					_name_id,
					_import_id,
					_quantity_diff,
					_price
				);
  
              	-- we update the warehouse [incoming, outgoing] tables
              	update accounting.warehouse_incoming wi set 
              	  	quantity = wi.quantity - _quantity_diff,
              	  	closed = case when wi.quantity - _quantity_diff = 0 then true else false end,
              	  	updated = _updated
              	where unique_import_number = _import_id
              	and name_id = _name_id
              	and unit_price = _price;
  
			ELSIF _quantity_diff < 0 THEN
				-- we update the warehouse [incoming, outgoing] tables
				update accounting.warehouse_incoming wi set 
				  	quantity = wi.quantity + abs(_quantity_diff),
				  	closed = false,
				  	updated = _updated
				where unique_import_number = _import_id
				and name_id = _name_id
				and unit_price = _price;
              
			END IF;
  
			-- minused = true + updated table_data
			_updated_table_data = _updated_table_data || jsonb_build_array(
				_row || jsonb_build_object('minused', true)
			);
		END IF;
	END LOOP;
		
	RETURN _updated_table_data;
END;
$BODY$;


