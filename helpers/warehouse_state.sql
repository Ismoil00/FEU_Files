select * from accounting.warehouse_incoming;

select * from accounting.warehouse_outgoing;

select * from hr.notification

/* CAST THE product_category_id to INT[] */

CREATE OR REPLACE FUNCTION accounting.get_warehouse_state(jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
    product_location text = jdata->>'product_location';
    _financing text = jdata->>'financing';
    _department_id int = (jdata->>'department_id')::int;
    _staff_id int = (jdata->>'staff_id')::int;
    _date_from date = (jdata->>'date_from')::date;
    _date_to date = (jdata->>'date_to')::date;
    _low_cost bool = (jdata->>'low_cost')::bool;
    limit int = (jdata->>'limit')::int;
    offset int = (jdata->>'offset')::int;
	products bigint[] := (
  		SELECT array_agg(value::bigint)
  		FROM jsonb_array_elements_text((jdata->>'products')::jsonb)
	);
    categories int[] = (
		SELECT array_agg(value::int)
  		FROM jsonb_array_elements_text((jdata->>'categories')::jsonb)
	);
	
	_result json;
	total int;
begin

	IF product_location = 'in_warehouse' then
		-- total
		select count(*) into total 
		from accounting.warehouse_incoming wi
		left join (
			select 
				id, 
				product_category_id,
				low_cost
			from commons.nomenclature
		) n on wi.name_id = n.id
		where wi.closed IS NOT true
		and wi.status = 'approved'
		and wi.financing = _financing
		and (products is null or wi.name_id = any(products))
		and (categories IS NULL OR categories && n.product_category_id)
		and (_date_from is null or (wi.created->>'date')::date >= _date_from)
		and (_date_to is null or (wi.created->>'date')::date <= _date_to)
		and (_low_cost is not true or n.low_cost = _low_cost);
		
		-- main query
		with imports as (
			SELECT 
				wi.name_id,
				wi.quantity,
				wi.unit_price,
				wi.unique_import_number,
				wi.order_number,
				(wi.created->>'date')::date createdAt,
				wi.doc_id,
				n.low_cost
			FROM accounting.warehouse_incoming wi
			left join (
				select 
					id, 
					product_category_id,
					low_cost
				from commons.nomenclature
			) n on wi.name_id = n.id
			where wi.closed IS NOT true
			and wi.status = 'approved'
			and wi.financing = _financing
			and (products is null or wi.name_id = any(products))
			and (categories IS NULL OR categories && n.product_category_id)
			and (_date_from is null or (wi.created->>'date')::date >= _date_from)
			and (_date_to is null or (wi.created->>'date')::date <= _date_to)
			and (_low_cost is not true or n.low_cost = _low_cost)
		), exports as (
			SELECT 
				wo.name_id,
				wo.import_id,
				sum(wo.quantity)
			FROM accounting.warehouse_outgoing wo
			left join (
				select 
					id, 
					product_category_id
				from commons.nomenclature
			) n on wo.name_id = n.id
			where wo.closed IS NOT true
			and wo.status = 'approved'
			and wo.financing = _financing
			and (products is null or wo.name_id = any(products))
			and (categories IS NULL OR categories && n.product_category_id)
			and (_date_from is null or (wo.created->>'date')::date >= _date_from)
			and (_date_to is null or (wo.created->>'date')::date <= _date_to)
			and (_low_cost is not true or n.low_cost = _low_cost)
			group by wo.name_id, wo.import_id
		),
		joining as (
			select jsonb_build_object(
				'name_id', i.name_id,
				'quantity', coalesce(i.quantity, 0) - COALESCE(e.quantity, 0),
				'unit_price', i.unit_price,
				'low_cost', i.low_cost,
				'createdAt', i.createdAt,
				'order_number', i.order_number,
				'doc_id', i.doc_id
			) obj from imports i
			left join exports e
			on i.unique_import_number = e.import_id
			and i.name_id = e.name_id
			order by i.createdAt, i.order_number desc
			limit _limit offset _offset
		) 
		select jsonb_agg(j.obj) from joining j into _result;

	ELSIF product_location = 'in_use' or product_location = 'exported' then
		-- total
		select count(*) into total 
		from accounting.warehouse_outgoing wo
		left join (
			select 
				id, 
				product_category_id
			from commons.nomenclature
		) n on wo.name_id = n.id
		where wo.status = 'approved'
		and wo.financing = _financing
		and (_department_id is null or _department_id = wo.department_id)
		and (_staff_id is null or _staff_id = wo.staff_id)
		and (products is null or wo.name_id = any(products))
		and (categories IS NULL OR categories && n.product_category_id)
		and (_date_from is null or (wo.created->>'date')::date >= _date_from)
		and (_date_to is null or (wo.created->>'date')::date <= _date_to)
		and (_low_cost is not true or n.low_cost = _low_cost);

		-- main query
		with exports as (
			SELECT jsonb_build_object(
				'id', wo.id,
				'name_id', wo.name_id,
				'quantity', wo.quantity,
				'unit_price', wo.unit_price,
				'low_cost', n.low_cost,
				'createdAt', wo.issued_date,
				'order_number', wo.order_number,
				'doc_id', wo.doc_id
			) obj
			FROM accounting.warehouse_outgoing wo
			left join (
				select 
					id, 
					product_category_id
				from commons.nomenclature
			) n on wo.name_id = n.id
			where wo.status = 'approved'
			and wo.financing = _financing
			and (_department_id is null or _department_id = wo.department_id)
			and (_staff_id is null or _staff_id = wo.staff_id)
			and (products is null or wo.name_id = any(products))
			and (categories IS NULL OR categories && n.product_category_id)
			and (_date_from is null or (wo.created->>'date')::date >= _date_from)
			and (_date_to is null or (wo.created->>'date')::date <= _date_to)
			and (_low_cost is not true or n.low_cost = _low_cost)
			order by wo.issued_date, wo.order_number desc
			limit _limit offset _offset
		)
		select jsonb_agg(e.obj) from exports e into _result;
		
	else
		RAISE EXCEPTION 'Недопустимое значение поля "Место нахождения товара"' USING ERRCODE = 'P0001';
	end if;

	return jsonb_build_object(
		'status', 200,
		'results', _result,
		'total', total
	); 
end;
$BODY$;

------------------------------------------------

CREATE OR REPLACE FUNCTION accounting.get_warehouse_product_amount(
	_name_id bigint,
	_financing accounting.budget_distribution_type)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result jsonb;
begin

	with exports as (
		select 
			name_id,
			financing,
			import_id,
			sum(quantity) quantity
		from accounting.warehouse_outgoing
		where name_id = _name_id
		and closed IS NOT true
		and financing = _financing
		-- and status = 'approved'
		group by name_id, financing, import_id
	),
	imports as (
		SELECT 
			name_id,
			quantity,
			unit_price,
			financing,
			unique_import_number,
			order_number,
			(created->>'date')::date createdAt
		FROM accounting.warehouse_incoming
		where name_id = _name_id
		and closed IS NOT true
		and financing = _financing
		and status = 'approved'
	),
	joining as (
		select
			i.name_id,
			jsonb_agg(
				jsonb_build_object(
					'import_id', i.unique_import_number,
					'left_quantity', coalesce(i.quantity, 0) - COALESCE(e.quantity, 0),
					'unit_price', i.unit_price,
					'order_number', i.order_number,
					'year', extract(year from i.createdAt),
					'createdAt', i.createdAt
				) order by i.order_number, i.createdAt
			) aggregated
		from imports i
		left join exports e
		on i.unique_import_number = e.import_id
			and i.name_id = e.name_id 
			and i.financing = e.financing
		group by i.name_id
	) select jsonb_build_object(
		'name_id', j.name_id,
		'imports', j.aggregated
	) from joining j into _result;

	return json_build_object(
		'status', 200,
		'result', _result
	);
end;
$BODY$;