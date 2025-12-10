CREATE OR REPLACE FUNCTION accounting.get_warehouse_state(
	jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
    product_location text = jdata->>'product_location';
    _financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
    _department_id int = (jdata->>'department_id')::int;
    _staff_id int = (jdata->>'staff_id')::int;
    _date_from date = (jdata->>'date_from')::date;
    _date_to date = (jdata->>'date_to')::date;
    _low_cost boolean = (jdata->>'low_cost')::boolean;
    _limit int = (jdata->>'limit')::int;
    _offset int = (jdata->>'offset')::int;
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
		SELECT COUNT(*) into total
		FROM accounting.warehouse_incoming wi
		left join (
		    select 
		        id, 
		        product_category_id,
		        low_cost
		    from commons.nomenclature
		) n on wi.name_id = n.id
		where wi.closed IS NOT true
		-- and wi.status = 'approved'
		and wi.financing = _financing
		and (products is null or wi.name_id = any(products))
		and (categories IS NULL OR categories && n.product_category_id)
		and (_date_from is null or (wi.created->>'date')::date >= _date_from)
		and (_date_to is null or (wi.created->>'date')::date <= _date_to)
		and (_low_cost is null or (
		        n.low_cost is not true and _low_cost is false
		    ) or (
		        n.low_cost is true and _low_cost is true
		    )
		)
		group by wi.name_id, wi.unit_price, 
		wi.unique_import_number, wi.order_number; -- do not make it shorter, this logic is made so intentionally
		
		-- main query
		with imports as (
			SELECT
				min(wi.id) id,
				wi.name_id,
				sum(wi.quantity) quantity,
				coalesce(sum(wi.unit_price), 0) unit_price,
				wi.unique_import_number,
				wi.order_number,
				min((wi.created->>'date')::date) createdAt,
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
			-- and wi.status = 'approved'
			and wi.financing = _financing
			and (products is null or wi.name_id = any(products))
			and (categories IS NULL OR categories && n.product_category_id)
			and (_date_from is null or (wi.created->>'date')::date >= _date_from)
			and (_date_to is null or (wi.created->>'date')::date <= _date_to)
			and (_low_cost is null or (
					n.low_cost is not true and _low_cost is false
				) or (
					n.low_cost is true and _low_cost is true
				)
			)
			group by wi.name_id, wi.unit_price, 
			wi.unique_import_number, wi.order_number, n.low_cost
		), exports as (
			SELECT 
				wo.name_id,
				wo.import_id,
				wo.unit_price,
				sum(wo.quantity) quantity
			FROM accounting.warehouse_outgoing wo
			left join (
				select 
					id, 
					product_category_id,
					low_cost
				from commons.nomenclature
			) n on wo.name_id = n.id
			where wo.closed IS NOT true
			-- and wo.status = 'approved'
			and wo.financing = _financing
			and (products is null or wo.name_id = any(products))
			and (categories IS NULL OR categories && n.product_category_id)
			and (_date_from is null or (wo.created->>'date')::date >= _date_from)
			and (_date_to is null or (wo.created->>'date')::date <= _date_to)
			and (_low_cost is null or (
					n.low_cost is not true and _low_cost is false
				) or (
					n.low_cost is true and _low_cost is true
				)
			)
			group by wo.name_id, wo.import_id, wo.unit_price
		),
		joining as (
			select jsonb_build_object(
				'id', i.id,
				'name_id', i.name_id,
				'quantity', round(i.quantity - COALESCE(e.quantity, 0), 2),
				'unit_price', round(i.unit_price, 2),
				'total_price', round((i.quantity - COALESCE(e.quantity, 0)) * i.unit_price, 2),
				'low_cost', i.low_cost,
				'createdAt', i.createdAt,
				'order_number', i.order_number
			) obj from imports i
			left join exports e
			on i.unique_import_number = e.import_id
				and i.name_id = e.name_id
				and i.unit_price = e.unit_price
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
				product_category_id,
				low_cost
			from commons.nomenclature
		) n on wo.name_id = n.id
		where wo.financing = _financing
		-- and wo.status = 'approved'
		and (_department_id is null or _department_id = wo.department_id)
		and (_staff_id is null or _staff_id = wo.staff_id)
		and (products is null or wo.name_id = any(products))
		and (categories IS NULL OR categories && n.product_category_id)
		and (_date_from is null or (wo.created->>'date')::date >= _date_from)
		and (_date_to is null or (wo.created->>'date')::date <= _date_to)
		and (_low_cost is null or (
				n.low_cost is not true and _low_cost is false
			) or (
				n.low_cost is true and _low_cost is true
			)
		);

		-- main query
		with exports as (
			SELECT jsonb_build_object(
				'id', wo.id,
				'name_id', wo.name_id,
				'quantity', round(wo.quantity, 2),
				'unit_price', round(coalesce(wo.unit_price, 0), 2),
				'total_price', round(wo.quantity * coalesce(wo.unit_price, 0), 2),
				'low_cost', n.low_cost,
				'createdAt', (wo.created->>'date')::date,
				'order_number', wo.order_number,
				'doc_id', wo.doc_id
			) obj
			FROM accounting.warehouse_outgoing wo
			left join (
				select 
					id, 
					product_category_id,
					low_cost
				from commons.nomenclature
			) n on wo.name_id = n.id
			where wo.financing = _financing
			-- and wo.status = 'approved'
			and (_department_id is null or _department_id = wo.department_id)
			and (_staff_id is null or _staff_id = wo.staff_id)
			and (products is null or wo.name_id = any(products))
			and (categories IS NULL OR categories && n.product_category_id)
			and (_date_from is null or (wo.created->>'date')::date >= _date_from)
			and (_date_to is null or (wo.created->>'date')::date <= _date_to)
			and (_low_cost is null or (
					n.low_cost is not true and _low_cost is false
				) or (
					n.low_cost is true and _low_cost is true
				)
			)
			order by (wo.created->>'date')::date, wo.order_number desc
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