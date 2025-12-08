
select * from accounting.warehouse_outgoing;

select * from commons.nomenclature

select accounting.get_warehouse_products_ware (
'
	{
    "financing": "budget"
}
'
);

CREATE OR REPLACE FUNCTION accounting.get_warehouse_products_ware(
	jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
    _financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
    _department_id int = (jdata->>'department_id')::int;
    _staff_id int = (jdata->>'staff_id')::int;
    _date_from date = (jdata->>'date_from')::date;
    _date_to date = (jdata->>'date_to')::date;
    _low_cost bool = (jdata->>'low_cost')::bool;
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

	-- total
	select count(*) into total 
	from accounting.warehouse_outgoing wo
	left join (
		select 
			id, 
			product_category_id,
			low_cost,
			ware_percent
		from commons.nomenclature
	) n on wo.name_id = n.id
	where wo.status = 'approved'
	and n.low_cost is true
	and wo.financing = _financing
	and (_department_id is null or _department_id = wo.department_id)
	and (_staff_id is null or _staff_id = wo.staff_id)
	and (products is null or wo.name_id = any(products))
	and (categories IS NULL OR categories && n.product_category_id)
	and (_date_from is null or wo.issued_date::date >= _date_from)
	and (_date_to is null or wo.issued_date::date <= _date_to)
	and n.ware_percent > 0
	and (
		wo.quantity * coalesce(wo.unit_price, 0) -- intial price
		-
		wo.quantity * coalesce(wo.unit_price, 0) -- intial price 
		* 
		(n.ware_percent / 100) -- percent to portion convertion
		*
		extract(year from age(coalesce(_date_to, current_date), wo.issued_date::date)) -- years in numbers
	) > 0;
	
	-- main query
	with wares as (
		SELECT
			wo.id,
			wo.name_id,
			wo.issued_date::date as issued_date,
			wo.quantity,
			wo.unit_price,
			n.ware_percent,
			
			wo.quantity * coalesce(wo.unit_price, 0) as init_price,
			n.ware_percent / 100 as ware_portion,
			extract(year from age(coalesce(_date_to, current_date), wo.issued_date::date)) as years
		FROM accounting.warehouse_outgoing wo
		left join (
			select 
				id, 
				product_category_id,
				low_cost,
				ware_percent
			from commons.nomenclature
		) n on wo.name_id = n.id
		where wo.status = 'approved'
		and n.low_cost is true
		and wo.financing = _financing
		and (_department_id is null or _department_id = wo.department_id)
		and (_staff_id is null or _staff_id = wo.staff_id)
		and (products is null or wo.name_id = any(products))
		and (categories IS NULL OR categories && n.product_category_id)
		and (_date_from is null or wo.issued_date::date >= _date_from)
		and (_date_to is null or wo.issued_date::date <= _date_to)
		and n.ware_percent > 0
		and (
			wo.quantity * coalesce(wo.unit_price, 0) -- intial price
			-
			wo.quantity * coalesce(wo.unit_price, 0) -- intial price 
			* 
			(n.ware_percent / 100) -- percent to portion convertion
			*
			extract(year from age(coalesce(_date_to, current_date), wo.issued_date::date)) -- years in numbers
		) > 0
		order by wo.issued_date desc
		limit _limit offset _offset
	),
	filtered as (
		select jsonb_build_object(
			'id', id,
			'name_id', name_id,
			'issued_date', issued_date,
			'quantity', quantity,
			'unit_price', unit_price,
			'total_price',  init_price,
			'ware_percent', ware_percent,
			'years', years,
			
			'prev_ware_amount', round(greatest(init_price * ware_portion * (years - 1), 0), 2),
			'ware_amount', round(init_price * ware_portion, 2),
			'total_ware_amount', round(init_price * ware_portion * years, 2),
			'left_price', round(init_price - init_price * ware_portion * years, 2)
		) obj from wares
	)
	select jsonb_agg(f.obj) from filtered f into _result;	

	return jsonb_build_object(
		'status', 200,
		'results', _result,
		'total', total
	); 
end;
$BODY$;