select * from auth.submodule
where module_id = 11



select * from auth.module
where id = 11



select * FROM accounting.warehouse_incoming;

select * FROM accounting.product_transfer;

select * FROM accounting.warehouse_outgoing;

select * FROM accounting.product_transfer;

select * FROM accounting.goods_return;

select * FROM accounting.inventory_entry;


select * FROM accounting.warehouse_total_routing;




select reports.get_material_statement (
	'budget',
	'2026-01-02',
	'2026-01-04',
	54321,
	100,
	0
);





CREATE OR REPLACE FUNCTION reports.get_material_statement(
	_financing accounting.budget_distribution_type,
	start_date text,
	end_date text,
	_debit integer DEFAULT null,
	_location_id bigint default null,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
	_date_from date = start_date::date;
    _date_to date = end_date::date;
BEGIN

	with main_warehouse as (
		SELECT 
			name_id,
			unit_price,
			storage_location_id as to_storage_location_id,
			null::bigint as from_storage_location_id,
			quantity,
			(created->>'date')::date as created_date
		FROM accounting.warehouse_incoming
		where financing = _financing
		and (_debit is null or _debit = debit)
		and (_location_id is null or _location_id = storage_location_id)
		-- and status = 'approved'
	),
	movements as (
		select
			name_id,
	        unit_price,
			to_storage_location_id,
			from_storage_location_id,
			quantity,
			moved_at::date as created_date
		FROM accounting.product_transfer
		WHERE financing = _financing
		and (_debit is null or _debit = debit)
		and (
			_location_id is null 
			or _location_id = to_storage_location_id 
			or _location_id = from_storage_location_id
		)
		-- and status = 'approved'
	),
	all_warehouses as (
		select * from main_warehouse
		UNION all
		select * from movements
	),
	movement_incoming AS (
	    SELECT
	        name_id,
	        unit_price,
	        to_storage_location_id AS location_id,
	        SUM(quantity) AS quantity,
			created_date
	    FROM all_warehouses
	    GROUP BY
			name_id, 
			unit_price, 
			to_storage_location_id,
			created_date
	),
	movement_outgoing AS (
	    SELECT
	        name_id,
	        unit_price,
	        from_storage_location_id AS location_id,
	        SUM(quantity) AS quantity,
			created_date
	    FROM all_warehouses
		WHERE from_storage_location_id IS NOT NULL
	    GROUP BY
			name_id, 
			unit_price, 
			from_storage_location_id,
			created_date
	),
	warehouse_exports as (
		select 
			name_id,
			unit_price,
			storage_location_id as location_id,
			sum(quantity) quantity,
			(created->>'date')::date as created_date
		from accounting.warehouse_outgoing
		where financing = _financing
		and (_debit is null or _debit = credit)
		and (_location_id is null or _location_id = storage_location_id)
		-- and status = 'approved'
		group by
			name_id, 
			unit_price, 
			storage_location_id,
			(created->>'date')::date
	),
	movement_combined AS (
	    SELECT
	        COALESCE(mi.name_id, mo.name_id) AS name_id,
	        COALESCE(mi.unit_price, mo.unit_price) AS unit_price,
	        COALESCE(mi.location_id, mo.location_id) AS location_id,
	        COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) quantity,
			COALESCE(mi.created_date, mo.created_date) AS created_date
	    FROM movement_incoming mi
	    FULL JOIN movement_outgoing mo
	    	ON mi.name_id = mo.name_id
	    	AND mi.unit_price = mo.unit_price
	    	AND mi.location_id = mo.location_id
		where COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) > 0
	),
	period_start as (
		select
			mc.name_id,
			mc.location_id,
			mc.unit_price,
			((COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0)) * COALESCE(mc.unit_price, 0)) as start_total_price,
			(COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0)) as start_quantity
		from (
			select
				name_id,
				location_id,
				unit_price,
				sum(quantity) quantity
			from movement_combined
			where created_date <= _date_from
			group by name_id, unit_price, location_id
		) mc
		left join (
			select 
				name_id,
				location_id,
				unit_price,
				sum(quantity) quantity
			from warehouse_exports
			where created_date <= _date_from
			group by name_id, unit_price, location_id
		) we
			ON mc.name_id = we.name_id
			AND mc.unit_price = we.unit_price
			AND mc.location_id = we.location_id
	),
	imports as (
		select
			name_id,
	        unit_price,
	        location_id,
	        sum(quantity * unit_price) as imported_total_price,
	        sum(quantity) as imported_quantity
		from movement_combined
		where created_date > _date_from
		and created_date < _date_to
		group by name_id, unit_price, location_id
	),
	exports as (
		select
			name_id,
	        unit_price,
	        location_id,
	        sum(quantity * unit_price) as exported_total_price,
			sum(quantity) as exported_quantity
		from warehouse_exports
		where created_date > _date_from
		and created_date < _date_to
		group by name_id, unit_price, location_id
	),
	period_end as (
		select 
			mc.name_id,
			mc.location_id,
			mc.unit_price,
			((COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0)) * COALESCE(mc.unit_price, 0)) as end_total_price,
			(COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0)) as end_quantity
		from (
			select
				name_id,
				location_id,
				unit_price,
				sum(quantity) quantity
			from movement_combined
			where created_date <= _date_to
			group by name_id, unit_price, location_id
		) mc
		left join (
			select 
				name_id,
				location_id,
				unit_price,
				sum(quantity) quantity
			from warehouse_exports
			where created_date <= _date_to
			group by name_id, unit_price, location_id
		) we
			ON mc.name_id = we.name_id
			AND mc.unit_price = we.unit_price
			AND mc.location_id = we.location_id
	),
	all_keys as (
		select name_id, unit_price, location_id from period_start 
		union 
		select name_id, unit_price, location_id from imports 
		union 
		select name_id, unit_price, location_id from exports 
		union 
		select name_id, unit_price, location_id from period_end 
	),
	all_combined as (
		select
			k.name_id, 
			k.unit_price, 
			k.location_id,
			
			ps.start_total_price,
			ps.start_quantity,
			i.imported_total_price,
	        i.imported_quantity,
			e.exported_total_price,
			e.exported_quantity,
			pe.end_total_price,
			pe.end_quantity
		from all_keys k 
		left join period_start ps 
			on k.name_id = ps.name_id 
			and k.unit_price = ps.unit_price 
			and k.location_id = ps.location_id 
		left join imports i 
			on k.name_id = i.name_id 
			and k.unit_price = i.unit_price 
			and k.location_id = i.location_id 
		left join exports e 
			on k.name_id = e.name_id 
			and k.unit_price = e.unit_price 
			and k.location_id = e.location_id 
		left join period_end pe 
			on k.name_id = pe.name_id 
			and k.unit_price = pe.unit_price 
			and k.location_id = pe.location_id
	),
	total_count as (
		select count(*) total from all_combined
	),
	ordered as (
		select jsonb_build_object(
			'key', row_number() over(order by location_id),
			'name_id', name_id,
			'unit_price', unit_price,
			'location_id', location_id,
			'start_total_price', start_total_price,
			'start_quantity', start_quantity,
			'imported_total_price', imported_total_price,
	        'imported_quantity', imported_quantity,
			'exported_total_price', exported_total_price,
			'exported_quantity', exported_quantity,
			'end_total_price', end_total_price,
			'end_quantity', end_quantity
		) as paginated
		from all_combined
		order by location_id
		limit _limit offset _offset
	)
	select jsonb_build_object(
		'status', 200,
		'total', (select total from total_count),
		'results', jsonb_agg(od.paginated)
	) into _result from ordered od;
	
	return _result;
end;
$BODY$;













