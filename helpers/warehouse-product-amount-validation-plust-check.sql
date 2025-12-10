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
			sum(quantity) as quantity,
			min((created->>'date')::date) as created_date,
			min(credit) as credit
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
			quantity,
			moved_at::date as created_date,
			credit
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
	        SUM(quantity) AS quantity,
			min(created_date) as created_date,
			min(credit) as credit
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
	        COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) quantity,
			mi.created_date,
			mi.credit
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






CREATE OR REPLACE FUNCTION accounting.warehouse_check_product_amount(
	_location_id bigint,
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

	-- 1. Main warehouse
	with main_warehouse as (
		SELECT 
			name_id,
			unique_import_number as import_id,
			unit_price,
			storage_location_id as to_storage_location_id,
			null::bigint as from_storage_location_id,
			sum(quantity) as quantity,
			min((created->>'date')::date) as created_date,
			min(credit) as credit
		FROM accounting.warehouse_incoming
		where name_id = _name_id
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
			quantity,
			moved_at::date as created_date,
			credit
		FROM accounting.product_transfer
		WHERE name_id = _name_id
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
	        SUM(quantity) AS quantity,
			min(created_date) as created_date,
			min(credit) as credit
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
	        COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) quantity,
			mi.created_date,
			mi.credit
	    FROM movement_incoming mi
	    FULL JOIN movement_outgoing mo
	    ON mi.name_id = mo.name_id
	    AND mi.import_id = mo.import_id
	    AND mi.unit_price = mo.unit_price
	    AND mi.location_id = mo.location_id
		where COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) > 0
		and COALESCE(mi.location_id, mo.location_id) = _location_id
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
		and financing = _financing
		-- and status = 'approved'
		group by name_id, import_id, unit_price, storage_location_id
	),
	-- 8. We calculate final numbers
	left_quantities as (
		select jsonb_build_object(
		    'key', ROW_NUMBER() OVER (ORDER BY mc.location_id),
		    'name_id', mc.name_id,
		    'import_id', mc.import_id,
		    'unit_price', mc.unit_price,
		    'storage_location_id', mc.location_id,
		    'left_quantity', COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0),
		    'credit', mc.credit,
		    'created_date', mc.created_date
		) as aggregated 
		from movement_combined mc
		left join warehouse_exports we
		ON mc.name_id = we.name_id
	    AND mc.import_id = we.import_id
	    AND mc.unit_price = we.unit_price
	    AND mc.location_id = we.location_id
		where COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0) > 0
		order by mc.location_id
	)
	select jsonb_agg(lq.aggregated) 
	into _result
	from left_quantities lq;

	return json_build_object(
		'status', 200,
		'results', _result
	);
end;
$BODY$;