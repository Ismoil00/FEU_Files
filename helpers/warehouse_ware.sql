












select accounting.check_and_update_exported_assets()

select * FROM accounting.assets_recognition;



CREATE OR REPLACE FUNCTION accounting.check_and_update_exported_assets()
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _asset_record RECORD;
    _total_available numeric;
    _left_quantity numeric;
BEGIN
    -- Loop through all non-exported assets_recognition records
    FOR _asset_record IN 
        SELECT 
            id,
            name_id,
            import_id,
            unit_price,
            financing,
            quantity
        FROM accounting.assets_recognition
        WHERE exported IS NOT TRUE
        -- AND status = 'approved'
    LOOP
        -- Calculate total available quantity across ALL warehouses
        WITH main_warehouse AS (
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
            WHERE name_id = _asset_record.name_id
            AND unique_import_number = _asset_record.import_id
            AND unit_price = _asset_record.unit_price
            AND financing = _asset_record.financing
            GROUP BY name_id, unique_import_number, unit_price, storage_location_id
        ),
        movements AS (
            SELECT
                name_id,
                import_id,
                unit_price,
                to_storage_location_id,
                from_storage_location_id,
                quantity,
                moved_at::date as created_date,
                credit
            FROM accounting.product_transfer
            WHERE name_id = _asset_record.name_id
            AND import_id = _asset_record.import_id
            AND unit_price = _asset_record.unit_price
            AND financing = _asset_record.financing
        ),
        all_warehouses AS (
            SELECT * FROM main_warehouse
            UNION ALL
            SELECT * FROM movements
        ),
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
        movement_combined AS (
            SELECT
                COALESCE(mi.name_id, mo.name_id) AS name_id,
                COALESCE(mi.import_id, mo.import_id) AS import_id,
                COALESCE(mi.unit_price, mo.unit_price) AS unit_price,
                COALESCE(mi.location_id, mo.location_id) AS location_id,
                COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) AS quantity,
                mi.created_date,
                mi.credit
            FROM movement_incoming mi
            FULL JOIN movement_outgoing mo
            ON mi.name_id = mo.name_id
            AND mi.import_id = mo.import_id
            AND mi.unit_price = mo.unit_price
            AND mi.location_id = mo.location_id
            WHERE COALESCE(mi.quantity, 0) - COALESCE(mo.quantity, 0) > 0
        ),
        warehouse_exports AS (
            SELECT 
                name_id,
                import_id,
                unit_price,
                storage_location_id as location_id,
                sum(quantity) AS quantity
            FROM accounting.warehouse_outgoing
            WHERE name_id = _asset_record.name_id
            AND import_id = _asset_record.import_id
            AND unit_price = _asset_record.unit_price
            AND financing = _asset_record.financing
            GROUP BY name_id, import_id, unit_price, storage_location_id
        ),
        warehouse_availability AS (
            SELECT 
                SUM(COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0)) AS total_available
            FROM movement_combined mc
            LEFT JOIN warehouse_exports we
            ON mc.name_id = we.name_id
            AND mc.import_id = we.import_id
            AND mc.unit_price = we.unit_price
            AND mc.location_id = we.location_id
        )
        SELECT COALESCE(total_available, 0) 
        INTO _total_available
        FROM warehouse_availability;

		raise notice '_total_available %', _total_available;
		raise notice '____________________________________';

        -- If no quantity available in any warehouse, mark as exported
        IF _total_available IS NULL OR _total_available <= 0 THEN
            UPDATE accounting.assets_recognition
            SET exported = true
            WHERE id = _asset_record.id;
        END IF;
    END LOOP;
END;
$BODY$;





select accounting.get_warehouse_products_ware (
	'
		{
		    "products": null,
		    "financing": "budget",
		    "categories": null,
		    "date_from": null,
		    "date_to": "2026-05-05",
		    "limit": 1000,
		    "offset": 0
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
DECLARE
    _financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
    _department_id int = (jdata->>'department_id')::int;
    _staff_id int = (jdata->>'staff_id')::int;
    _date_from date = (jdata->>'date_from')::date;
    _date_to date = (jdata->>'date_to')::date;
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
BEGIN
	
	-- First, update exported status before calculating depreciation
	PERFORM accounting.check_and_update_exported_assets();
	
	-- Main query with monthly depreciation calculation
	WITH wares AS (
		SELECT
			wo.id,
			wo.name_id,
			(wo.created->>'date')::date as issued_date,
			round(wo.quantity, 2) AS quantity,
			round(coalesce(wo.unit_price, 0), 2) AS unit_price,
			wo.depreciation_percent,
			wo.quantity * coalesce(wo.unit_price, 0) AS init_price,
			round(wo.depreciation_percent / 100, 4) AS ware_portion,
			-- Calculate months: date difference in days / 30.44
			(coalesce(_date_to, current_date) - (wo.created->>'date')::date)::numeric / 30.44 AS months,
			-- Calculate full months (floor of the calculation)
			FLOOR((coalesce(_date_to, current_date) - (wo.created->>'date')::date)::numeric / 30.44) AS full_months
		FROM accounting.assets_recognition wo
		LEFT JOIN (
			SELECT 
				id, 
				product_category_id
			FROM commons.nomenclature
		) n ON wo.name_id = n.id
		WHERE wo.depreciation IS TRUE
		-- AND wo.status = 'approved'
		AND wo.exported IS NOT TRUE  -- Filter out exported products
		AND wo.financing = _financing
		AND (_department_id IS NULL OR _department_id = wo.department_id)
		AND (_staff_id IS NULL OR _staff_id = wo.staff_id)
		AND (products IS NULL OR wo.name_id = ANY(products))
		AND (categories IS NULL OR categories && n.product_category_id)
		AND (_date_from IS NULL OR (wo.created->>'date')::date >= _date_from)
		AND (_date_to IS NULL OR (wo.created->>'date')::date <= _date_to)
	),
	filtered_with_depreciation AS (
		SELECT 
			id,
			name_id,
			issued_date,
			quantity,
			unit_price,
			depreciation_percent,
			init_price,
			ware_portion,
			full_months,
			-- Display months: use full_months directly (only full months, no partial)
			full_months AS display_months,
			-- Display years: full_months / 12, rounded to 2 decimal places
			round(full_months::numeric / 12, 2) AS display_years,
			-- Yearly depreciation amount
			init_price * ware_portion AS yearly_depreciation_amount,
			-- Monthly depreciation amount (yearly / 12)
			init_price * ware_portion / 12 AS monthly_depreciation_amount,
			-- Total depreciation: monthly amount * number of full months
			init_price * ware_portion / 12 * full_months AS total_depreciation_amount,
			-- Previous period depreciation (up to last month)
			init_price * ware_portion / 12 * GREATEST(full_months - 1, 0) AS prev_ware_amount,
			-- Current month depreciation
			CASE 
				WHEN full_months > 0 THEN init_price * ware_portion / 12
				ELSE 0
			END AS ware_amount,
			-- Left price (initial - total depreciation)
			init_price - (init_price * ware_portion / 12 * full_months) AS left_price
		FROM wares
		WHERE init_price - (init_price * ware_portion / 12 * full_months) > 0
		ORDER BY issued_date DESC
		LIMIT _limit OFFSET _offset
	),
	filtered AS (
		SELECT jsonb_build_object(
			'id', id,
			'name_id', name_id,
			'issued_date', issued_date,
			'quantity', quantity,
			'unit_price', unit_price,
			'total_price', round(quantity * unit_price, 2),
			'ware_percent', depreciation_percent,
			'months', display_months,
			'years', display_years,
			'prev_ware_amount', round(GREATEST(prev_ware_amount, 0), 2),
			'ware_amount', round(GREATEST(ware_amount, 0), 2),
			'total_ware_amount', round(GREATEST(total_depreciation_amount, 0), 2),
			'left_price', round(GREATEST(left_price, 0), 2)
		) AS obj 
		FROM filtered_with_depreciation
	)
	SELECT jsonb_agg(f.obj) FROM filtered f INTO _result;
	
	-- Get total count for pagination
	SELECT COUNT(*) INTO total
	FROM accounting.assets_recognition wo
	LEFT JOIN (
		SELECT id, product_category_id
		FROM commons.nomenclature
	) n ON wo.name_id = n.id
	WHERE wo.depreciation IS TRUE
	-- AND wo.status = 'approved'
	AND wo.exported IS NOT TRUE
	AND wo.financing = _financing
	AND (_department_id IS NULL OR _department_id = wo.department_id)
	AND (_staff_id IS NULL OR _staff_id = wo.staff_id)
	AND (products IS NULL OR wo.name_id = ANY(products))
	AND (categories IS NULL OR categories && n.product_category_id)
	AND (_date_from IS NULL OR (wo.created->>'date')::date >= _date_from)
	AND (_date_to IS NULL OR (wo.created->>'date')::date <= _date_to)
	AND (
		wo.quantity * coalesce(wo.unit_price, 0) -- initial price
		- 
		wo.quantity * coalesce(wo.unit_price, 0) * (wo.depreciation_percent / 100) / 12  -- monthly depreciation
		* 
		FLOOR((coalesce(_date_to, current_date) - (wo.created->>'date')::date)::numeric / 30.44) -- full months
	) > 0;
	
	RETURN jsonb_build_object(
		'status', 200,
		'results', _result,
		'total', total
	); 
END;
$BODY$;







