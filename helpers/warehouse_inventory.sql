CREATE OR REPLACE FUNCTION accounting.get_warehouse_inventory(jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
    _date_from date = (jdata->>'date_from')::date;
    _date_to date = (jdata->>'date_to')::date;
    _low_cost boolean = (jdata->>'low_cost')::boolean;
    _limit int = COALESCE((jdata->>'limit')::int, 100);
    _offset int = COALESCE((jdata->>'offset')::int, 0);
    
    products bigint[] := CASE 
        WHEN jdata->>'products' IS NULL OR jdata->'products' = 'null'::jsonb THEN NULL
        ELSE (
            SELECT array_agg(value::bigint)
            FROM jsonb_array_elements_text(jdata->'products')
            WHERE value ~ '^\d+$'
        )
    END;
    
    categories int[] := CASE 
        WHEN jdata->>'categories' IS NULL OR jdata->'categories' = 'null'::jsonb THEN NULL
        ELSE (
            SELECT array_agg(value::int)
            FROM jsonb_array_elements_text(jdata->'categories')
            WHERE value ~ '^\d+$'
        )
    END;
    
    locations bigint[] := CASE 
        WHEN jdata->>'locations' IS NULL OR jdata->'locations' = 'null'::jsonb THEN NULL
        ELSE (
            SELECT array_agg(value::bigint)
            FROM jsonb_array_elements_text(jdata->'locations')
            WHERE value ~ '^\d+$'
        )
    END;
    
    _result jsonb;
    _total int;
BEGIN

    -- 1. Main warehouse
    WITH main_warehouse AS (
        SELECT 
            name_id,
            unique_import_number AS import_id,
            unit_price,
            storage_location_id AS to_storage_location_id,
            NULL::bigint AS from_storage_location_id,
            SUM(quantity) AS quantity,
            MIN((created->>'date')::date) AS created_date,
            MIN(credit) AS credit
        FROM accounting.warehouse_incoming
        WHERE financing = _financing
            AND (products IS NULL OR name_id = ANY(products))
            AND (locations IS NULL OR storage_location_id = ANY(locations))
        GROUP BY name_id, unique_import_number, unit_price, storage_location_id
    ),
    
	-- 2. Movements history
    movements AS (
        SELECT
            name_id,
            import_id,
            unit_price,
            to_storage_location_id,
            from_storage_location_id,
            quantity,
            moved_at::date AS created_date,
            credit
        FROM accounting.product_transfer
        WHERE financing = _financing
            AND (products IS NULL OR name_id = ANY(products))
            AND (
                locations IS NULL 
                OR from_storage_location_id = ANY(locations)
                OR to_storage_location_id = ANY(locations)
            )
    ),
    
    -- 3. All the warehouses
    all_warehouses AS (
        SELECT * FROM main_warehouse
        UNION ALL
        SELECT * FROM movements
    ),
    
    -- 4. Warehouses' incoming movements
    movement_incoming AS (
        SELECT
            name_id,
            import_id,
            unit_price,
            to_storage_location_id AS location_id,
            SUM(quantity) AS quantity,
            MIN(created_date) AS created_date,
            MIN(credit) AS credit
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
    
    -- 7. Exports count
    warehouse_exports AS (
        SELECT 
            name_id,
            import_id,
            unit_price,
            storage_location_id AS location_id,
            SUM(quantity) AS quantity
        FROM accounting.warehouse_outgoing
        WHERE financing = _financing
            AND (products IS NULL OR name_id = ANY(products))
            AND (locations IS NULL OR storage_location_id = ANY(locations))
        GROUP BY name_id, import_id, unit_price, storage_location_id
    ),
    
    -- 8. We calculate final numbers
    final_results AS (
        SELECT 
            mc.name_id,
            mc.import_id,
            mc.unit_price,
            COALESCE(mc.quantity, 0) - COALESCE(we.quantity, 0) AS left_quantity,
            mc.location_id,
            mc.created_date,
            n.low_cost,
            n.product_category_id
        FROM movement_combined mc
        LEFT JOIN warehouse_exports we
            ON mc.name_id = we.name_id
            AND mc.import_id = we.import_id
            AND mc.unit_price = we.unit_price
            AND mc.location_id = we.location_id
        LEFT JOIN (
            SELECT 
                id, 
                product_category_id,
                low_cost
            FROM commons.nomenclature
        ) n ON mc.name_id = n.id
    ),
    
    -- 9. Filtered results (for counting)
    filtered_results AS (
        SELECT *
        FROM final_results
        WHERE left_quantity > 0
            AND (_date_from IS NULL OR created_date >= _date_from)
            AND (_date_to IS NULL OR created_date <= _date_to)
            AND (categories IS NULL OR categories && product_category_id)
            AND (
                _low_cost IS NULL 
                OR (low_cost IS NOT TRUE AND _low_cost IS FALSE) 
                OR (low_cost IS TRUE AND _low_cost IS TRUE)
            )
    ),
    
    -- 10. Total count (FIXED: Count filtered results)
    total_count AS (
        SELECT COUNT(*) AS cnt 
        FROM filtered_results
    ),
    
    -- 11. Paginated results
    paginated AS (
        SELECT jsonb_build_object(
            'key', ROW_NUMBER() OVER (ORDER BY created_date, location_id DESC),
            'name_id', name_id,
            'unit_price', ROUND(unit_price::numeric, 2),
            'left_quantity', ROUND(left_quantity::numeric, 2),
            'storage_location_id', location_id,
            'created_date', created_date,
            'low_cost', low_cost
        ) AS aggregated
        FROM filtered_results
        ORDER BY created_date, location_id DESC
        LIMIT _limit 
        OFFSET _offset
    )
    
    -- 12. Pack everything for return
    SELECT jsonb_build_object(
        'status', 200,
        'results', jsonb_agg(p.aggregated),
        'total', (SELECT cnt FROM total_count)
    ) INTO _result
    FROM paginated p;
    
    RETURN _result;
END;
$BODY$;