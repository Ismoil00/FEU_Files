


select * from  accounting.warehouse_outgoing
	




CREATE OR REPLACE FUNCTION accounting.get_warehouse_export_state(jdata jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _financing     accounting.budget_distribution_type := (jdata->>'financing')::accounting.budget_distribution_type;
    _department_id int := (jdata->>'department_id')::int;
    _staff_id      int := (jdata->>'staff_id')::int;
    _date_from     date := (jdata->>'date_from')::date;
    _date_to       date := (jdata->>'date_to')::date;
    _low_cost      boolean := (jdata->>'low_cost')::boolean;
    _limit         int := COALESCE((jdata->>'limit')::int, 50);
    _offset        int := COALESCE((jdata->>'offset')::int, 0);

	products bigint[] := (
  		SELECT array_agg(value::bigint)
  		FROM jsonb_array_elements_text((jdata->>'products')::jsonb)
	);
	
    categories int[] = (
		SELECT array_agg(value::int)
  		FROM jsonb_array_elements_text((jdata->>'categories')::jsonb)
	);

    _result jsonb;
BEGIN
    WITH filtered AS (
        SELECT
            wo.id,
            wo.name_id,
            wo.quantity,
            wo.unit_price,
            wo.order_number,
            wo.doc_id,
            wo.department_id,
            wo.staff_id,
            (wo.created->>'date')::date AS created_date,
            n.low_cost,
            COUNT(*) OVER () AS total_count
        FROM accounting.warehouse_outgoing wo
        LEFT JOIN commons.nomenclature n 
			ON n.id = wo.name_id
        WHERE wo.financing = _financing
        AND (_department_id IS NULL OR wo.department_id = _department_id)
        AND (_staff_id IS NULL OR wo.staff_id = _staff_id)
        AND (products IS NULL OR wo.name_id = ANY(products))
		AND (categories IS NULL OR categories && n.product_category_id)
        AND (_date_from IS NULL OR (wo.created->>'date')::date >= _date_from)
        AND (_date_to IS NULL OR (wo.created->>'date')::date <= _date_to)
        AND (
			_low_cost IS NULL
			OR (_low_cost IS TRUE  AND n.low_cost IS TRUE)
			OR (_low_cost IS FALSE AND COALESCE(n.low_cost, FALSE) IS FALSE)
        )
    ),
    paged AS (
        SELECT 
			row_number() over(
				order by f.created_date DESC, f.order_number DESC
			) as key,
			f.*
        FROM filtered f
        ORDER BY f.created_date DESC, f.order_number DESC
        LIMIT _limit OFFSET _offset
    )
    SELECT jsonb_build_object(
        'status', 200,
        'total', COALESCE(MAX(total_count), 0),
        'results', COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'id', id,
                    'name_id', name_id,
                    'department_id', department_id,
                    'staff_id', staff_id,
                    'quantity', round(quantity, 2),
                    'unit_price', round(COALESCE(unit_price, 0), 2),
                    'total_price', round(quantity * COALESCE(unit_price, 0), 2),
                    'low_cost', low_cost,
                    'createdAt', created_date,
                    'order_number', order_number,
                    'doc_id', doc_id
                )
            ),
            '[]'::jsonb
        )
    )
    INTO _result
    FROM paged;

    RETURN _result;
END;
$BODY$;





