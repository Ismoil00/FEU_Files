CREATE OR REPLACE FUNCTION accounting.get_warehouse_products_ware(
	jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
	 _main_department_id integer = (jdata->>'main_department_id')::integer;
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
		left join commons.department d
			on wo.main_department_id = d.id
		LEFT JOIN (
			SELECT 
				id, 
				product_category_id
			FROM commons.nomenclature
		) n ON wo.name_id = n.id
		WHERE wo.depreciation IS TRUE
		AND (wo.main_department_id = _main_department_id or _main_department_id = d.parent_id)
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
			row_number() over(order by issued_date) as key,
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
			'key', key,
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
	left join commons.department d
		on wo.main_department_id = d.id
	LEFT JOIN (
		SELECT id, product_category_id
		FROM commons.nomenclature
	) n ON wo.name_id = n.id
	WHERE wo.depreciation IS TRUE
	AND (wo.main_department_id = _main_department_id or _main_department_id = d.parent_id)
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