





-- select * from pension.pensioner pp


select * from pension.get_pensioners_vedemost (
	'pensioners',
	null,
	null,
	null,
	null,
	null,
	null,
	null,
	7,
	2
);


CREATE OR REPLACE FUNCTION pension.get_pensioners_vedemost(
	vedomost_type text,
	_pension_type integer DEFAULT NULL::integer,
	_with_restrictions boolean DEFAULT NULL::boolean,
	_sum_from numeric DEFAULT NULL::numeric,
	_sum_to numeric DEFAULT NULL::numeric,
	_retention_type pension.retention_types DEFAULT NULL::pension.retention_types,
	regions_ids text DEFAULT NULL::text,
	created_by text DEFAULT NULL::text,
	_limit integer DEFAULT 500,
	_offset integer DEFAULT 0
	)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
	_regions_ids bigint[] := (
  		SELECT array_agg(value::bigint)
  		FROM jsonb_array_elements_text(regions_ids::jsonb)
	);
	_result jsonb;
	total bigint;
BEGIN

	if vedomost_type = 'pensioners' then
		-- total
		select count(*) into total
		from (
			select 
				*,
				CASE 
		            WHEN (pension_obj->>'restricted_pension')::bool IS TRUE 
		            THEN ROUND(COALESCE((pension_obj->>'restricted_pension_amount')::numeric, 0) / 100.0, 2)
		            ELSE ROUND(COALESCE((pension_obj->>'pension_amount')::numeric, 0) / 100.0, 2)
		        END AS pension_amount_calc
			from pension.pensioner
		) pp
		LEFT JOIN (
			SELECT * FROM (
				SELECT 
					staff_id,
					region_id,
					ROW_NUMBER() OVER (
						PARTITION BY staff_id
						ORDER BY 
							CASE 
								WHEN end_date IS NULL THEN 1 
								ELSE 2 
							END, 
							end_date DESC
					) AS row_rank
				FROM hr.jobposition
				WHERE disabled IS NULL
			) WHERE row_rank = 1
		) j on pp.staff_id = j.staff_id
		WHERE pp.pension_obj is not null 
		-- AND pp.status = 'approved'
		and (pp.pension_end_date IS NULL OR pp.pension_end_date >= current_date)
		AND (_pension_type IS NULL OR pp.pension_type = _pension_type)
		AND (
			_with_restrictions IS NULL 
			OR 
			(pp.pension_obj->>'restricted_pension')::bool IS NOT DISTINCT FROM _with_restrictions
		) AND (_sum_from IS NULL OR pp.pension_amount_calc >= _sum_from)
		AND (_sum_to IS NULL OR pp.pension_amount_calc <= _sum_to)
		AND (created_by is null or (pp.created->>'user_id')::uuid = created_by::uuid)
		AND (_regions_ids is null or j.region_id = any(_regions_ids));

		-- data
		WITH pension_with_amount AS (
		    SELECT 
				row_number() OVER (ORDER BY pp.pension_number) AS rn,
		        pp.pension_number,
		        pp.bank_account,
		        case 
					when pp.pension_type = 2
					then concat_ws(' ', 
						pp.details->>'spouseLastname', 
						pp.details->>'spouseFirstname', 
						pp.details->>'spouseMiddlename'
					) else s.fullname
				end as fullname,					
				pp.pension_amount_calc,
				pp.registration_date_is_valid
		    FROM (
				select 
					*,
					CASE 
			            WHEN (pension_obj->>'restricted_pension')::bool IS TRUE 
			            THEN ROUND(COALESCE((pension_obj->>'restricted_pension_amount')::numeric, 0) / 100.0, 2)
			            ELSE ROUND(COALESCE((pension_obj->>'pension_amount')::numeric, 0) / 100.0, 2)
			        END AS pension_amount_calc,
					case
						when registration_date is null or
							age(CURRENT_DATE, registration_date::date) <= interval '6 months' 
						then true else false
					end as registration_date_is_valid
				from pension.pensioner
			) pp
		    left JOIN (
		        SELECT id, CONCAT_WS(' ', lastname, firstname, middlename) AS fullname
		        FROM hr.staff
		    ) s ON pp.staff_id = s.id
			left JOIN (
				SELECT * FROM (
					SELECT 
						staff_id,
						region_id,
						ROW_NUMBER() OVER (
							PARTITION BY staff_id
							ORDER BY 
								CASE 
									WHEN end_date IS NULL THEN 1 
									ELSE 2 
								END, 
								end_date DESC
						) AS row_rank
					FROM hr.jobposition
					WHERE disabled IS NULL
				) WHERE row_rank = 1
			) j on pp.staff_id = j.staff_id
		    WHERE pp.pension_obj is not null 
			-- AND pp.status = 'approved'
			and (pp.pension_end_date IS NULL OR pp.pension_end_date >= current_date)
		    AND (_pension_type IS NULL OR pp.pension_type = _pension_type)
		    AND (
				_with_restrictions IS NULL 
				OR 
		        (pp.pension_obj->>'restricted_pension')::bool IS NOT DISTINCT FROM _with_restrictions
			) AND (created_by is null or (pp.created->>'user_id')::uuid = created_by::uuid)
			AND (_regions_ids is null or j.region_id = any(_regions_ids))
			and (_sum_from IS NULL OR pp.pension_amount_calc >= _sum_from)
			and (_sum_to IS NULL OR pp.pension_amount_calc <= _sum_to)
			ORDER BY pension_number
			limit _limit offset _offset
		)
		SELECT jsonb_agg(
		    jsonb_build_object(
				'key', rn,
		        'pension_number', pension_number,
		        'bank_account', bank_account,
		        'pension_amount', pension_amount_calc,
		        'fullname', fullname,
				'registration_date_is_valid', registration_date_is_valid
		    )
		) INTO _result FROM pension_with_amount;
		
	elsif vedomost_type = 'retentions' then
		-- total
		select count(*) into total 
		from pension.pensioner_retention pr
		left join (
			select id, staff_id, pension_end_date, 
				status, pension_type, created
			from pension.pensioner
		) p on pr.pensioner_id = p.id
		left JOIN (
			SELECT * FROM (
				SELECT 
					staff_id,
					region_id,
					ROW_NUMBER() OVER (
						PARTITION BY staff_id
						ORDER BY 
							CASE 
								WHEN end_date IS NULL THEN 1 
								ELSE 2 
							END, 
							end_date DESC
					) AS row_rank
				FROM hr.jobposition
				WHERE disabled IS NULL
			) WHERE row_rank = 1
		) j on p.staff_id = j.staff_id
		WHERE pr.retention_type <> 'overpay' 
		-- AND p.status = 'approved'
		AND (_retention_type IS NULL OR pr.retention_type = _retention_type)
		AND (p.pension_end_date IS NULL OR p.pension_end_date >= current_date)
		AND (_pension_type IS NULL OR p.pension_type = _pension_type)
		AND (created_by is null or (p.created->>'user_id')::uuid = created_by::uuid)
		AND (_regions_ids is null or j.region_id = any(_regions_ids))
		AND (_sum_from IS NULL OR pr.amount >= _sum_from)
		AND (_sum_to IS NULL OR pr.amount <= _sum_to);

		-- data
		with rtns as (
			select jsonb_build_object(
				'key', row_number() OVER (ORDER BY pp.pension_number),
			    'pension_number', p.pension_number,
			    'bank_account', pr.bank_account,
			    'pension_amount', pr.amount,
			    'fullname', pr.receiver_fullname,
			    'retention_type', pr.retention_type,
				'registration_date_is_valid', case
					when 
						p.registration_date is null or
						age(CURRENT_DATE, p.registration_date::date) <= interval '6 months' 
					then true else false
				end
			) aggregated from pension.pensioner_retention pr
			left join (
				select 
					id, 
					pension_number, 
					registration_date, 
					pension_end_date,
					status,
					pension_type,
					staff_id,
					created
				from pension.pensioner
			) p on pr.pensioner_id = p.id
			left JOIN (
				SELECT * FROM (
					SELECT 
						staff_id,
						region_id,
						ROW_NUMBER() OVER (
							PARTITION BY staff_id
							ORDER BY 
								CASE 
									WHEN end_date IS NULL THEN 1 
									ELSE 2 
								END, 
								end_date DESC
						) AS row_rank
					FROM hr.jobposition
					WHERE disabled IS NULL
				) WHERE row_rank = 1
			) j on p.staff_id = j.staff_id
			WHERE pr.retention_type <> 'overpay'
			-- AND p.status = 'approved'
			AND (_retention_type IS NULL OR pr.retention_type = _retention_type)
			AND (p.pension_end_date IS NULL OR p.pension_end_date >= current_date)
			AND (_pension_type IS NULL OR p.pension_type = _pension_type)
			AND (created_by is null or (p.created->>'user_id')::uuid = created_by::uuid)
			AND (_regions_ids is null or j.region_id = any(_regions_ids))
			AND (_sum_from IS NULL OR pr.amount >= _sum_from)
			AND (_sum_to IS NULL OR pr.amount <= _sum_to)
			ORDER BY p.pension_number
			limit _limit offset _offset
		) select jsonb_agg(rtns.aggregated) into _result from rtns;
	end if;

	return jsonb_build_object(
		'results', _result,
		'total', total
	);
END;
$BODY$;









