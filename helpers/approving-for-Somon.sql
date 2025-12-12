
WITH pp AS (
    SELECT id
    FROM pension.pensioner
    WHERE updated IS NOT NULL
      AND pension_obj IS NOT NULL
	  and status <> 'approved'
	--   and id in (1971)
	-- limit 5
)
SELECT pension.upsert_pension_routing(
    jsonb_build_object(
        'pensioner_id', pp.id,
        'jobposition_id', 134,
        'department_id', 160,
        'status', 'approved'
    )
)
FROM pp;

SELECT count(*)
FROM pension.pensioner
WHERE updated IS NOT NULL
  AND pension_obj IS NOT NULL
  and status = 'approved'


select * from pension.pensioner
where pension_number in (12882)


select *
from pension.pension_routing;