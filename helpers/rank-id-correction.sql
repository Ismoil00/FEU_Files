
select * from pension.pensioner
where pension_number = 10786;

select * from hr.jobposition
where staff_id = 1034;

select * from commons.militaryrank;
-- 4 -> генераль майор
-- 6 -> подполковник
-- 48 -> старший проворщик
-- 17 -> младший сержант
-- 11 -> младший лейтенант



/* updating pensioners rank_id-s */
WITH pensioners AS (
    SELECT id
    FROM pension.pensioner
    WHERE pension_number IN (
		6821, 
		7426
    )
)
UPDATE pension.pensioner p SET 
	salary_obj = p.salary_obj || jsonb_build_object('rank_id', 11)
FROM pensioners pp
WHERE p.id = pp.id;



	
/* update jobposition rank_id-s */
WITH pensioners AS (
    SELECT staff_id
    FROM pension.pensioner
    WHERE pension_number IN (
		6821, 
		7426
	)
),
latest_finished AS (
    SELECT DISTINCT ON (j.staff_id)
        j.id
    FROM hr.jobposition j
    JOIN pensioners p ON p.staff_id = j.staff_id
    WHERE j.end_date IS NOT NULL
    ORDER BY j.staff_id, (j.created->>'date')::date DESC
)
UPDATE hr.jobposition jp SET 
	rank_id = 11,
	updated = jsonb_build_object(
		'date', localtimestamp(0),
		'user_id', jp.created->>'user_id'
	)
WHERE jp.id IN (SELECT id FROM latest_finished);





