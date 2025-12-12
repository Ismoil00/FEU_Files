

/* ONLY INSPECTORS */
WITH routes AS (
    select * from (
		SELECT 
	        pensioner_id,
	        jobposition_id,
	        "createdAt",
	        ROW_NUMBER() OVER (
	            PARTITION BY pensioner_id 
	            ORDER BY "createdAt" DESC
	        ) AS rn
	    FROM (
			select * from pension.pension_routing
			where jobposition_id <> 134
		)
	) where rn = 1
),
pensioners as (
	select id, status
	from pension.pensioner
	where updated is not null
	and pension_obj is not null
),
joined as (
	select 
		p.id, 
		r.jobposition_id
	from pensioners p
	join routes r
	on p.id = r.pensioner_id
),
found_names as (
	select *
	from joined j
	left join (
		select 
			j.id, 
			concat_ws(' ', s.lastname, s.firstname, s.middlename) fullname 
		from hr.jobposition j
		join hr.staff s
		on j.staff_id = s.id
		where s.id in (select staff_id from auth.user)
	) f on j.jobposition_id = f.id
)
select 
	fn.fullname as "шумора",
	count(*) as "микдор"
from found_names fn
group by fn.fullname;







/* WITH SOMON APPROVED */
WITH routes AS (
    SELECT 
        pensioner_id,
        jobposition_id,
        "createdAt",
        ROW_NUMBER() OVER (
            PARTITION BY pensioner_id 
            ORDER BY "createdAt" DESC
        ) AS rn
    FROM pension.pension_routing
),
pensioners as (
	select id, status
	from pension.pensioner
	where updated is not null
	and pension_obj is not null
),
joined as (
	select 
		p.id, 
		r.jobposition_id
	from pensioners p
	join routes r
	on p.id = r.pensioner_id
		and (
			(p.status = 'approved' and r.rn = 2) 
			or 
			(p.status != 'approved' and r.rn = 1)
		)
),
found_names as (
	select *
	from joined j
	left join (
		select 
			j.id, 
			concat_ws(' ', s.lastname, s.firstname, s.middlename) fullname 
		from hr.jobposition j
		join hr.staff s
		on j.staff_id = s.id
		where s.id in (select staff_id from auth.user)
	) f on j.jobposition_id = f.id
), 
t1 as (
	select 
		fn.fullname as name,
		count(*) as "БП"
	from found_names fn
	group by fn.fullname
),
t2 as (
	select 
		su.fullname as name,
		count(*) as "СП"
	from pension.pensioner p
	left join (
		select 
			u.id as user_id, 
			concat_ws(' ', s.lastname, s.firstname, s.middlename) as fullname
		from hr.staff s
		left join auth.user u
		on s.id = u.staff_id
	) su on (p.updated->>'user_id')::uuid = su.user_id
	where updated is not null and pension_obj is not null
	group by su.fullname
)
select * 
from t2 
left join t1 
on t2.name = t1.name;



