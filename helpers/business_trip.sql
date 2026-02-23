
/*
	|| ORDER ||
*/


SELECT * FROM commons.local_trip_prices;

select * from commons.countries

select * from commons.trip_daily_pay;

select * from  hr.staff;


select * from commons.jobtitle

select * from  hr.business_trip;


select hr.download_business_trip_order_file (32)





CREATE OR REPLACE FUNCTION hr.download_business_trip_order_file (_id bigint)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result jsonb;
begin

	/* VALIDATION */
	if not exists (
		select 1
		from hr.business_trip
		where id = _id
	) then 
		raise exception 'Файл не найден';
	end if;

	/* QUERY */
	select jsonb_build_object (
		'organization', (        
			select department->>'tj' 
		    from commons.department
		    where id = 12
		),
		'id', bt.id,
		'created_date', (bt.created->>'date')::date,
		'staff_fullname', s.staff_fullname,
		'personnel_number', s.personnel_number,
		'staff_department', s.staff_department,
		'staff_jobtitle', s.staff_jobtitle,
		'destination', case
			when bt.abroad_trip is true
			then (
				select country->>'tj' 
				from commons.countries
				where id = bt.destination
			) else (
				select intercity->>'tj'
				FROM commons.local_trip_prices
				where id = bt.destination
			) 
		end,
		'visiting_organization', bt.visiting_organization,
		'days', (bt.date_to - bt.date_from)::integer,
		'date_from', bt.date_from,
		'date_to', bt.date_to,
		'purpose', bt.purpose,
		'description', bt.description
	) into _result
	from hr.business_trip bt
	left join (
		select
			s.id,
			concat_ws(' ', s.lastname, s.firstname, s.middlename) as staff_fullname, 
			s.ident_number as personnel_number,
			j.staff_department,
			j.staff_jobtitle
		from hr.staff s
		left join (
			SELECT
				m.staff_id,
				(
					select department->>'tj'
					from commons.department
					where id = m.department_id
				) as staff_department,
				(
					select jobtitle->>'tj'
					from commons.jobtitle
					where id = m.jobtitle_id
				) as staff_jobtitle
			FROM (
				SELECT 
					staff_id,
					department_id,
					jobtitle_id,
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
			) m WHERE m.row_rank = 1
		) j on s.id = j.staff_id
	) s on bt.staff_id = s.id
	where bt.id = _id;
	
	/* RETURN */
	return _result;
end;
$BODY$;

















/*
	|| CERTIFICATE ||
*/


SELECT * FROM commons.local_trip_prices;

select * from commons.countries

select * from commons.trip_daily_pay;

select * from  hr.staff;


select * from commons.jobtitle

select * from  hr.business_trip;


select hr.download_business_trip_certificate_file (32)





CREATE OR REPLACE FUNCTION hr.download_business_trip_certificate_file (_id bigint)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result jsonb;
begin

	/* VALIDATION */
	if not exists (
		select 1
		from hr.business_trip
		where id = _id
	) then 
		raise exception 'Файл не найден';
	end if;

	/* QUERY */
	select jsonb_build_object (
		'organization', (        
			select department->>'tj' 
		    from commons.department
		    where id = 12
		),
		'staff_fullname_and_jobtitle', concat_ws(', ', s.staff_fullname, s.staff_jobtitle),
		'organization_and_staff_department', concat_ws(', ', (        
			select department->>'tj' 
		    from commons.department
		    where id = 12
		), s.staff_department),
		'visiting_organization', bt.visiting_organization,
		'days', (bt.date_to - bt.date_from)::integer,
		'date_from', bt.date_from,
		'date_to', bt.date_to,
		'purpose', bt.purpose,
		'id', bt.id,
		'created_date', (bt.created->>'date')::date,
		'staff_passport_data', s.passport_details

	) into _result
	from hr.business_trip bt
	left join (
		select
			s.id,
			concat_ws(' ', s.lastname, s.firstname, s.middlename) as staff_fullname, 
			j.staff_department,
			j.staff_jobtitle,
			concat_ws(', ', 
				concat_ws(': ', 'Рақами шиноснома', s.details->'passport'->>'doc_number'),
				concat_ws(': ', 'Оғози эътибор', s.details->'passport'->>'issued'),
				concat_ws(': ', 'Мақоми шиносномадиҳанда', s.details->'passport'->>'authority')
			) as passport_details
		from hr.staff s
		left join (
			SELECT
				m.staff_id,
				(
					select department->>'tj'
					from commons.department
					where id = m.department_id
				) as staff_department,
				(
					select jobtitle->>'tj'
					from commons.jobtitle
					where id = m.jobtitle_id
				) as staff_jobtitle
			FROM (
				SELECT 
					staff_id,
					department_id,
					jobtitle_id,
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
			) m WHERE m.row_rank = 1
		) j on s.id = j.staff_id
	) s on bt.staff_id = s.id
	where bt.id = _id;
	
	/* RETURN */
	return _result;
end;
$BODY$;

