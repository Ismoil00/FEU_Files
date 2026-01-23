select * from pension.pensioner;


	-- СПК -> 2
	-- Выслуга лет -> 3
	-- Инвалидность -> 1

	-- select * from hr.deceased_staff;
	
	-- select * from hr.disabled_staff;
	
	-- select * from commons.pension_type;

	select * from pension.pensioner
	where staff_id = 3660;


	select * from hr.deceased_staff
	where staff_id = 3660;


	select * from hr.disabled_staff
	where staff_id = 3660;


	select status from hr.staff where id = 3660
	
	select * from hr.staff where id = 3660





create or replace function pension.change_pensioner_type (
	jdata jsonb
)
RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_id bigint = (jdata->>'id')::bigint;
	_staff_id bigint = (jdata->>'staff_id')::bigint;
	_new_type_id integer = (jdata->>'new_type_id')::integer;
	_deceased_date date = (jdata->>'deceased_date')::date;
	_group_id bigint = (jdata->>'group_id')::bigint;
	_department_id bigint = (jdata->>'department_id')::bigint; 
	
	_result jsonb;
	createdOrUpdated jsonb = jsonb_build_object(
		'date', localtimestamp(0),
		'user_id', jdata->>'user_id'
	);
	_new_id bigint;
BEGIN

	-- if new and old types are the same
	if _new_type_id = (
		select pension_type 
		from pension.pensioner 
		where id = _id
	) then
		RAISE EXCEPTION 'Новые и старые виды пенсий одинаковы.';
	end if;

	-- we close the pensioner old pension type
	update pension.pensioner set
		pension_end_date = current_date,
		pension_type_changed = true,
		updated = createdOrUpdated
	where id = _id;

	-- we insert pensioner with the new type
	with pensioner as (
		select * from pension.pensioner where id = _id
	)
	insert into pension.pensioner (
		staff_id,
		doc_id,
		pension_type,
		pension_start_date,
		order_number,
		details,
		department_id,
		order_date,
		transport_card,
		registration_date,
		awards_ids,
		pension_number,
		children,
		status,
		percentage_for_years_of_services,
		contact_numbers,
		manual_salary,
		pension_obj,
		salary_obj,
		bank_account,
		old_record_jobtitle,
		transport_card_date,
		allowances_ids,
		retention,
		created
	) 
	select
		staff_id,
		doc_id,
		_new_type_id,
		current_date,
		order_number,
		details,
		department_id,
		order_date,
		transport_card,
		registration_date,
		awards_ids,
		pension_number,
		children,
		status,
		percentage_for_years_of_services,
		contact_numbers,
		manual_salary,
		pension_obj,
		salary_obj,
		bank_account,
		old_record_jobtitle,
		transport_card_date,
		allowances_ids,
		retention,
		createdOrUpdated
	FROM pensioner
	RETURNING id INTO _new_id;

	-- we create routing 
	perform pension.upsert_pension_routing (
		(jdata->>'routing')::jsonb || jsonb_build_object('pensioner_id', _new_id)
	);

	-- we set pensioner status to pensioner;
	if _new_type_id = 3 then
		UPDATE hr.staff SET 
			status = 4,
			updated = createdOrUpdated
		WHERE id = _staff_id;
	end if;
	
	-- we insert died staff
	if _new_type_id = 2 then
		if exists (
			select 1 from hr.deceased_staff
			where staff_id = _staff_id
		) then
			update hr.deceased_staff set
				deceased_date = _deceased_date,
				group_id = _group_id,
				updated = createdOrUpdated
			where staff_id = _staff_id;
		else
			insert into hr.deceased_staff (
				staff_id,
				deceased_date,
				group_id,
				department_id,
				created
			) values (
				_staff_id,
				_deceased_date,
				_group_id,
				_department_id,
				createdOrUpdated
			);

			-- we change died person status;
			UPDATE hr.staff
			SET status = 7,
			updated = createdOrUpdated
			WHERE id = _staff_id;
		end if;
	end if;

	-- we insert invalid staff
	if _new_type_id = 1 then
		if exists (
			select 1 from hr.disabled_staff
			where staff_id = _staff_id
		) then
			update hr.disabled_staff set
				group_id = _group_id,
				updated = createdOrUpdated
			where staff_id = _staff_id;
		else
			insert into hr.disabled_staff (
				staff_id,
				group_id,
				department_id,
				created
			) values (
				_staff_id,
				_group_id,
				_department_id,
				createdOrUpdated
			);

			-- we change disabled person status;
			UPDATE hr.staff SET 
				status = 8,
				updated = createdOrUpdated
			WHERE id = _staff_id;
		end if;
	end if;
	
	return jsonb_build_object(
		'status', 200,
		'new_id', _new_id
	);
END;
$BODY$;
















select pension.get_a_pensioner_for_type_convertion ()



CREATE OR REPLACE FUNCTION pension.get_a_pensioner_for_type_convertion(
	_type_id bigint DEFAULT NULL::bigint,
	_firstname text DEFAULT NULL::text,
	_lastname text DEFAULT NULL::text,
	_middlename text DEFAULT NULL::text,
	_pension_number bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
	_result jsonb;
BEGIN

	with main as (
		select jsonb_build_object (
			'key', row_number() over (order by p.id),
			'id', p.id,
			'staff_id', p.staff_id,
			'pension_type', p.pension_type,
			'pension_number', p.pension_number,
			'department_id', s.department_id,
			'lastname', s.lastname,
			'firstname', s.firstname,
			'middlename', s.middlename
		) as aggregated
		from pension.pensioner p
		left join (
			select
				s.id,
				s.lastname, 
				s.firstname, 
				s.middlename,
				j.department_id 
			from hr.staff s
			left join (
				SELECT * FROM (
					SELECT 
						staff_id,
						department_id,
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
			) j on s.id = j.staff_id
		) s on p.staff_id = s.id
		where p.pension_type_changed is not true
		AND (p.pension_end_date is null or p.pension_end_date > current_date)
		AND (_type_id is null or _type_id = p.pension_type)
		AND (_firstname IS NULL OR LOWER(TRIM(s.firstname)) ILIKE LOWER(_firstname))
		AND (_lastname IS NULL OR LOWER(TRIM(s.lastname)) ILIKE LOWER(_lastname))
		AND (_middlename IS NULL OR LOWER(TRIM(s.middlename)) ILIKE LOWER(_middlename))
		and (_pension_number is null or _pension_number = p.pension_number)
		order by p.id
	),
	table_total as (
		select count(*) as total from main
	)
	select jsonb_build_object (
		'status', 200,
		'results', jsonb_agg(m.aggregated),
		'total', (select total from table_total)
	) into _result from main m;

	return _result;
END;
$BODY$;












