




CREATE OR REPLACE FUNCTION pension.calculate_pension_amount(
	_id bigint)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	-- pensioner info:
	_staff_id bigint;
	_type integer;
	_awards_ids bigint[];
	_allowances_ids bigint[];
	_percentage_for_years_of_services_id bigint;
	_awards_coef numeric = 0; -- нфграды
	_allowances_sum numeric = 0; -- надбавки;

	-- from the salary section:
	_os bigint; -- official-salary/оклад;
	_ra bigint; -- rank-allowance/оплата-за-звания;
	_exc numeric; -- experience-coefficient/оплата-за-стаж;
	_actual_years int;
	_grace_years int;
	_rank_id int;
	_civic_experience boolean;
	additional_percent int = 0; -- additional percentage;
	_years_of_service int = 0;
	_percentage int = 0;

	-- retention:
	_retention boolean;
	_retention_sum numeric;

	-- for calculation:
	_salary numeric;
	_type_coef numeric = 0 ;
	_pa numeric = 0; -- pension_amount before subtracting retention;
	_pension_amount numeric = 0;
	_clean_rpa numeric = 0; -- clean restricted_pension_amount;
	_rpa numeric = 0; -- restricted_pension_amount before subtracting retention;
	_restricted_pension_amount numeric = 0;
	_restricted_pension boolean = false;

	/* children info */
	_children_amount int;
	_childrenAid numeric;

	/* spouse info */
	_spouse jsonb;
	_spouse_age int := 0;
	_spouseEligible int := 0;
	under8Sustenance int := 0;
BEGIN

	/* VALIDATION START */
	PERFORM pension.update_pensioner_children_validity(_id);

	PERFORM pension.update_pensioner_salary_obj(_id);
	/* VALIDATION END */
	
	/* INFO COLLECTION START */
	-- from the pensioner table
	select 
		pp.staff_id, 
		pp.pension_type, 
		pp.awards_ids, 
		COALESCE(array_length(pp.children, 1), 0),
		case when pp.pension_type = 2 then pp.details else null end,
		pp.percentage_for_years_of_services,
		(pp.salary_obj->>'official_salary')::bigint,
		(pp.salary_obj->>'rank_allowance')::bigint,
		round((pp.salary_obj->'experience_coef')::numeric / 100, 4),
		(pp.salary_obj->>'actual_years')::integer,
		(pp.salary_obj->>'grace_years')::integer,
		(pp.salary_obj->>'rank_id')::integer,
		pp.retention,
		(pp.salary_obj->>'civic_experience')::boolean,
		pp.allowances_ids
	into 
		_staff_id, 
		_type, 
		_awards_ids, 
		_children_amount,
		_spouse,
		_percentage_for_years_of_services_id,
		_os,
		_ra,
		_exc,
		_actual_years,
		_grace_years,
		_rank_id,
		_retention,
		_civic_experience,
		_allowances_ids
	from pension.pensioner pp where pp.id = _id;

	-- additional payment for children + old-person
	_childrenAid = COALESCE((
		select amount
		from commons.pension_children_aid
		where end_date is null
	), 0);

	_awards_coef = round(coalesce((
		select sum(percentage)
		from commons.pension_awards pa
		where pa.id = any(_awards_ids)
		and disabled is not true
	), 0)::numeric / 100, 2);
	
	_allowances_sum = coalesce((
		select sum(amount)
		from commons.pension_allowances pa
		where pa.id = any(_allowances_ids)
		and disabled is not true
	), 0)::numeric;
	/* INFO COLLECTION END */

	-- raise notice '---------------------------------------------';
	-- raise notice '_staff_id %', _staff_id;
	-- raise notice '_type %', _type;
	-- raise notice '_awards_ids %', _awards_ids;
	-- raise notice '_awards_ids %', _awards_ids;
	-- raise notice '_allowances_sum %', _allowances_sum;
	-- raise notice '_children_amount %', _children_amount;
	-- raise notice '_spouse %', _spouse;
	-- raise notice '_childrenAid %', _childrenAid;
	-- raise notice '_awards_coef %', _awards_coef;
	-- raise notice '_os %', _os;
	-- raise notice '_ra %', _ra;
	-- raise notice '_exc %', _exc;
	-- raise notice '_actual_years %', _actual_years;
	-- raise notice '_rank_id %', _rank_id;
	-- raise notice '_grace_years %', _grace_years;
	-- raise notice '---------------------------------------------';
	

	/* PENSION CALCULATION START */
	-- Salary = Oklad + Rutba + (Oklad + Rutba) * FoiziSobikaiKori;
	_salary := _os + _ra + (_os + _ra) * _exc;
	
	-- disabled:
	IF _type = 1 THEN
		_type_coef = round(coalesce((
			select dsg.percentage
			from hr.disabled_staff ds
			left join (
				select id, percentage
				from commons.disabled_staff_groups
				where disabled is not true
			) dsg on ds.group_id = dsg.id
			where staff_id = _staff_id
		), 0)::numeric / 100, 2);
		
		-- Pension Amount Calculation:
		_pa = _salary * _type_coef + -- часть из ЗП
			(_salary * _type_coef) * _awards_coef + -- награды 
			_childrenAid * _children_amount + -- за детей
			_allowances_sum; -- надбавки
		
	-- dead:
    ELSIF _type = 2 THEN
		/* we define the coef */		
		_type_coef = round(coalesce((
			select dsg.percentage
			from hr.deceased_staff ds
			left join (
				select id, percentage
				from commons.deceased_staff_groups
				where disabled is not true
			) dsg on ds.group_id = dsg.id
			where staff_id = _staff_id
		), 0)::numeric / 100, 2);

		/* spouse validity check */
		_spouse_age := coalesce(
    		date_part('year', age((_spouse->>'spouseBirthdate')::date)),
    		0
		);
		_spouseEligible := CASE
		    WHEN _spouse_age >= CASE WHEN _type_coef >= 0.4 THEN 50 ELSE 58 END
				AND (_spouse->>'spouse_receives')::bool is true 
		    THEN 1 
		    ELSE 0 
		END;

		/* under 8 child sustenance */
		under8Sustenance := coalesce(
		    (select pension.died_pensioner_under_8_sustenance (_id)),
		    0
		);

		-- Pension Amount Calculation:
		_pa = _salary * _type_coef * (
			_children_amount + -- за детей 
			_spouseEligible + -- супругу/е старше 50/58
			under8Sustenance -- пропитание
		) +
		_allowances_sum; -- надбавки

	-- byService:
	ELSIF _type = 3 THEN
		-- INFOR Collection:
		select percentage, years
		into _percentage, _years_of_service
		from commons.pension_percentage_years_of_services
		where id = _percentage_for_years_of_services_id
		and disabled is not true;
		
		if _civic_experience is true then -- civic experience consideration
			additional_percent = case 
				when _grace_years > _years_of_service then (_grace_years - _years_of_service) * 1
				else 0 end;
		else
			additional_percent = case 
				when _grace_years >= 30 then 15 
				when _grace_years > _years_of_service then (_grace_years - _years_of_service) * 3
				else 0 end;
		end if;

		_type_coef = round(((_percentage + additional_percent)::numeric / 100), 2);
		
		-- with Restriction:
		if _actual_years < 25 then
			_restricted_pension = true;
			
			_clean_rpa = coalesce((
				select amount
				from commons.pension_restrictions pr
				where _rank_id = any(pr.ranks)
				and disabled is not true
			), 0)::numeric;

			_rpa = _clean_rpa + _clean_rpa * _awards_coef + -- награды
				_childrenAid * _children_amount + -- за детей
				_allowances_sum; -- надбавки
		end if;
			
		-- without Restriction:
		if _restricted_pension is true then
			_pa = _salary * _type_coef; -- чистая ЗП если с огроничением
		else
			_pa = _salary * _type_coef + -- часть из ЗП
				(_salary * _type_coef) * _awards_coef + -- награды
				_childrenAid * _children_amount + -- за детей
				_allowances_sum; -- надбавки
		end if;
    ELSE
        RAISE EXCEPTION 'Invalid pension_type: %', _type;
    END IF;
	/* PENSION CALCULATION END */

	-- raise notice '---------------------------------------------';
	-- raise notice 'salary %', _salary;
	-- raise notice '_type_coef %', _type_coef;
	-- raise notice '_pa %', _pa;
	-- raise notice '_clean_rpa %', _clean_rpa;
	-- raise notice '_rpa %', _rpa;
	-- raise notice '_restricted_pension %', _restricted_pension;
	-- raise notice '_retention %', _retention;
	-- raise notice '_retention_sum %', _retention_sum;
	-- raise notice '---------------------------------------------';

	-- удержание от пенсии
	if _retention is true then
		_retention_sum = coalesce((
			select sum(amount) * 100
			from pension.pensioner_retention
			where pensioner_id = _id
			and (valid_to is null or valid_to >= current_date)
		), 0);
		

		if _restricted_pension then
			_restricted_pension_amount = _rpa - _retention_sum;
		else
			if _pa is not null and _pa > 0 then
				_pension_amount = _pa - _retention_sum;
			end if;
		end if;
	else
		_pension_amount = _pa;
		_restricted_pension_amount = _rpa;
	end if;

	-- raise notice '---------------------------------------------';
	-- raise notice '_pension_amount %', _pension_amount;
	-- raise notice '_restricted_pension_amount %', _restricted_pension_amount;
	-- raise notice '---------------------------------------------';

	return jsonb_build_object(
		'pension_amount', case 
			when _pension_amount < 35750 then 35750 
			else round(_pension_amount, 2) end,
		'restricted_pension_amount', round(_restricted_pension_amount, 2),
		'restricted_pension', _restricted_pension,
		'total_coef', _type_coef * 100,
		'awards_total_coef', _awards_coef * 100
	);
END;
$BODY$;
