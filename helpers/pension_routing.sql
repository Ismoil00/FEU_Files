
select pension.pension_giving_access_to_update(28, 155);

select * from pension.pension_routing
order by "createdAt" desc

create or replace function pension.pension_giving_access_to_update (
	_pensioner_id bigint,
	_jobposition_id bigint,
	_user_id text
)
returns void
language plpgsql
as $$
begin

	/********** LAST REVIEW UPDATE **********/
	update pension.pension_routing set
		status = 'pending',
		"updatedAt" = localtimestamp(0)
	where id = (
		select id
		from pension.pension_routing
		where pensioner_id = _pensioner_id
		order by "createdAt" desc
		limit 1
	);
		
	/********** PENSIONER STATUS UPDATE **********/
	update pension.pensioner set
		status = 'pending',
		updated = jsonb_build_object(
			'date', localtimestamp(0),
			'user_id', _user_id::uuid
		)
	where id = _pensioner_id;
end;
$$;
-----------------------------------------------------------------

select * from pension.pension_routing
order by "createdAt" desc;

select pension.get_pensioner_routing(28, 1);

create or replace function pension.get_pensioner_routing (
	_pensioner_id bigint,
	_department_id int
)
returns json
language plpgsql
as $$
declare
	_result jsonb;
begin

	with ordered as (
	  select
	  	row_number() over (order by "createdAt") as rownumber,
	  	pr.pensioner_id,
		l.level,
		pr.jobposition_id,
		d.fullname,
		pr.status,
		pr.declined_text,
		(wtr."createdAt")::date as date,
		(wtr."createdAt")::time as time
	  from pension.pension_routing pr
	  left join (
		  select 
		  	j.id, 
			concat_ws(' ', s.firstname, s.lastname, s.middlename) as fullname 
	      from hr.jobposition j
		  left join hr.staff s
		  on j.staff_id = s.id	
	  ) d on pr.jobposition_id = d.id
	  left join (
		select level, unnest(jobpositions) as jobposition_id 
		from commons.department_routing_levels
		where department_id = _department_id
	  ) l on pr.jobposition_id = l.jobposition_id
	  where pensioner_id = _pensioner_id
	  order by "createdAt"
	)
	select	jsonb_build_object(
		'pensioner_id', pensioner_id,
		'routing', jsonb_object_agg(
			rownumber,
			jsonb_build_object(
				'jobposition_id', jobposition_id,
				'level', level,
				'fullname', fullname,
				'status', status,
				'declined_text', declined_text,
				'date', date,
				'time', time
			)
		)
	)
	from ordered into _result
	group by pensioner_id;

	return json_build_object(
		'status', 200,
		'data', _result
	);
end;
$$;

------------------------------------------------------------------

select * from pension.pension_routing
order by "createdAt";

select * from pension.pensioner where id = 28;

select pension.upsert_pension_routing(
	'
		{
			"pensioner_id": 28,
			"jobposition_id": 155,
			"department_id": 1,
			"status": "approved",
			"declined_text": null
		}
	'
);

CREATE OR REPLACE FUNCTION pension.upsert_pension_routing(
	jdata jsonb)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_pensioner_id bigint = (jdata->>'pensioner_id')::bigint;
	_jobposition_id bigint = (jdata->>'jobposition_id')::bigint;
	_department_id int = (jdata->>'department_id')::int;
	_status commons.routing_status = (jdata->>'status')::commons.routing_status;
	_declined_text text = (jdata->>'declined_text')::text;

	lastReviewRec record;
	curReviewRec record;

	_pension_amount_obj jsonb;
	_pension_type int;
	_staff_id bigint;
	_awards_ids bigint[];
	_children bigint[];
	_details jsonb;
	_user_id uuid = (
		select u.id
		from auth.user u
		left join hr.jobposition j
		on u.staff_id = j.staff_id
		where j.id = (jdata->>'jobposition_id')::bigint
		limit 1
	);
begin

	/*************************** INFO COLLECTION ****************************/
	select
		pr.id as route_id,
		pr.pensioner_id,
		pr.jobposition_id,
		pr.status,
		drl.department_id,
		drl.level,
		drl.jobpositions
	into lastReviewRec
	from (
		select * from pension.pension_routing
		where pensioner_id = _pensioner_id
		order by "createdAt" desc limit 1
	) pr left join commons.department_routing_levels drl
	on pr.jobposition_id = any(drl.jobpositions)
	and drl.department_id = _department_id;

	select department_id, level, jobpositions
	into curReviewRec
	from commons.department_routing_levels
	where department_id = _department_id
	and _jobposition_id = any(jobpositions);
	/*************************** INFO COLLECTION ****************************/

	/*************************** VALIDATION ****************************/
	/* 
		APPROVED RECORD DOES NOT GO ROUTING ANY MORE 
	*/	
	if exists (
		select 1 from pension.pensioner
		where id = _pensioner_id
		and status = 'approved'
	) then
		raise exception 'Запись одобрена и не подлежит маршрутизации.'
		using errcode = 'P0001';
	end if;
	
	/* 
		DEPARTMENT MUST HAVE A ROUTING SCHEMA AND USER MUST BE PART OF IT 
	*/
	if not exists (
		select 1 from commons.department_routing_levels
		where department_id = _department_id
		and _jobposition_id = any(jobpositions)
	) then
		raise exception 'Для текущего отдела нет схемы маршрутизации ИЛИ Этот пользователь не является частью схемы маршрутизации данного отдела.'
		using errcode = 'P0001';
	end if;

	/* 
		INITIALLY ONLY INSPECTORS CAN CREATE AND APPROVE THE PROCESS
	*/
	if lastReviewRec.pensioner_id is null 
	and curReviewRec.level != 1
	then
		raise exception 'Именно инспектор изначально создает и утверждает процесс.'
		using errcode = 'P0001';
	end if;

	/* 
		ONLY INSPECTORS CAN CHANGE DECLINED OR PENDING PROCESS 
	*/
	if lastReviewRec.status in ('declined', 'pending')
	and lastReviewRec.jobposition_id != _jobposition_id
	and curReviewRec.level != 1
	then
		raise exception 'Процесс либо отклонен, либо находится в состоянии ожидания, и только инспекторы могут его рассмотреть.'
		using errcode = 'P0001';
	end if;

	/* 
		STAFF CAN SET HIS REVIEW, ONLY IF ONE LEVEL-LOWER STAFF HAS DONE IT (EXCETP LEVEL 1 STAFF)
	*/
	if curReviewRec.level - 1 != lastReviewRec.level 
	and lastReviewRec.jobposition_id != _jobposition_id
	and lastReviewRec.status = 'approved'
	then
		raise exception 'Это не ваша стадия для обзора процесса. Во-первых, его должен рассмотреть персонал на один уровень ниже.'
		using errcode = 'P0001';
	end if;
	/*************************** VALIDATION ****************************/

	
	/*************************** UPSERT ****************************/
	if lastReviewRec.jobposition_id = _jobposition_id then
		update pension.pension_routing set
			status = _status,
			declined_text = _declined_text,
			"updatedAt" = localtimestamp(0)
		where id = lastReviewRec.route_id;
	else
		insert into pension.pension_routing (
			pensioner_id,
			jobposition_id,
			status,
			declined_text
		) values (
			_pensioner_id,
			_jobposition_id,
			_status,
			_declined_text
		);
	end if;
	/*************************** UPSERT ****************************/

	/*************************** FINAL APPROVAL ****************************/
	if (
		select level
		from commons.department_routing_levels
		where department_id = _department_id
		order by level desc limit 1
	) = curReviewRec.level and _status = 'approved'
	then
		-- info collection
		select 
			pension_type, 
			staff_id, 
			awards_ids, 
			children,
			details
		into
			_pension_type, 
			_staff_id, 
			_awards_ids, 
			_children,
			_details
		from pension.pensioner 
		where id = _pensioner_id;

		-- pension-amount calculation
		_pension_amount_obj := pension.calculate_pension_amount(
			_pension_type,
			_staff_id,
			_awards_ids,
			_children,
			case when _pension_type = 2 then _details else null end
		);

		-- pensioner status change
		update pension.pensioner set
			status = 'approved',
			pension_amount_obj = _pension_amount_obj,
			updated = jsonb_build_object(
				'date', localtimestamp(0),
				'user_id', _user_id
			)
		where id = _pensioner_id;

		-- staff status change
		if _pension_type = 3 then
	 		UPDATE hr.staff SET 
				status = 4,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', current_date
				) 
			WHERE id = _staff_id;
		END IF;
	end if;
	/*************************** FINAL APPROVAL ****************************/

	return jsonb_build_object(
		'status', 200,
		'msg', 'upserted!'
	);
end;
$BODY$;