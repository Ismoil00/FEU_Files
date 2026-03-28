




select * from pension.pension_routing;


SELECT * from hr.jobposition;


select * from accounting.warehouse_total_routing;


select * from files.document


create table if not exists pension.pension_documents (
	id bigserial primary key,
	pension_document_type pension.pension_document_type not null,
	document_id bigint REFERENCES files.document (id), 
	search_parameters jsonb not null,
	main_department_id integer not null references commons.department (id),
	status commons.routing_status default 'pending',
	created_at TIMESTAMP (0) without time zone default localtimestamp (0),
	updated_at TIMESTAMP (0) without time zone
);


create table if not exists pension.pension_document_routing (
	id bigserial primary key,
	pension_document_id bigint not null references pension.pension_documents (id),
	jobposition_id bigint references hr.jobposition (id),
	status commons.routing_status default 'pending',
	declined_text text,
	created_at TIMESTAMP (0) without time zone default localtimestamp (0),
	updated_at TIMESTAMP (0) without time zone
);


create type pension.pension_document_type as enum (
	'vedomost'
);




select * from pension.pension_documents






/* UPSERT PENSION DOCUMENT */
CREATE OR REPLACE FUNCTION pension.upsert_pension_document (jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_routing jsonb = (jdata->>'routing')::jsonb;
	_id bigint = (jdata ->> 'id')::bigint;
BEGIN

	if _id is null then
		insert into pension.pension_documents (
			pension_document_type, 
			search_parameters,
			main_department_id
		) values (
			(jdata ->> 'pension_document_type')::pension.pension_document_type,
			(jdata ->> 'search_parameters')::jsonb,
			(jdata ->> 'main_department_id')::integer
		) RETURNING id into _id;
	else
		update pension.pension_documents
		SET
			search_parameters = (jdata ->> 'search_parameters')::jsonb,
			updated_at = localtimestamp(0)
		where id = _id;
	end if;

	-- routing
	perform pension.upsert_pension_document_routing (
		(jdata->>'routing')::jsonb || jsonb_build_object('pension_document_id', _id)
	);

	-- return with data
	return select pension.get_pension_document_routing (_id);
END;
$BODY$;






/* UPSERT PENSION DOCUMENT ROUTING */
CREATE OR REPLACE FUNCTION pension.upsert_pension_document_routing (jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_pension_document_id bigint = (jdata->>'pension_document_id')::bigint;
	_jobposition_id bigint = (jdata->>'jobposition_id')::bigint;
	_department_id int = (jdata->>'department_id')::int;
	_status commons.routing_status = (jdata->>'status')::commons.routing_status;
	_declined_text text = (jdata->>'declined_text')::text;
	_document_id bigint = (jdata->>'document_id')::bigint;

	lastReviewRec record;
	curReviewRec record;
BEGIN

	/*************************** INFO COLLECTION ****************************/
	select
		pr.id as route_id,
		pr.pension_document_id,
		pr.jobposition_id,
		pr.status,
		drl.department_id,
		drl.level,
		drl.jobpositions
	into lastReviewRec
	from (
		select * from pension.pension_document_routing
		where pension_document_id = _pension_document_id
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
		select 1 from pension.pension_documents
		where id = _pension_document_id
		and status = 'approved'
	) then
		raise exception 'Документ одобрена и не подлежит маршрутизации.'
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
	if lastReviewRec.pension_document_id is null 
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
		update pension.pension_document_routing set
			status = _status,
			declined_text = _declined_text,
			updated_at = localtimestamp(0)
		where id = lastReviewRec.route_id;
	else
		insert into pension.pension_document_routing (
			pension_document_id,
			jobposition_id,
			status,
			declined_text
		) values (
			_pension_document_id,
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
		 -- if document is not generated then closing the routing is not valid
		if _document_id is null then
			raise exception 'Документ не был сгенерирован. Что-то не так на стороне сервера!'
			using errcode = 'P0001';
		end if;

		-- we close the record
		update pension.pension_documents set
			status = 'approved',
			document_id = _document_id,
			updated_at = localtimestamp (0)
		where id = _pension_document_id;
	end if;
	/*************************** FINAL APPROVAL ****************************/

	return jsonb_build_object(
		'status', 200,
		'msg', 'upserted!'
	);
END;
$BODY$;




/* GET BY ID */
CREATE OR REPLACE FUNCTION pension.get_pension_document_routing_by_id (
	_id bigint,
	_department_id integer
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result jsonb;
BEGIN

	with main as (
		select *
		from pension.pension_documents
		where id = _id
	),
	routing_1 as (
		select
			pdr.pension_document_id,
			row_number() over(
				partition by pdr.pension_document_id
				order by pdr.pension_document_id, pdr.created_at
			) as rownumber,
			jsonb_build_object(
				'jobposition_id', pdr.jobposition_id,
				'level', l.level,
				'fullname', d.fullname,
				'status', pdr.status,
				'declined_text', pdr.declined_text,
				'date', case when pdr.updated_at is not null
					then (pdr.updated_at)::date 
					else (pdr.created_at)::date end,
				'time', case when pdr.updated_at is not null
					then (pdr.updated_at)::time 
					else (pdr.created_at)::time end
			) routing_object
		from pension.pension_document_routing pdr
		left join (
			select 
		  		j.id, 
				concat_ws(' ', s.lastname, s.firstname, s.middlename ) as fullname 
			from hr.jobposition j
		  	left join hr.staff s
		  	on j.staff_id = s.id
		) d on pdr.jobposition_id = d.id
		left join (
			select level, unnest(jobpositions) as jobposition_id 
			from commons.department_routing_levels
			where department_id = _department_id
			and submodule_id = 14
		) l on pdr.jobposition_id = l.jobposition_id
		order by pdr.pension_document_id, pdr.created_at
	), 
	routing_2 as (
		select
			pension_document_id,
			jsonb_object_agg(
				rownumber,
				routing_object
			) as routing
		from routing_1		
		group by pension_document_id
	)
	select jsonb_build_object(
		'id', m.id,
		'pension_document_type', m.pension_document_type,
		'document_id', m.document_id,
		'search_parameters', m.search_parameters,
		'main_department_id', m.main_department_id,
		'status', m.status,
		'created_at', m.created_at,
		'routing', r.routing
	) into _result
	from main m
	left join routing_2 r
	on m.id = r.pension_document_id;
	
	return jsonb_build_object (
		'status', 200,
		'result', _result
	);
END;
$BODY$;











