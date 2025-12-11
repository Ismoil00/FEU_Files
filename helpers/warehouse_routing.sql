select * from accounting.warehouse_total_routing;





CREATE OR REPLACE FUNCTION accounting.upsert_warehouse_total_routing(
	jdata jsonb)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_warehouse_id bigint = (jdata->>'warehouse_id')::bigint;
	_warehouse_section accounting.warehouse_sections = (jdata->>'warehouse_section')::accounting.warehouse_sections;
	_jobposition_id bigint = (jdata->>'jobposition_id')::bigint;
	_department_id int = (jdata->>'department_id')::int;
	_status commons.routing_status = (jdata->>'status')::commons.routing_status;
	_declined_text text = (jdata->>'declined_text')::text;

	lastReviewRec record;
	curReviewRec record;
	is_approved BOOLEAN := FALSE;

	_updated jsonb = jsonb_build_object(
		'date', localtimestamp(0),
		'user_id', (
			select u.id
			from auth.user u
			left join hr.jobposition j
			on u.staff_id = j.staff_id
			where j.id = (jdata->>'jobposition_id')::bigint
			limit 1
		)::uuid
	);
begin

	/*************************** INFO COLLECTION ****************************/
	select
		wtr.id as route_id,
		wtr.warehouse_id,
		wtr.warehouse_section,
		wtr.jobposition_id,
		wtr.status,
		drl.department_id,
		drl.level,
		drl.jobpositions
	into lastReviewRec
	from (
		select * from accounting.warehouse_total_routing
		where warehouse_id = _warehouse_id
		and warehouse_section = _warehouse_section
		order by "createdAt" desc limit 1
	) wtr left join commons.department_routing_levels drl
	on wtr.jobposition_id = any(drl.jobpositions)
	and drl.department_id = _department_id;

	select 
		department_id, 
		level, 
		jobpositions
	into curReviewRec
	from commons.department_routing_levels
	where department_id = _department_id
	and _jobposition_id = any(jobpositions);
	/*************************** INFO COLLECTION ****************************/

	/*************************** VALIDATION ****************************/
	/* 
		APPROVED RECORD DOES NOT GO ROUTING ANY MORE 
	*/	
	IF _warehouse_section = 'order' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.warehouse_order
            WHERE id = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
    ELSIF _warehouse_section = 'incoming' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.warehouse_incoming
            WHERE unique_import_number = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
    ELSIF _warehouse_section = 'outgoing' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.warehouse_outgoing
            WHERE unique_outgoing_number = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
    ELSIF _warehouse_section = 'defect' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.product_defect
            WHERE id = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
	ELSIF _warehouse_section = 'service' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.services
            WHERE id = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
	ELSIF _warehouse_section = 'goods_return' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.goods_return
            WHERE id = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
	ELSIF _warehouse_section = 'inventory_entry' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.inventory_entry
            WHERE id = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
	ELSIF _warehouse_section = 'product_transfer' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.product_transfer
            WHERE transfer_number = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
	ELSIF _warehouse_section = 'assets_recognition' THEN
        SELECT EXISTS (
            SELECT 1 FROM accounting.assets_recognition
            WHERE operation_number = _warehouse_id AND status = 'approved'
        ) INTO is_approved;
    END IF;

    IF is_approved THEN
        RAISE EXCEPTION 'Запись одобрена и не подлежит маршрутизации.'
        USING ERRCODE = 'P0001';
    END IF;
	
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
	if lastReviewRec.warehouse_id is null 
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
		raise exception 'Это не ваша стадия для обзора процесса. Либо его не одобрил нижестоящий сотрудник или его уже одобрил сотрудник на уровень выше. Сделайте Поиск заново!'
		using errcode = 'P0001';
	end if;
	/*************************** VALIDATION ****************************/
	
	
	/*************************** UPSERT ****************************/
	if lastReviewRec.jobposition_id = _jobposition_id then
		update accounting.warehouse_total_routing set
			status = _status,
			declined_text = _declined_text,
			"updatedAt" = localtimestamp(0)
		where id = lastReviewRec.route_id;
	else
		insert into accounting.warehouse_total_routing (
			warehouse_id,
			warehouse_section,
			jobposition_id,
			status,
			declined_text
		) values (
			_warehouse_id,
			_warehouse_section,
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
		IF _warehouse_section = 'order' THEN
    	    UPDATE accounting.warehouse_order
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE id = _warehouse_id;
	
    	ELSIF _warehouse_section = 'incoming' THEN
    	    UPDATE accounting.warehouse_incoming
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE unique_import_number = _warehouse_id;
	
    	ELSIF _warehouse_section = 'outgoing' THEN
    	    UPDATE accounting.warehouse_outgoing
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE unique_outgoing_number = _warehouse_id;
	
    	ELSIF _warehouse_section = 'defect' THEN
    	    UPDATE accounting.product_defect
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE id = _warehouse_id;
				
    	ELSIF _warehouse_section = 'service' THEN
    	    UPDATE accounting.services
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE id = _warehouse_id;
				
    	ELSIF _warehouse_section = 'goods_return' THEN
    	    UPDATE accounting.goods_return
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE id = _warehouse_id;
    	
		ELSIF _warehouse_section = 'inventory_entry' THEN
    	    UPDATE accounting.inventory_entry
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE id = _warehouse_id;
    	
		ELSIF _warehouse_section = 'product_transfer' THEN
    	    UPDATE accounting.product_transfer
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE transfer_number = _warehouse_id;
		
		ELSIF _warehouse_section = 'assets_recognition' THEN
    	    UPDATE accounting.assets_recognition
    	    SET status = 'approved',
    	        updated = _updated
    	    WHERE operation_number = _warehouse_id;
    	END IF;
	end if;
	/*************************** FINAL APPROVAL ****************************/

	return jsonb_build_object(
		'status', 200,
		'msg', 'upserted!'
	);
end;
$BODY$;











CREATE OR REPLACE FUNCTION accounting.warehouse_total_routing_update_access(
	jdata jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_warehouse_id bigint = (jdata->>'warehouse_id')::bigint;
	_warehouse_section accounting.warehouse_sections = (jdata->>'warehouse_section')::accounting.warehouse_sections;
	_id bigint = (
		select id
		from accounting.warehouse_total_routing
		where warehouse_id = (jdata->>'warehouse_id')::bigint
		and warehouse_section = (jdata->>'warehouse_section')::accounting.warehouse_sections
		order by "createdAt" desc
		limit 1
	);
	_updated jsonb = jsonb_build_object(
		'date', localtimestamp(0),
		'user_id', (jdata->>'user_id')::uuid
	);
begin

	/********** VALIDATION **********/
	IF NOT EXISTS (
		SELECT 1 FROM accounting.warehouse_total_routing
		WHERE status = 'approved' and id = _id
	) THEN
        RAISE EXCEPTION 'Запись не полностью одобрена, чтобы открыть ее для обновления.'
        USING ERRCODE = 'P0001';
    END IF;

	/********** LAST REVIEW UPDATE **********/
	update accounting.warehouse_total_routing wtr set
		status = 'pending',
		"updatedAt" = localtimestamp(0)
	where id = _id;
		
	/********** WAREHOUSE STATUS UPDATE **********/
	IF _warehouse_section = 'order' THEN
	    UPDATE accounting.warehouse_order
	    SET status = 'pending',
	        updated = _updated
	    WHERE id = _warehouse_id;
	
	ELSIF _warehouse_section = 'incoming' THEN
	    UPDATE accounting.warehouse_incoming
	    SET status = 'pending',
	        updated = _updated
	    WHERE unique_import_number = _warehouse_id;
	
	ELSIF _warehouse_section = 'outgoing' THEN
	    UPDATE accounting.warehouse_outgoing
	    SET status = 'pending',
	        updated = _updated
	    WHERE unique_outgoing_number = _warehouse_id;
	
	ELSIF _warehouse_section = 'defect' THEN
	    UPDATE accounting.product_defect
	    SET status = 'pending',
	        updated = _updated
	    WHERE id = _warehouse_id;
	
	ELSIF _warehouse_section = 'service' THEN
	    UPDATE accounting.services
	    SET status = 'pending',
	        updated = _updated
	    WHERE id = _warehouse_id;
	
	ELSIF _warehouse_section = 'goods_return' THEN
	    UPDATE accounting.goods_return
	    SET status = 'pending',
	        updated = _updated
	    WHERE id = _warehouse_id;
	
	ELSIF _warehouse_section = 'inventory_entry' THEN
	    UPDATE accounting.inventory_entry
	    SET status = 'pending',
	        updated = _updated
	    WHERE id = _warehouse_id;
	
	ELSIF _warehouse_section = 'product_transfer' THEN
	    UPDATE accounting.product_transfer
	    SET status = 'pending',
	        updated = _updated
	    WHERE transfer_number = _warehouse_id;
	
	ELSIF _warehouse_section = 'assets_recognition' THEN
	    UPDATE accounting.assets_recognition
	    SET status = 'pending',
	        updated = _updated
	    WHERE operation_number = _warehouse_id;
	END IF;

	return json_build_object(
		'status', 200,
		'msg', 'Доступ предоставлен!'
	);
end;
$BODY$;













CREATE OR REPLACE FUNCTION accounting.get_warehouse_total_routing(
	_warehouse_id bigint,
	_warehouse_section accounting.warehouse_sections,
	_department_id integer)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
	_result jsonb;
	_record_status commons.routing_status;
begin

	with ordered as (
		select
			row_number() over (order by "createdAt") as rownumber,
			wtr.warehouse_id,
			wtr.warehouse_section,
			l.level,
			wtr.jobposition_id,
			d.fullname,
			wtr.status,
			wtr.declined_text,
			(wtr."createdAt")::date as date,
			(wtr."createdAt")::time as time
		from accounting.warehouse_total_routing wtr
		left join (
			select 
		  		j.id, 
				concat_ws(' ', s.lastname, s.firstname, s.middlename) as fullname 
			from hr.jobposition j
		  	left join hr.staff s
		  	on j.staff_id = s.id	
		) d on wtr.jobposition_id = d.id
		left join (
			select level, unnest(jobpositions) as jobposition_id 
			from commons.department_routing_levels
			where department_id = _department_id
		) l on wtr.jobposition_id = l.jobposition_id
		where warehouse_id = _warehouse_id
		and warehouse_section = _warehouse_section
		order by "createdAt"
	)
	select	jsonb_build_object(
		'warehouse_id', warehouse_id,
		'warehouse_section', warehouse_section,
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
	group by warehouse_id, warehouse_section;

	return json_build_object(
		'status', 200,
		'data', _result 
	);
end;
$BODY$;