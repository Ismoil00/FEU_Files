







CREATE OR REPLACE FUNCTION accounting.upsert_services(
	jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = 
			(jdata->>'financing')::accounting.budget_distribution_type;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.services
			where id = _id 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;

		/* UPSERT */
		if _id is null then
			insert into accounting.services (
				counterparty_id,
				contract,
				table_data,
				comment,
				financing,
				created
			) values (
				(jdata->>'counterparty_id')::bigint,
				(jdata->>'contract')::text,
				(jdata->>'table_data')::jsonb,
				(jdata->>'comment')::text,
				_financing,
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;
		else
			update accounting.services s SET
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				contract = (jdata->>'contract')::text,
				table_data = (jdata->>'table_data')::jsonb,
				comment = (jdata->>'comment')::text,
				financing = _financing,
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             s.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE s.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;

		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _id)
		);

		return json_build_object(
			'msg', case when _id is null then 'created' else 'updated' end,
			'status', 200,
			'id', _id
		);
	end;
$BODY$;









CREATE OR REPLACE FUNCTION accounting.get_services_by_id(
	_department_id integer,
	_id bigint)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN
	
		/* MAIN QUERY */
		with main as (
			select 
				id,
				counterparty_id,
				contract,
				table_data,
				comment,
				financing,
				status,
				(created->>'date')::date as created_date
			from accounting.services
			where id = _id 
		), 
		routing_1 as (
			select
				wtr.warehouse_id,
				row_number() over(
					partition by wtr.warehouse_id
					order by wtr.warehouse_id, wtr."createdAt"
				) as rownumber,
				jsonb_build_object(
					'jobposition_id', wtr.jobposition_id,
					'level', l.level,
					'fullname', d.fullname,
					'status', wtr.status,
					'declined_text', wtr.declined_text,
					'date', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::date 
						else (wtr."createdAt")::date end,
					'time', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::time 
						else (wtr."createdAt")::time end
				) routing_object 
			from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'service' 
			) wtr
			left join (
				select 
			  		j.id, 
					concat_ws(' ', s.lastname, s.firstname, s.middlename ) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
			) d on wtr.jobposition_id = d.id
			left join (
				select level, unnest(jobpositions) as jobposition_id 
				from commons.department_routing_levels
				where department_id = _department_id
			) l on wtr.jobposition_id = l.jobposition_id
			order by wtr.warehouse_id, wtr."createdAt"
		), 
		routing_2 as (
			select
				warehouse_id,
				jsonb_object_agg(
					rownumber,
					routing_object
				) as routing
			from routing_1		
			group by warehouse_id
		)
		select jsonb_build_object(
			'id', m.id,
			'counterparty_id', m.counterparty_id,
			'contract', m.contract,
			'table_data', m.table_data,
			'comment', m.comment,
			'created_date', m.created_date,
			'financing', financing,
			'status', status,
			'routing', r.routing
		) into _result
		from main m 
		left join routing_2 r 
		on m.id = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'result', _result
		);
	end;
$BODY$;








CREATE OR REPLACE FUNCTION accounting.get_services(
	_department_id integer,
	_financing accounting.budget_distribution_type,
	_id bigint DEFAULT NULL::bigint,
	_created_from text DEFAULT NULL::text,
	_created_to text DEFAULT NULL::text,
	_counterparty_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 100,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
		total int;
	BEGIN

		/* TOTAL */
		select count(*) into total
		from accounting.services
		where financing = _financing 
		and (_id is null or id = _id)
		and (_created_from is null or _created_from::date <= (created->>'date')::date)
		and (_created_to is null or _created_to::date >= (created->>'date')::date)
		and (_counterparty_id is null or _counterparty_id = counterparty_id);

		/* MAIN QUERY */
		with main as (
			select 
				row_number() over(order by id) as key,
				id,
				counterparty_id,
				contract,
				table_data,
				comment,
				financing,
				status,
				(created->>'date')::date as created_date
			from accounting.services
			where financing = _financing 
			and (_id is null or id = _id)
			and (_created_from is null or _created_from::date <= (created->>'date')::date)
			and (_created_to is null or _created_to::date >= (created->>'date')::date)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
			order by id limit _limit offset _offset
		), routing_1 as (
		select
			wtr.warehouse_id,
			row_number() over(
				partition by wtr.warehouse_id
				order by wtr.warehouse_id, wtr."createdAt"
			) as rownumber,
			jsonb_build_object(
				'jobposition_id', wtr.jobposition_id,
				'level', l.level,
				'fullname', d.fullname,
				'status', wtr.status,
				'declined_text', wtr.declined_text,
				'date', case when wtr."updatedAt" is not null
					then (wtr."updatedAt")::date 
					else (wtr."createdAt")::date end,
				'time', case when wtr."updatedAt" is not null
					then (wtr."updatedAt")::time 
					else (wtr."createdAt")::time end
			) routing_object from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'service' 
			) wtr
			left join (
				select 
			  		j.id, 
					concat_ws(' ', s.lastname, s.firstname, s.middlename ) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
			) d on wtr.jobposition_id = d.id
			left join (
				select level, unnest(jobpositions) as jobposition_id 
				from commons.department_routing_levels
				where department_id = _department_id
			) l on wtr.jobposition_id = l.jobposition_id
			order by wtr.warehouse_id, wtr."createdAt"
		), 
		routing_2 as (
			select
				warehouse_id,
				jsonb_object_agg(
					rownumber,
					routing_object
				) as routing
			from routing_1		
			group by warehouse_id
		)
		select jsonb_agg(
			jsonb_build_object(
				'key', m.key,
				'id', m.id,
				'counterparty_id', m.counterparty_id,
				'contract', m.contract,
				'table_data', m.table_data,
				'comment', m.comment,
				'created_date', m.created_date,
				'financing', financing,
				'status', status,
				'routing', r.routing
			)
		) into _result
		from main m 
		left join routing_2 r 
		on m.id = r.warehouse_id;

		return jsonb_build_object(
			'status', 200,
			'total', total,
			'results', _result
		);
	end;
$BODY$;