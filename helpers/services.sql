

create table if not exists accounting.warehouse_services (
	id bigserial primary key,
	operation_number BIGINT not null,
	counterparty_id bigint not null REFERENCES accounting.counterparty (id),
	contract_id bigint not null REFERENCES commons.counterparty_contracts (id),
	financing accounting.budget_distribution_type not null,

	/* table */
	service_nomenclature_id bigint not null REFERENCES commons.services_nomenclature (id),
	quantity numeric not null default 1,
	unit_price numeric not null,
	estimate_id bigint REFERENCES accounting.estimates (id),
	debit integer not null REFERENCES accounting.accounts (account),
	credit integer not null REFERENCES accounting.accounts (account),
	ledger_id bigint not null REFERENCES accounting.ledger (id),
	description text,
	/* table */
	
	status commons.routing_status default 'pending',
	comment text,
	created jsonb not null,
	updated jsonb
);


select * from accounting.warehouse_services;


select * from accounting.ledger;


select * from accounting.warehouse_total_routing;



CREATE OR REPLACE FUNCTION accounting.upsert_warehouse_services(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		_counterparty_id bigint = (jdata->>'counterparty_id')::bigint;
		_contract_id bigint = (jdata->>'contract_id')::bigint;
		_financing accounting.budget_distribution_type = 
			(jdata->>'financing')::accounting.budget_distribution_type;
		_comment text = jdata->>'comment';
		isUpdate boolean = false;

		/* table variables */
		_product jsonb;
		_id bigint;
		_debit integer;
		_credit integer;
		_ledger_id bigint;
		_quantity numeric;
		_unit_price numeric;
	BEGIN

		/* VALIDATION START - STATUS CHECK */
		IF EXISTS (
	    	select 1 from accounting.warehouse_services
			where id = _id 
			and status = 'approved'
		) THEN
			RAISE EXCEPTION 'Запись полностью одобрена, поэтому вы не можете ее редактировать. запросить одобрение редактирования' USING ERRCODE = 'P0001';
		END IF;

		/* GENERATING NEW OPERATION-NUMBER */
		if _operation_number is null then
			SELECT coalesce(max(sub.operation_number), 0) + 1
			into _operation_number from (
				SELECT operation_number
	        	FROM accounting.warehouse_services
	        	GROUP BY operation_number
	        	ORDER BY operation_number DESC
	        	LIMIT 1
			) sub;
		end if;

		-- loop
		FOR _product IN SELECT * FROM json_array_elements(jdata->'table_data') LOOP
			_id = (_product->>'id')::bigint;
			_quantity = (_product->>'quantity')::numeric;
			_unit_price = (_product->>'unit_price')::numeric;
			_debit = (_product->>'debit')::int;
			_credit = (_product->>'credit')::int;
			_ledger_id = (_product->>'ledger_id')::bigint;

			if _id is null then
				/* we fill ledger with the accounting entry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					_contract_id,
					null,
					null
				) INTO _ledger_id;

				/* new insertion */
				insert into accounting.warehouse_services (
					operation_number,
					counterparty_id,
					contract_id,
					financing,
					
					service_nomenclature_id,
					quantity,
					unit_price,
					estimate_id,
					debit,
					credit,
					ledger_id,
					description,
					
					comment,
					created
				) values (
					_operation_number,
					_counterparty_id,
					_contract_id,
					_financing,

					(_product->>'service_nomenclature_id')::bigint,
					_quantity,
					_unit_price,
					(_product->>'estimate_id')::bigint,
					_debit,
					_credit,
					_ledger_id,
					_product->>'description',

					_comment,
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);
			else
				isUpdate = true;
				/* we update ledger with new accouting entry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					_credit,
					round(_unit_price * _quantity, 2),
					_contract_id,
					null,
					_ledger_id
				) INTO _ledger_id;
				
				/* we update rows */
				update accounting.warehouse_services ws SET
					operation_number = _operation_number,
					counterparty_id = _counterparty_id,
					contract_id = _contract_id,
					financing = _financing,
					
					service_nomenclature_id = (_product->>'service_nomenclature_id')::bigint,
					quantity = _quantity,
					unit_price = _unit_price,
					estimate_id = (_product->>'estimate_id')::bigint,
					debit = _debit,
					credit = _credit,
					ledger_id = _ledger_id,
					description = _product->>'description',
					
					comment = _comment,
					created = CASE
    				    WHEN _created_date IS NOT NULL
    				    THEN jsonb_set(
    				             ws.created,
    				             '{date}',
    				             to_jsonb(_created_date)
    				         )
    				    ELSE ws.created
    				END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', LOCALTIMESTAMP(0)
					)
				where id = _id;
			end if;
    	END LOOP;

		perform accounting.upsert_warehouse_total_routing (
			(jdata->>'routing')::jsonb || jsonb_build_object('warehouse_id', _operation_number)
		);

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'status', 200,
			'id', _operation_number
		);
	end;
$BODY$;









CREATE OR REPLACE FUNCTION accounting.get_warehouse_services_by_id(
	_department_id integer,
	_operation_number bigint
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN

		with services_parent as (
		 	SELECT DISTINCT ON (ws.operation_number)
				operation_number,
				counterparty_id,
				contract_id,
				financing,
				comment,
				status,
				(created->>'date')::date created_date
			FROM accounting.warehouse_services ws
			where ws.operation_number = _operation_number
			ORDER BY ws.operation_number, ws.id
		),
		services_child AS (
		    SELECT 
		        ws.operation_number,
		        jsonb_agg(
		            jsonb_build_object(
		                'key', ws.id,
		                'id', ws.id,
		            	'service_nomenclature_id', ws.service_nomenclature_id,
		            	'unit_price', ws.unit_price,
		            	'quantity', ws.quantity,
						'debit', ws.debit,
						'credit', ws.credit,
						'estimate_id', ws.estimate_id,
						'ledger_id', ws.ledger_id,
						'description', ws.description
		            ) ORDER BY ws.service_nomenclature_id
		        ) AS table_data
		    FROM accounting.warehouse_services ws
		    GROUP BY ws.operation_number
		),
		services as (
			SELECT 
			    p.*,
			    c.table_data
			FROM services_parent p
			JOIN services_child c 
			USING (operation_number)
		),
		routing_1 as (
			select
				wtr.warehouse_id,
				row_number() over(
					order by wtr."createdAt"
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
				and warehouse_id = _operation_number
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
			order by wtr."createdAt"
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
		) select jsonb_build_object(
			'operation_number', s.operation_number,
			'counterparty_id', s.counterparty_id,
			'contract_id', s.contract_id,
			'financing', s.financing,
			'comment', s.comment,
			'created_date', s.created_date,
			'status', s.status,
			'contract', cc.contract,		
			'table_data', s.table_data,
			'routing', r2.routing
		) from services s into _result
		left join routing_2 r2
		on s.operation_number = r2.warehouse_id
		left join commons.counterparty_contracts cc
		on s.contract_id = cc.id;

		return jsonb_build_object(
			'status', 200,
			'result', _result
		);
	end;
$BODY$;




select accounting.get_warehouse_services (
	157, 'budget'
)


CREATE OR REPLACE FUNCTION accounting.get_warehouse_services(
	_department_id integer,
	_financing accounting.budget_distribution_type,
	_operation_number bigint DEFAULT NULL::bigint,
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
		select count(DISTINCT(operation_number)) into total
		from accounting.warehouse_services
		where financing = _financing 
		and (_operation_number is null or _operation_number = operation_number)
		and (_created_from is null or _created_from::date <= (created->>'date')::date)
		and (_created_to is null or _created_to::date >= (created->>'date')::date)
		and (_counterparty_id is null or _counterparty_id = counterparty_id);

		/* MAIN QUERY */
		with services_parent as (
		 	SELECT DISTINCT ON (ws.operation_number)
				operation_number,
				counterparty_id,
				contract_id,
				financing,
				comment,
				status,
				(created->>'date')::date created_date
			FROM accounting.warehouse_services ws
			where financing = _financing 
			and (_operation_number is null or _operation_number = operation_number)
			and (_created_from is null or _created_from::date <= (created->>'date')::date)
			and (_created_to is null or _created_to::date >= (created->>'date')::date)
			and (_counterparty_id is null or _counterparty_id = counterparty_id)
			ORDER BY ws.operation_number, ws.id
		),
		services_child AS (
		    SELECT 
		        ws.operation_number,
		        jsonb_agg(
		            jsonb_build_object(
						'key', ws.id,
		                'id', ws.id,
		            	'service_nomenclature_id', ws.service_nomenclature_id,
		            	'unit_price', ws.unit_price,
		            	'quantity', ws.quantity,
						'debit', ws.debit,
						'credit', ws.credit,
						'estimate_id', ws.estimate_id,
						'ledger_id', ws.ledger_id,
						'description', ws.description
		            ) ORDER BY ws.service_nomenclature_id
		        ) AS table_data
		    FROM accounting.warehouse_services ws
		    GROUP BY ws.operation_number
		),
		services as (
			SELECT 
			    p.*,
			    c.table_data
			FROM services_parent p
			JOIN services_child c 
			USING (operation_number)
		),
		routing_1 as (
			select
				wtr.warehouse_id,
				row_number() over(
					order by wtr."createdAt"
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
				and (_operation_number is null or _operation_number = warehouse_id)
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
			order by wtr."createdAt"
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
		),
		final_join as (
			select
				row_number() over(order by s.operation_number) as key,
				s.operation_number,
				s.counterparty_id,
				s.contract_id,
				s.financing,
				s.comment,
				s.created_date,
				s.status,
				cc.contract,
				s.table_data,
				r2.routing
			from services s
			left join routing_2 r2
				on s.operation_number = r2.warehouse_id
			left join commons.counterparty_contracts cc
				on s.contract_id = cc.id
			order by s.operation_number desc
			limit _limit offset _offset
		) select jsonb_agg(fj)
		into _result
		from final_join fj;

		return jsonb_build_object(
			'status', 200,
			'total', total,
			'results', _result
		);
	end;
$BODY$;









CREATE OR REPLACE FUNCTION accounting.get_warehouse_services_id(
	)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_operation_number bigint;
	BEGIN

		select operation_number 
		into _last_operation_number
		from accounting.warehouse_services
		group by operation_number
		order by operation_number desc
		limit 1;
		
		return _last_operation_number;
	end;
$BODY$;



