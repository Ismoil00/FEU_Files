

select * from accounting.assets_recognition

select * from hr.staff

CREATE INDEX idx_assets_recognition_committee 
ON accounting.assets_recognition USING GIN (committee);

select * from accounting.assets_recognition;



select accounting.download_assets_recognition_all_files (1, 157);



/* ALL FILES */

select accounting.download_assets_recognition_all_files (1, 157)

CREATE OR REPLACE FUNCTION accounting.download_assets_recognition_all_files(
	_operation_number bigint,
	_department_id bigint
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result json;
	BEGIN

		with staffs_data as (
			select
				s.id as staff_id,
				jsonb_build_object(
					'committee_fullname', concat_ws(' ', s.lastname, s.firstname, s.middlename),
					'committee_jobtitle', j.staff_jobtitle
				) as paginated
			from hr.staff s
			left join (
				SELECT
					m.staff_id,
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
			order by s.lastname
		),
		main as (
			select distinct on (operation_number)
				ar.operation_number,
				'Рачабзода Шариф Рачаб' as head,
				(
					select department->>'tj'
					from commons.department
					where id = 12
				) as organization,
				(ar.created->>'date')::date as created_date,
				(
					select sd.paginated 
					from staffs_data sd
					where ar.committee_chairman = sd.staff_id
				) as chairman,
				(
					select jsonb_agg(sd.paginated) 
					from staffs_data sd
					where ar.committee @> to_jsonb(sd.staff_id)
				) as committees
			from accounting.assets_recognition ar
			where operation_number = _operation_number
		),
		child as (
			select 
				t.operation_number,
				jsonb_agg(t.paginated) as table_data
			from (
				select
					ar.operation_number,
					jsonb_build_object(
						'key', row_number() over (order by (ar.created->>'date')::date),
						'location_from', (
							select name->>'tj'
							from commons.storage_location
							where id = ar.storage_location_id
						),
						'department_to', (
							select department->>'tj' 
							from commons.department
							where id = ar.department_id
						),
						'name', (
							select name->>'tj'
							from commons.nomenclature
							where id = ar.name_id
						),
						'debit', ar.debit,
						'credit', ar.credit,
						'initial_price', ar.unit_price,
						'months', ar.depreciation_period,
						'initial_ware_price', 0,
						'yearly_ware_percent', ar.depreciation_percent,
						'monthly_ware_amount', round(ar.unit_price * ar.depreciation_percent / 1200, 4),
						'invent_number', ar.inventory_number
					) as paginated
				from accounting.assets_recognition ar
				where ar.operation_number = _operation_number
				order by (ar.created->>'date')::date
			) t group by t.operation_number
		),
		routing_1 as (
			select
				wtr.warehouse_id,
				row_number() over(
					order by wtr."createdAt"
				) as rownumber,
				jsonb_build_object(
					'user_id', d.user_id,
					'fullname', d.fullname,
					'status', wtr.status,
					'date', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::date 
						else (wtr."createdAt")::date end,
					'time', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::time 
						else (wtr."createdAt")::time end
				) routing_object
			from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'assets_recognition'
				and warehouse_id = _operation_number
			) wtr
			left join (
				select 
			  		j.id,
					u.id as user_id,
					concat_ws(' ', s.lastname, s.firstname, s.middlename) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
				left join auth.user u
				on j.staff_id = u.staff_id
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
		)
		select jsonb_build_object(
			'operation_number', m.operation_number,
			'head', m.head,
			'organization', m.organization,
			'created_date', m.created_date,
			'chairman', m.chairman,
			'committees', m.committees,
			'table_data', c.table_data,
			'routing', r2.routing
		) into _result
		from main m
		left join child c
			using(operation_number)
		left join routing_2 r2
			on m.operation_number = r2.warehouse_id;

		return _result; 
	end;
$BODY$;



select * from accounting.assets_recognition













/* ONE FILES */


select * from commons.nomenclature;

select * from accounting.assets_recognition;


select accounting.download_assets_recognition_one_file (15, 1, 157)




CREATE OR REPLACE FUNCTION accounting.download_assets_recognition_one_file (
	_id bigint,
	_operation_number bigint,
	_department_id bigint
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result json;
	BEGIN

		with staffs_data as (
			select
				s.id as staff_id,
				jsonb_build_object(
					'committee_fullname', concat_ws(' ', s.lastname, s.firstname, s.middlename),
					'committee_jobtitle', j.staff_jobtitle
				) as paginated
			from hr.staff s
			left join (
				SELECT
					m.staff_id,
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
			order by s.lastname
		),
		main as (
			select
				ar.operation_number,
				jsonb_build_object(
					'operation_number', ar.operation_number,
					'head', 'Рачабзода Шариф Рачаб',
					'organization', (
						select department->>'tj'
						from commons.department
						where id = 12
					),
					'created_date', (ar.created->>'date')::date,
					'chairman', (
						select sd.paginated 
						from staffs_data sd
						where ar.committee_chairman = sd.staff_id
					),
					'committees', (
						select jsonb_agg(sd.paginated) 
						from staffs_data sd
						where ar.committee @> to_jsonb(sd.staff_id)
					),
					'location_from', (
						select name->>'tj'
						from commons.storage_location
						where id = ar.storage_location_id
					),
					'department_to', (
						select department->>'tj' 
						from commons.department
						where id = ar.department_id
					),
					'name', (
						select name->>'tj'
						from commons.nomenclature
						where id = ar.name_id
					),
					'code', (
						select code
						from commons.nomenclature
						where id = ar.name_id
					),
					'product_category', (
						select p.name->>'tj'
						from commons.nomenclature n
						left join commons.product_category p
							on p.id = any(n.product_category_id)
						where n.id = ar.name_id
					),
					'debit', ar.debit,
					'credit', ar.credit,
					'initial_price', ar.unit_price,
					'months', ar.depreciation_period,
					'initial_ware_price', 0,
					'init_price_minus_init_ware_price', ar.unit_price - 0,
					'yearly_ware_percent', ar.depreciation_percent,
					'monthly_ware_amount', round(ar.unit_price * ar.depreciation_percent / 1200, 4),
					'invent_number', ar.inventory_number
				) as paginated
			from accounting.assets_recognition ar
			where id = _id 
			and operation_number = _operation_number
		),
		routing_1 as (
			select
				wtr.warehouse_id,
				row_number() over(
					order by wtr."createdAt"
				) as rownumber,
				jsonb_build_object(
					'user_id', d.user_id,
					'fullname', d.fullname,
					'status', wtr.status,
					'date', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::date 
						else (wtr."createdAt")::date end,
					'time', case when wtr."updatedAt" is not null
						then (wtr."updatedAt")::time 
						else (wtr."createdAt")::time end
				) routing_object
			from (
				select * from accounting.warehouse_total_routing
				where warehouse_section = 'assets_recognition'
				and warehouse_id = _operation_number
			) wtr
			left join (
				select 
			  		j.id,
					u.id as user_id,
					concat_ws(' ', s.lastname, s.firstname, s.middlename) as fullname 
				from hr.jobposition j
			  	left join hr.staff s
			  	on j.staff_id = s.id
				left join auth.user u
				on j.staff_id = u.staff_id
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
		)
		select m.paginated || jsonb_build_object('routing', r2.routing) 
		into _result from main m
		left join routing_2 r2
			on m.operation_number = r2.warehouse_id;
		
		return _result; 
	end;
$BODY$;




