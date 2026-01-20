/* -------------------------------------- */
-- 	       Advance Report TMZ/OS
/* -------------------------------------- */

create table if not exists accounting.advance_report_tmzos (
	operation_number bigint not null,
	financing accounting.budget_distribution_type not null,
	credit integer not null default 114610 references accounting.accounts (account),
	staff_id bigint not null references hr.staff (id),
	description text,

	/* table part */
	id bigserial primary key,
	document_name text not null,
	document_number integer not null,
	document_date date not null,
	name_id bigint not null references commons.nomenclature (id),
	quantity numeric not null,
	unit_price numeric not null,
	debit integer not null references accounting.accounts (account),
	ledger_id bigint not null references accounting.ledger (id),
	counterparty text not null,
	
	created jsonb not null,
	updated jsonb not null
);



select * from accounting.advance_report_tmzos;



select * from accounting.ledger
where id > 123
order by id;



create or replace function accounting.upsert_advance_report_tmzos (
	jdata jsonb
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		_user_id uuid = (jdata->>'user_id')::uuid;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_department_id bigint = (jdata->>'department_id')::bigint;
		_description text = jdata->>'description';
		_created_date date = (jdata->>'created_date')::date;
		isUpdate boolean = false;
				
		/* table variables */
		_row jsonb;
		_id bigint;
		_unit_price numeric;
		_quantity numeric;
		_ledger_id bigint;
		_debit integer;
	BEGIN

		/* GENERATING NEW OPERATION NUMBER */
		if _operation_number is null then
			SELECT coalesce(max(sub.operation_number), 0) + 1
			into _operation_number from (
				SELECT operation_number
	        	FROM accounting.advance_report_tmzos
	        	GROUP BY operation_number
	        	ORDER BY operation_number DESC
	        	LIMIT 1
			) sub;
		end if;

		FOR _row IN SELECT * FROM jsonb_array_elements((jdata->>'table_data')::jsonb) LOOP
			_id = (_row->>'id')::bigint;
			_quantity = (_row->>'quantity')::numeric;
			_unit_price = (_row->>'unit_price')::numeric;
			_debit = (_row->>'debit')::int;
			_ledger_id = (_row->>'ledger_id')::bigint;

			-- insertion
			if _id is null then
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					114610,
					round(_unit_price * _quantity, 2),
					null,
					_staff_id,
					null
				) INTO _ledger_id;

				insert into accounting.advance_report_tmzos (
					operation_number,
					financing,
					department_id,
					staff_id,
					description,
				
					/* table part */
					document_name,
					document_number,
					document_date,
					name_id,
					quantity,
					unit_price,
					debit,
					ledger_id,
					counterparty,
					created
				) values (
					_operation_number,
					_financing,
					_department_id,
					_staff_id,
					_description,
				
					/* table part */
					_row->>'document_name',
					(_row->>'document_number')::integer,
					(_row->>'document_date')::date,
					(_row->>'name_id')::bigint,
					_quantity,
					_unit_price,
					_debit,
					_ledger_id,
					_row->>'counterparty',
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);

			-- update
			else
				isUpdate = true;
				
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					114610,
					round(_unit_price * _quantity, 2),
					null,
					_staff_id,
					_ledger_id
				) INTO _ledger_id;

				update accounting.advance_report_tmzos art set 
					operation_number = _operation_number,
					financing = _financing,
					department_id = _department_id,
					staff_id = _staff_id,
					description = _description,
				
					/* table part */
					document_name = _row->>'document_name',
					document_number = (_row->>'document_number')::integer,
					document_date = (_row->>'document_date')::date,
					name_id = (_row->>'name_id')::bigint,
					quantity = _quantity,
					unit_price = _unit_price,
					debit = _debit,
					ledger_id = _ledger_id,
					counterparty = _row->>'counterparty',
					
					created = CASE
    				    WHEN _created_date IS NOT NULL
    				    THEN jsonb_set(
    				             art.created,
    				             '{date}',
    				             to_jsonb(_created_date)
    				         )
    				    ELSE art.created
    				END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', localtimestamp(0)
					)
				where id = _id;

			end if;
		END LOOP;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'operation_number', _operation_number,
			'status', 200
		);
	END;
$BODY$;
/* -------------------------------------- */
--------------------------------------------
/* -------------------------------------- */



/* -------------------------------------- */
-- 	       Advance Report OPLATA
/* -------------------------------------- */

create table if not exists accounting.advance_report_oplata (
	operation_number bigint not null,
	financing accounting.budget_distribution_type not null,
	credit integer not null default 114610 references accounting.accounts (account),
	staff_id bigint not null references hr.staff (id),
	description text,

	/* table part */
	id bigserial primary key,
	document_name text not null,
	document_number integer not null,
	document_date date not null,
	debit integer not null references accounting.accounts (account),
	ledger_id bigint not null references accounting.ledger (id),
	
	amount numeric not null,
	counterparty_id bigint not null references accounting.counterparty(id),
	contract_id bigint not null references commons.counterparty_contracts (id),
	contract_text text,
	content text,
	
	created jsonb not null,
	updated jsonb not null
);




select * from accounting.advance_report_oplata;



select * from accounting.ledger
where id > 123
order by id;



select * from accounting.product_transfer;



create or replace function accounting.upsert_advance_report_oplata (
	jdata jsonb
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		_user_id uuid = (jdata->>'user_id')::uuid;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_department_id bigint = (jdata->>'department_id')::bigint;
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_description text = jdata->>'description';
		_created_date date = (jdata->>'created_date')::date;
		isUpdate boolean = false;
				
		/* table variables */
		_row jsonb;
		_id bigint;
		_amount numeric;
		_ledger_id bigint;
		_debit integer;
		_contract_id bigint;
	BEGIN

		/* GENERATING NEW OPERATION NUMBER */
		if _operation_number is null then
			SELECT coalesce(max(sub.operation_number), 0) + 1
			into _operation_number from (
				SELECT operation_number
	        	FROM accounting.advance_report_oplata
	        	GROUP BY operation_number
	        	ORDER BY operation_number DESC
	        	LIMIT 1
			) sub;
		end if;

		FOR _row IN SELECT * FROM jsonb_array_elements((jdata->>'table_data')::jsonb) LOOP
			_id = (_row->>'id')::bigint;
			_amount = (_row->>'amount')::numeric;
			_debit = (_row->>'debit')::int;
			_ledger_id = (_row->>'ledger_id')::bigint;
			_contract_id = (_row->>'contract_id')::bigint;

			-- insertion
			if _id is null then
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					114610,
					_amount,
					_contract_id,
					_staff_id,
					null
				) INTO _ledger_id;

				insert into accounting.advance_report_oplata (
					operation_number,
					financing,
					department_id,
					staff_id,
					description,
				
					/* table part */
					document_name,
					document_number,
					document_date,
					debit,
					ledger_id,
					
					amount,
					counterparty_id,
					contract_id,
					contract_text,
					content,
					
					created
				) values (
					_operation_number,
					_financing,
					_department_id,
					_staff_id,
					_description,
				
					/* table part */
					_row->>'document_name',
					(_row->>'document_number')::integer,
					(_row->>'document_date')::date,
					_debit,
					_ledger_id,

					_amount,
					(_row->>'counterparty_id')::bigint,
					_contract_id,
					_row->>'contract_text',
					_row->>'content',
					
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);

			-- update
			else
				isUpdate = true;
				
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					114610,
					_amount,
					_contract_id,
					_staff_id,
					_ledger_id
				) INTO _ledger_id;

				update accounting.advance_report_oplata aro set 
					operation_number = _operation_number,
					financing = _financing,
					department_id = _department_id,
					staff_id = _staff_id,
					description = _description,
				
					/* table part */
					document_name = _row->>'document_name',
					document_number = (_row->>'document_number')::integer,
					document_date = (_row->>'document_date')::date,
					debit = _debit,
					ledger_id = _ledger_id,
					
					amount = _amount,
					counterparty_id = (_row->>'counterparty_id')::bigint,
					contract_id = _contract_id,
					contract_text = _row->>'contract_text',
					content = _row->>'content',
					
					created = CASE
    				    WHEN _created_date IS NOT NULL
    				    THEN jsonb_set(
    				             aro.created,
    				             '{date}',
    				             to_jsonb(_created_date)
    				         )
    				    ELSE aro.created
    				END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', localtimestamp(0)
					)
				where id = _id;

			end if;
		END LOOP;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'operation_number', _operation_number,
			'status', 200
		);
	END;
$BODY$;
/* -------------------------------------- */
--------------------------------------------
/* -------------------------------------- */





/* -------------------------------------- */
-- 	       Advance Report PROCHEE
/* -------------------------------------- */

create table if not exists accounting.advance_report_prochee (
	operation_number bigint not null,
	financing accounting.budget_distribution_type not null,
	credit integer not null default 114610 references accounting.accounts (account),
	staff_id bigint not null references hr.staff (id),
	description text,

	/* table part */
	id bigserial primary key,
	document_name text not null,
	document_number integer not null,
	document_date date not null,
	debit integer not null references accounting.accounts (account),
	ledger_id bigint not null references accounting.ledger (id),
	
	amount numeric not null,
	content text,
	cost_analytics_id bigint references commons.cost_analytics (id),
	
	created jsonb not null,
	updated jsonb not null
);





select * from accounting.advance_report_prochee;



select * from accounting.ledger
where id > 123
order by id;



create or replace function accounting.upsert_advance_report_prochee (
	jdata jsonb
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_operation_number bigint = (jdata->>'operation_number')::bigint;
		_user_id uuid = (jdata->>'user_id')::uuid;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_department_id bigint = (jdata->>'department_id')::bigint;
		_staff_id bigint = (jdata->>'staff_id')::bigint;
		_description text = jdata->>'description';
		_created_date date = (jdata->>'created_date')::date;
		isUpdate boolean = false;
				
		/* table variables */
		_row jsonb;
		_id bigint;
		_amount numeric;
		_ledger_id bigint;
		_debit integer;
	BEGIN

		/* GENERATING NEW OPERATION NUMBER */
		if _operation_number is null then
			SELECT coalesce(max(sub.operation_number), 0) + 1
			into _operation_number from (
				SELECT operation_number
	        	FROM accounting.advance_report_prochee
	        	GROUP BY operation_number
	        	ORDER BY operation_number DESC
	        	LIMIT 1
			) sub;
		end if;

		FOR _row IN SELECT * FROM jsonb_array_elements((jdata->>'table_data')::jsonb) LOOP
			_id = (_row->>'id')::bigint;
			_amount = (_row->>'amount')::numeric;
			_debit = (_row->>'debit')::int;
			_ledger_id = (_row->>'ledger_id')::bigint;

			-- insertion
			if _id is null then
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					114610,
					_amount,
					null,
					_staff_id,
					null
				) INTO _ledger_id;

				insert into accounting.advance_report_prochee (
					operation_number,
					financing,
					department_id,
					staff_id,
					description,
				
					/* table part */
					document_name,
					document_number,
					document_date,
					debit,
					ledger_id,
					
					amount,
					cost_analytics_id,
					content,
					
					created
				) values (
					_operation_number,
					_financing,
					_department_id,
					_staff_id,
					_description,
				
					/* table part */
					_row->>'document_name',
					(_row->>'document_number')::integer,
					(_row->>'document_date')::date,
					_debit,
					_ledger_id,

					_amount,
					(_row->>'cost_analytics_id')::bigint,
					_row->>'content',
					
					jsonb_build_object(
						'user_id', _user_id,
						'date', coalesce(_created_date, LOCALTIMESTAMP(0))
					)
				);

			-- update
			else
				isUpdate = true;
				
				/* we fill ledger with the accountingentry */
				SELECT accounting.upsert_ledger(
					_financing,
					_debit,
					114610,
					_amount,
					null,
					_staff_id,
					_ledger_id
				) INTO _ledger_id;

				update accounting.advance_report_prochee arp set 
					operation_number = _operation_number,
					financing = _financing,
					department_id = _department_id,
					staff_id = _staff_id,
					description = _description,
				
					/* table part */
					document_name = _row->>'document_name',
					document_number = (_row->>'document_number')::integer,
					document_date = (_row->>'document_date')::date,
					debit = _debit,
					ledger_id = _ledger_id,
					
					amount = _amount,
					cost_analytics_id = (_row->>'cost_analytics_id')::bigint,
					content = _row->>'content',
					
					created = CASE
    				    WHEN _created_date IS NOT NULL
    				    THEN jsonb_set(
    				             arp.created,
    				             '{date}',
    				             to_jsonb(_created_date)
    				         )
    				    ELSE arp.created
    				END,
					updated = jsonb_build_object(
						'user_id', _user_id,
						'date', localtimestamp(0)
					)
				where id = _id;

			end if;
		END LOOP;

		return json_build_object(
			'msg', case when isUpdate then 'updated' else 'created' end,
			'operation_number', _operation_number,
			'status', 200
		);
	END;
$BODY$;
/* -------------------------------------- */
--------------------------------------------
/* -------------------------------------- */


/* -------------------------------------- */
-- 	     Advance Report Staff Debts
/* -------------------------------------- */


select accounting.fetch_staff_debts_for_advance_reports (
	3674
);

create or replace function accounting.fetch_staff_debts_for_advance_reports (
	_staff_id bigint,
	_limit integer default 100,
	_offset integer default 0
)
RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result jsonb;
	BEGIN

		with advances as (
			select
				'payment_order_outgoing' as "section",
				financing,
				id,
				cash_flow_article_id,
				amount,
				debit,
				credit,
				(created->>'date')::date created_date
			from accounting.payment_order_outgoing
			where staff_id = _staff_id
	
			union all
				
			select
				'cash_payment_order' as "section",
				financing,
				id,
				cash_flow_article_id,
				amount,
				debit,
				credit,
				(created->>'date')::date created_date
			from accounting.cash_payment_order
			where staff_id = _staff_id
		),
		returnss as (
			select
				round(quantity * unit_price, 2) as amount,
				advance_id
			from accounting.advance_report_tmzos
			where staff_id = _staff_id
	
			union all
	
			select
				amount,
				advance_id
			from accounting.advance_report_oplata
			where staff_id = _staff_id
	
			union all
	
			select
				amount,
				advance_id
			from accounting.advance_report_prochee
			where staff_id = _staff_id
		),
		lefts as (
			select
				a.id,
				a."section",
				a.financing,
				a.cash_flow_article_id,
				(a.amount - sum(coalesce(r.amount, 0))) as left_amount,
				a.debit,
				a.credit,
				a.created_date
			from advances a
			left join returnss r
				on a.id = r.advance_id
			group by a.id,
				a."section",
				a.financing,
				a.cash_flow_article_id,
				a.amount,
				a.debit,
				a.credit,
				a.created_date
		),
		table_total as (
			select count(*) as total from lefts
		),
		paginated as (
			select jsonb_build_object(
				'key', row_number() over(order by created_date),
				'id', id,
				'section', "section",
				'financing', financing,
				'cash_flow_article_id', cash_flow_article_id,
				'left_amount', left_amount,
				'debit', debit,
				'credit', credit,
				'created_date', created_date
			) as aggregated from lefts
			order by created_date
			limit _limit offset _offset
		) select jsonb_build_object (
			'status', 200,
			'results', jsonb_agg(p.aggregated),
			'total', (select total from table_total)
		) into _result from paginated p;

		return _result;
	END;
$BODY$;
/* -------------------------------------- */
--------------------------------------------
/* -------------------------------------- */

create or replace function accounting.upsert_advance_report (
	_advance_type accounting.advance_type,
	jdata jsonb
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_result jsonb;
	BEGIN

		if _advance_type = 'tmz' then
			SELECT accounting.upsert_advance_report_tmzos (jdata)
			INTO _result;
			
		elsif _advance_type = 'oplata' then
			SELECT accounting.upsert_advance_report_oplata (jdata)
			INTO _result;
			
		elsif _advance_type = 'prochee' then
			SELECT accounting.upsert_advance_report_prochee (jdata)
			INTO _result;
			
		end if;

		return _result;
	END;
$BODY$;




select accounting.get_advance_report();





CREATE OR REPLACE FUNCTION accounting.get_advance_report(
	_financing accounting.budget_distribution_type,
	_advance_type accounting.advance_type,
	_date_from text DEFAULT NULL::text,
	_date_to text DEFAULT NULL::text,
	_staff_id bigint DEFAULT NULL::bigint,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_result json;
	BEGIN

		/* -------------------- tmz -------------------- */
		if _advance_type = 'tmz' then
			with main_parent as (
				select DISTINCT ON (operation_number)
					operation_number,
					financing,
					department_id,
					staff_id,
					description,
					(created->>'date')::date created_date
				from  accounting.advance_report_tmzos
				where financing = _financing 
				and (_date_from is null or _date_from::date <= (created->>'date')::date)
				and (_date_to is null or _date_to::date >= (created->>'date')::date)
				and (_staff_id is null or _staff_id = staff_id)
				ORDER BY operation_number, id
			),
			main_child as (
				SELECT 
			        operation_number,
			        jsonb_agg(
			            jsonb_build_object(
			                'id', id,
			                'key', id,
							'document_name', document_name,
			            	'document_number', document_number,
			            	'document_date', document_date,
			            	'counterparty', counterparty,
			            	'name_id', name_id,
			            	'unit_price', unit_price,
			            	'quantity', quantity,
							'debit', debit,
							'ledger_id', ledger_id
			            ) ORDER BY (created->>'date')::date
			        ) AS table_data
			    FROM accounting.advance_report_tmzos
			    GROUP BY operation_number
			),
			main as (
				SELECT 
				    p.*,
				    c.table_data
				FROM main_parent p
				JOIN main_child c 
				USING (operation_number)
			),
			total_count as (
				select count(*) total from main
			),
			paginated as (
				select jsonb_build_object(
					'key', row_number() over(order by created_date),
					'operation_number', operation_number,
					'financing', financing,
					'department_id', department_id,
					'staff_id', staff_id,
					'description', description,
					'table_data', table_data,
					'created_date', created_date
				) aggregated from main
				order by created_date 
				limit _limit offset _offset
			)
			select jsonb_build_object(
				'results', jsonb_agg(p.aggregated),
				'total', (select total from total_count),
				'status', 200
			) into _result from paginated p;

		/* -------------------- oplata -------------------- */
		elsif _advance_type = 'oplata' then
			with main_parent as (
				select DISTINCT ON (operation_number)
					operation_number,
					financing,
					department_id,
					staff_id,
					description,
					(created->>'date')::date created_date
				from  accounting.advance_report_oplata
				where financing = _financing 
				and (_date_from is null or _date_from::date <= (created->>'date')::date)
				and (_date_to is null or _date_to::date >= (created->>'date')::date)
				and (_staff_id is null or _staff_id = staff_id)
				ORDER BY operation_number, id
			),
			main_child as (
				SELECT 
			        operation_number,
			        jsonb_agg(
			            jsonb_build_object(
			                'id', id,
							'key', id,
			            	'document_name', document_name,
			            	'document_number', document_number,
			            	'document_date', document_date,
			            	'amount', amount,
							'debit', debit,
							'ledger_id', ledger_id,
							'counterparty_id', counterparty_id,
							'contract_id', contract_id,
							'contract_text', contract_text,
							'content', content
			            ) ORDER BY (created->>'date')::date
			        ) AS table_data
			   FROM accounting.advance_report_oplata
			   GROUP BY operation_number
			),
			main as (
				SELECT 
				    p.*,
				    c.table_data
				FROM main_parent p
				JOIN main_child c 
				USING (operation_number)
			),
			total_count as (
				select count(*) total from main
			),
			paginated as (
				select jsonb_build_object(
					'key', row_number() over(order by created_date),
					'operation_number', operation_number,
					'financing', financing,
					'department_id', department_id,
					'staff_id', staff_id,
					'description', description,
					'table_data', table_data,
					'created_date', created_date
				) aggregated from main
				order by created_date 
				limit _limit offset _offset
			)
			select jsonb_build_object(
				'results', jsonb_agg(p.aggregated),
				'total', (select total from total_count),
				'status', 200
			) into _result from paginated p;

		/* -------------------- prochee -------------------- */
		elsif _advance_type = 'prochee' then
			with main_parent as (
				select DISTINCT ON (operation_number)
					operation_number,
					financing,
					department_id,
					staff_id,
					description,
					(created->>'date')::date created_date
				from  accounting.advance_report_prochee
				where financing = _financing 
				and (_date_from is null or _date_from::date <= (created->>'date')::date)
				and (_date_to is null or _date_to::date >= (created->>'date')::date)
				and (_staff_id is null or _staff_id = staff_id)
				ORDER BY operation_number, id
			),
			main_child as (
				SELECT 
			        operation_number,
			        jsonb_agg(
			            jsonb_build_object(
			                'id', id,
							'key', id,
			            	'document_name', document_name,
			            	'document_number', document_number,
			            	'document_date', document_date,
			            	'amount', amount,
							'debit', debit,
							'ledger_id', ledger_id,
							'cost_analytics_id', cost_analytics_id,
							'content', content
			            ) ORDER BY (created->>'date')::date
			        ) AS table_data
			   FROM accounting.advance_report_prochee
			   GROUP BY operation_number
			),
			main as (
				SELECT 
				    p.*,
				    c.table_data
				FROM main_parent p
				JOIN main_child c 
				USING (operation_number)
			),
			total_count as (
				select count(*) total from main
			),
			paginated as (
				select jsonb_build_object(
					'key', row_number() over(order by created_date),
					'operation_number', operation_number,
					'financing', financing,
					'department_id', department_id,
					'staff_id', staff_id,
					'description', description,
					'table_data', table_data,
					'created_date', created_date
				) aggregated from main
				order by created_date 
				limit _limit offset _offset
			)
			select jsonb_build_object(
				'results', jsonb_agg(p.aggregated),
				'total', (select total from total_count),
				'status', 200
			) into _result from paginated p;
			
		end if;

		return _result;
	end;
$BODY$;
















select accounting.get_advance_report_report_number(

);






CREATE OR REPLACE FUNCTION accounting.get_advance_report_id(
	_advance_type accounting.advance_type
)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE	
		_last_id bigint;
	BEGIN


		if _advance_type = 'tmz' then
			select operation_number
			into _last_id
			from accounting.advance_report_tmzos
			group by operation_number
			order by operation_number desc
			limit 1;
			
		elsif _advance_type = 'oplata' then
			select operation_number
			into _last_id
			from accounting.advance_report_oplata
			group by operation_number
			order by operation_number desc
			limit 1;
			
		elsif _advance_type = 'prochee' then
			select operation_number
			into _last_id
			from accounting.advance_report_prochee
			group by operation_number
			order by operation_number desc
			limit 1;
			
		end if;
		
		return _last_id;
	end;
$BODY$;

