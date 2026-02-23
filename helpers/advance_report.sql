-- eccepted_amount_till_today
-- total_accepted_amount
-- total_expenses
-- left_in_staff_hand_amount = total_accepted_amount - total_expenses > 0
-- to_staff_owed_amount = total_accepted_amount - total_expenses < 0




select * from accounting.advance_report_oplata;



select * from accounting.advance_report_prochee;



select * from accounting.advance_report_tmzos;


select * from hr.staff


select accounting.download_advance_report(3674)


CREATE OR REPLACE FUNCTION accounting.download_advance_report(_staff_id bigint)
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
				staff_id,
				id,
				'bank'::varchar(10) as "section",
				amount,
				(created->>'date')::date as created_date
			from accounting.payment_order_outgoing
			where staff_id = _staff_id
	
			union all
				
			select
				staff_id,
				id,
				'cash'::varchar(10) as "section",
				amount,
				(created->>'date')::date as created_date
			from accounting.cash_payment_order
			where staff_id = _staff_id
		),
		returnss as (
			select
				staff_id,
				document_name,
				document_number,
				document_date,
				round(quantity * unit_price, 2) as amount,
				advance_id,
				advance_section,
				credit,
				debit,
				description
			from accounting.advance_report_tmzos
			where staff_id = _staff_id
	
			union all
	
			select
				staff_id,
				document_name,
				document_number,
				document_date,
				amount,
				advance_id,
				advance_section,
				credit,
				debit,
				description
			from accounting.advance_report_oplata
			where staff_id = _staff_id
	
			union all
	
			select
				staff_id,
				document_name,
				document_number,
				document_date,
				amount,
				advance_id,
				advance_section,
				credit,
				debit,
				description
			from accounting.advance_report_prochee
			where staff_id = _staff_id
		),
		filtered as (
			select
				a.staff_id,
				a.id,
				a."section",
				a.amount as eccepted_amount_per_row,
				sum(coalesce(r.amount, 0)) as spent_amount_per_row,
				a.created_date,
				jsonb_agg(
					jsonb_build_object(
						'doc_name', r.document_name,
						'doc_numb', r.document_number,
						'doc_date', r.document_date,
						'amount', r.amount,
						'credit', r.credit,
						'debit', r.debit,
						'description', r.description
					)
				) as aggregateds
			from advances a
			left join returnss r
				on a.staff_id = r.staff_id
				and a.id = r.advance_id
				and a.section = r.advance_section
			group by a.staff_id, a.id, a."section", a.amount, a.created_date
			having a.amount - sum(coalesce(r.amount, 0)) != 0
		),
		reaggregateds as (
		    select
		        staff_id,
		        jsonb_agg(elem) as aggregateds
		    from (
		        select 
		            staff_id,
		            jsonb_array_elements(aggregateds) as elem
		        from filtered
		    ) unnested
		    group by staff_id
		),
		till_todays as (
			select
				staff_id,
				case
					when sum(eccepted_amount_per_row) - sum(spent_amount_per_row) > 0
					then 'eccepted_amount_till_today'
					else 'left_amount_till_today'
				end as col_name,
				abs(sum(eccepted_amount_per_row) - sum(spent_amount_per_row)) as amount
			from filtered
			where created_date < current_date
			group by staff_id
		),
		todays as (
			select
				staff_id,
				jsonb_agg(
					jsonb_build_object (
						'today_expense_amount', eccepted_amount_per_row,
						'today_expense_name', case
							when section = 'cash'
							then 'РКО № ' || id || ' от ' || created_date 
							when section = 'bank'
							then 'ППИ № ' || id || ' от ' || created_date
						end
					) order by created_date
				) as todays_expenses
			from filtered
			where created_date >= current_date
			group by staff_id
		),
		totals as (
			select
				staff_id,
				sum(spent_amount_per_row) as total_expenses,
				sum(eccepted_amount_per_row) as total_accepted_amount,
				case
					when sum(eccepted_amount_per_row) - sum(spent_amount_per_row) > 0
					then 'left_in_staff_hand_amount'
					else 'to_staff_owed_amount'
				end as col_name,
				abs(sum(eccepted_amount_per_row) - sum(spent_amount_per_row)) as amount
			from filtered
			group by staff_id
		)
		select jsonb_build_object (
			'organization', (
				select department->>'tj'
				from commons.department
				where id = 12
			),
			'created_date', current_date,
			'operation_number', t.staff_id,
			'head', 'Рачабзода Шариф Рачаб',
			'staff_jobtitle', s.staff_jobtitle,
			'staff_department', s.staff_department,
			'total_expenses', t.total_expenses,
			'total_accepted_amount', t.total_accepted_amount,
			t.col_name, t.amount,
			'table_data', rag.aggregateds,
			tt.col_name, tt.amount,
			'todays_expenses', td.todays_expenses
		) into _result
		from totals t
		left join reaggregateds rag
			using (staff_id)
		left join till_todays tt
			using (staff_id)
		left join todays td
			using (staff_id)
		left join (
			SELECT
				m.staff_id,
				(
					select jobtitle->>'tj'
					from commons.jobtitle
					where id = m.jobtitle_id
				) as staff_jobtitle,
				(
					select department->>'tj'
					from commons.department
					where id = m.department_id
				) as staff_department
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
		) s on t.staff_id = s.staff_id;
		
    	RETURN _result;
	END;
$BODY$;




