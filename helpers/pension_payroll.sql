
create table if not exists pension.pensioners_payroll (
	id bigserial primary key,
	pensioner_id bigint references pension.pensioner (id),
	retention_id bigint references pension.pensioner_retention (id),
	amount numeric not null,
	ledger_id bigint not null references accounting.ledger (id),
	paid_via pension.payroll_payment_method, 
	status pension.payroll_status not null default 'pending',
	created_at date not null default current_date,
	updated_at date
);


create type pension.payroll_status as enum ('paid', 'pending', 'rejected');

create type pension.payroll_payment_method as enum ('bank', 'cash');


ALTER TABLE pension.pensioners_payroll
ADD CONSTRAINT ux_pensioner_payroll_month
UNIQUE (pensioner_id, created_year, created_month)
WHERE retention_id IS NULL;

ALTER TABLE pension.pensioners_payroll
ADD CONSTRAINT ux_retention_payroll_month
UNIQUE (retention_id, created_year, created_month)
WHERE pensioner_id IS NULL;



-----------------------------------------------------------------------------------


select * from pension.pensioner_retention;


select * from pension.pensioner;


select * from accounting.ledger
order by id
limit 1000 offset 1000


select * from pension.pensioners_payroll




select pension.create_pensioners_payroll();


/* CALCULATE НАЧИСЛЕНИЕ */
CREATE OR REPLACE FUNCTION pension.create_pensioners_payroll()
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    current_year INT := extract(year from current_date);
    current_month INT := extract(month from current_date);
BEGIN

	/* VALIDATION */
	-- if exists (
	-- 	select 1 from pension.pensioners_payroll
	-- 	where created_year = current_year
	-- 	and created_month = current_month
	-- 	and status = 'paid' 
	-- ) then
	-- 	raise exception 'Оплата за этот месяц уже произведена, поэтому вы не можете повторно выполнить бухгалтерскую проводку.';
	-- end if;

	/* || PENSIONERS || */
	WITH main AS (
	    SELECT 
	        p.id as pensioner_id,
	        p.staff_id,
	        CASE 
	            WHEN (p.pension_obj->>'restricted_pension')::bool IS TRUE 
	            THEN ROUND(COALESCE((p.pension_obj->>'restricted_pension_amount')::numeric, 0) / 100.0, 2)
	            ELSE ROUND(COALESCE((p.pension_obj->>'pension_amount')::numeric, 0) / 100.0, 2)
	        END AS amount,
			517111 as debit,
			212590 as credit
	    FROM pension.pensioner p
	    WHERE (p.pension_end_date IS NULL OR p.pension_end_date > current_date)
		and p.pension_type_changed is not true
		-- AND p.status = 'approved'
	),
	payroll as (
		select
			id,
			pensioner_id,
			ledger_id
		from pension.pensioners_payroll
		where created_year = current_year
		and created_month = current_month
		and retention_id is null
	),
	joined as (
		select 
			m.*,
			p.id AS payroll_id,
        	p.ledger_id AS old_ledger_id
		from main m
		left join payroll p
			ON p.pensioner_id = m.pensioner_id
	),
	ledgerized AS (
	    SELECT
	        j.*,
	        accounting.upsert_ledger(
	            'budget',
	            j.debit,
	            j.credit,
	            j.amount,
	            NULL,
	            j.staff_id,
	            j.old_ledger_id
	        ) AS new_ledger_id,
			'pending' as status
	    FROM joined j
	)
	INSERT INTO pension.pensioners_payroll (
		pensioner_id,
		amount,
		ledger_id,
		created_at
	)
	SELECT
		l.pensioner_id,
		l.amount,
		l.new_ledger_id,
		now()::date
	FROM ledgerized l
	ON CONFLICT (
	    pensioner_id,
	    created_year,
	    created_month
	) DO UPDATE
	SET
		amount = EXCLUDED.amount,
		ledger_id = EXCLUDED.ledger_id,
		updated_at = now(),
		status = EXCLUDED.status::pension.payroll_status
	WHERE pensioners_payroll.retention_id IS NULL;
		

	/* || RETENTIONS || */
	WITH main AS (
	   	select
			pr.id as retention_id,
			p.staff_id,
			amount,
			517111 as debit,
			212590 as credit
		from pension.pensioner_retention pr
		join pension.pensioner p 
			on pr.pensioner_id = p.id
			and (p.pension_end_date is null or p.pension_end_date > current_date) 
			and p.pension_type_changed is not true
			-- AND p.status = 'approved'
		WHERE pr.retention_type <> 'overpay'
	),
	payroll as (
		select
			id,
			retention_id,
			ledger_id
		from pension.pensioners_payroll
		where created_year = current_year
		and created_month = current_month
		AND pensioner_id IS NULL
	),
	joined as (
		select 
			m.*,
			p.id AS payroll_id,
        	p.ledger_id AS old_ledger_id
		from main m
		left join payroll p
			ON p.retention_id = m.retention_id
	),
	ledgerized AS (
	    SELECT
	        j.*,
	        accounting.upsert_ledger(
	            'budget',
	            j.debit,
	            j.credit,
	            j.amount,
	            NULL,
	            j.staff_id,
	            j.old_ledger_id
	        ) AS new_ledger_id,
			'pending' as status
	    FROM joined j
	)
	INSERT INTO pension.pensioners_payroll (
		retention_id,
		amount,
		ledger_id,
		created_at
	)
	SELECT
		l.retention_id,
		l.amount,
		l.new_ledger_id,
		now()::date
	FROM ledgerized l
	ON CONFLICT (
	    retention_id,
	    created_year,
	    created_month
	)
	DO UPDATE
	SET
		amount = EXCLUDED.amount,
		ledger_id = EXCLUDED.ledger_id,
		updated_at = now(),
		status = EXCLUDED.status::pension.payroll_status
	WHERE pensioners_payroll.pensioner_id IS NULL;

	-- return
	return jsonb_build_object(
		'status', 200
	);
END;
$BODY$;








select * from pension.pensioners_payroll


select pension.get_pensioners_this_month_active_payroll()



/* GET PAYMENT FOR CASH OR BANK PAYMENT */
CREATE OR REPLACE FUNCTION pension.get_pensioners_this_month_active_payroll()
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    current_year INT := extract(year from current_date);
    current_month INT := extract(month from current_date);
	_result jsonb;
BEGIN

	select jsonb_build_object(
		'year', created_year,
		'month', created_month,
		'amount', sum(amount)
	) into _result
	from pension.pensioners_payroll
	where status <> 'paid'
	and created_year = current_year
	and created_month = current_month
	group by created_year, created_month;
	
	return jsonb_build_object(
		'status', 200,
		'result', _result
	);
END;
$BODY$;













/* CLOSE PENSION PAYROLL */
CREATE OR REPLACE FUNCTION pension.close_pensioners_payroll(
	_paid_via pension.payroll_payment_method
)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    current_year INT := extract(year from current_date);
    current_month INT := extract(month from current_date);
BEGIN

	update pension.pensioners_payroll set
		paid_via = _paid_via,
		status = 'paid',
		updated_at = current_date
	where created_year = current_year
	and created_month = current_month;

END;
$BODY$;

