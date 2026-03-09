

select * from accounting.ledger;



CREATE OR REPLACE FUNCTION accounting.upsert_ledger (
	_financing accounting.budget_distribution_type,
	_debit integer,
	_credit integer,
	_amount numeric,
	_main_department_id integer,
	_contract_id bigint DEFAULT NULL::bigint,
	_staff_id bigint DEFAULT NULL::bigint,
	ledger_id bigint DEFAULT NULL::bigint,
	_created_date date default null::date
)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_id bigint = ledger_id;
BEGIN

	-- debit must not = credit
	-- if _debit = _credit then
	-- 	RAISE EXCEPTION 'Дебет и Кредит не могут быть одним и тем же бухгалтерским счетом.' USING ERRCODE = 'P0001';
	-- end if;

	-- we track history
	if _id is not null then
		update accounting.ledger set
			draft = true,
			updated_date = localtimestamp(0)
		where id = _id;
	end if;

	-- insert
	insert into accounting.ledger (
		financing,
		debit,
		credit,
		amount,
		contract_id,
		staff_id,
		main_department_id,
		draft,
		created_date
	) values (
		_financing,
		_debit,
		_credit,
		_amount,
		_contract_id,
		_staff_id,
		_main_department_id,
		false,
		COALESCE(_created_date, LOCALTIMESTAMP(0))
	) returning id into _id;
	
	return _id;
end;
$BODY$;












CREATE OR REPLACE FUNCTION accounting.upsert_ledger_for_payroll (
	_payroll_sheet_line_id bigint,
	_financing accounting.budget_distribution_type,
	_debit integer,
	_credit integer,
	_amount numeric,
	_staff_id bigint,
	_main_department_id integer,
	_created_date date default null::date
)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

	-- we track history
	-- if exists(
	-- 	select 1 from accounting.ledger
	-- 	where payroll_sheet_line_id = _payroll_sheet_line_id
	-- 	and staff_id = _staff_id
	-- 	and debit = _debit
	-- 	and credit = _credit
	-- ) is not null then
	-- 	update accounting.ledger set
	-- 		draft = true,
	-- 		updated_date = localtimestamp(0)
	-- 	where payroll_sheet_line_id = _payroll_sheet_line_id
	-- 	and staff_id = _staff_id
	-- 	and debit = _debit
	-- 	and credit = _credit;
	-- end if;

	-- insert
	insert into accounting.ledger (
		financing,
		debit,
		credit,
		amount,
		staff_id,
		draft,
		payroll_sheet_line_id,
		main_department_id,
		created_date
	) values (
		_financing,
		_debit,
		_credit,
		_amount,
		_staff_id,
		false,
		_payroll_sheet_line_id,
		_main_department_id,
		COALESCE(_created_date, LOCALTIMESTAMP(0))
	); 
end;
$BODY$;








CREATE OR REPLACE FUNCTION accounting.upsert_ledger_for_payroll_sheet(
	_payroll_sheet_line_id bigint,
	_financing accounting.budget_distribution_type,
	_debit integer,
	_credit integer,
	_amount numeric,
	_staff_id bigint,
	_main_department_id integer,
	_created_date date default null::date
)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

	-- we track history
	if exists(
		select 1 from accounting.ledger
		where payroll_sheet_line_id = _payroll_sheet_line_id
		and staff_id = _staff_id
		and debit = _debit
		and credit = _credit
	) is not null then
		update accounting.ledger set
			draft = true,
			updated_date = localtimestamp(0)
		where payroll_sheet_line_id = _payroll_sheet_line_id
		and staff_id = _staff_id
		and debit = _debit
		and credit = _credit;
	end if;

	-- insert
	insert into accounting.ledger (
		financing,
		debit,
		credit,
		amount,
		staff_id,
		draft,
		payroll_sheet_line_id,
		main_department_id,
		created_date
	) values (
		_financing,
		_debit,
		_credit,
		_amount,
		_staff_id,
		false,
		_payroll_sheet_line_id,
		_main_department_id,
		COALESCE(_created_date, LOCALTIMESTAMP(0))
	); 
end;
$BODY$;









