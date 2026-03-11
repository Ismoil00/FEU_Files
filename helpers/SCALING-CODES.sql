-- "generate_ledger_for_payroll_sheet"

-- "handle_payroll_sheet_from_bank_and_cash"

-- "process_retention"

-- "create_pensioners_payroll"

----------------------------------------------------------

/*
	FRONT:
		- SearchPage main-department-id
		- CreatePage main-department-id
	BACKEND:
		- UPSERT
		- GET
		- Ledger
		- FastAPI
*/





select parent_id from commons.department


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
);



/* we fill ledger with the accountingentry */
SELECT accounting.upsert_ledger(
	_financing,
	_debit,
	_credit,
	round(_amount * _quantity, 2),
	_main_department_id,
	_contract_id,
	null,
	_ledger_id,
	_created_date
) INTO _ledger_id;



select * from accounting.ledger
where id > 2332
order by id



select * from accounting.product_transfer
-- where id > 716
order by id







