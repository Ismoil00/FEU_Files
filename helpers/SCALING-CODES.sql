-- "generate_ledger_for_payroll_sheet"

-- "goods_return_table_data_validation"

-- "handle_payroll_sheet_from_bank_and_cash"

-- "inventory_entry_table_data_management"

-- "process_retention"

-- "upsert_advance_report_oplata"

-- "upsert_advance_report_prochee"

-- "upsert_advance_report_tmzos"

-- "upsert_cash_payment_order"

-- "upsert_cash_receipt_order"

-- "upsert_payment_order_incoming"

-- "upsert_payment_order_outgoing"

-- "upsert_product_transfer"

-- "upsert_warehouse_incoming"

-- "upsert_warehouse_outgoing"

-- "upsert_warehouse_services"

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



select * from accounting.ledger
where id > 2299
order by id


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



select * from accounting.payment_order_incoming order by id

select * from accounting.payment_order_outgoing order by id

select * from accounting.cash_payment_order order by id

select * from accounting.cash_receipt_order order by id

select * from accounting.advance_report_prochee order by id





















