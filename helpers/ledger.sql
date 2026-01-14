create table if not exists accounting.ledger (
	id bigserial primary key,
	debit integer not null references accounting.accounts (account), 
	credit integer not null references accounting.accounts (account),
	amount numeric not null,
	contract_id bigint references commons.counterparty_contracts (id),
	staff_id bigint references hr.staff (id),
	draft boolean not null DEFAULT false,
	created_date timestamp(0) without time zone default localtimestamp(0),
	updated_date timestamp(0) without time zone,
	
	CONSTRAINT chk_debit_credit_not_equal
        CHECK (debit <> credit)
);





select * from accounting.accounts





select accounting.upsert_ledger (
	213900,
	412520,
	100.25,
	null,
	1,
	6
);


select * from accounting.ledger;




CREATE OR REPLACE FUNCTION accounting.upsert_ledger(
	_financing accounting.budget_distribution_type,
	_debit integer,
	_credit integer,
	_amount numeric,
	_contract_id bigint default null,
	_staff_id bigint default null,
	ledger_id bigint default null
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
		draft
	) values (
		_financing,
		_debit,
		_credit,
		_amount,
		_contract_id,
		_staff_id,
		false
	) returning id into _id;
	
	return _id;
end;
$BODY$;


SELECT * FROM auth.submodule
where module_id = 1


/*
	-- ////////////////////////////////////// --
		ищем кредитоские или дебиторские задолжности
	-- ////////////////////////////////////// --
*/
select * from accounting.ledger ORDER BY id;



select accounting.look_for_debts_in_ledger (18, null);



CREATE OR REPLACE FUNCTION accounting.look_for_debts_in_ledger(
	_contract_id bigint default null,
	_staff_id bigint default null
)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	_result jsonb;
BEGIN
	-- Дебиторская задолженность	Accounts receivable   1.....
	-- Кредиторская задолженность	Accounts payable      2.....

	with debits as (
		select COALESCE(sum(amount), 0) as total_advances
		from accounting.ledger l
		where l.debit in (
			125100,
			125200,
			114610,
			211510
		)
		and draft is not true
		and (_contract_id is null or _contract_id = contract_id)
		and (_staff_id is null or _staff_id = staff_id)
	),
	credits as (
		select COALESCE(sum(amount), 0) as total_debts
		from accounting.ledger l
		where l.credit in (
			211110,
			211510,
			211580
		)
		and draft is not true
		and (_contract_id is null or _contract_id = contract_id)
		and (_staff_id is null or _staff_id = staff_id)
	)
	select jsonb_build_object (
		case 
			when debits.total_advances - credits.total_debts > 0
				then 'accounts_receivable' 
			when debits.total_advances - credits.total_debts < 0
				then 'accounts_payable'
			else 'nothing' end,
		abs(debits.total_advances - credits.total_debts)
	) into _result from debits, credits;
	
	return _result;
end;
$BODY$;











