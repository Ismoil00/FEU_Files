

select * from accounting.payment_order_outgoing;


select * from accounting.ledger





select * from payments.payroll_sheet_line;


CREATE OR REPLACE FUNCTION accounting.upsert_payment_order_outgoing(jdata json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	DECLARE
		_user_id text = jdata->>'user_id';
		_created_date date = (jdata->>'created_date')::date;
		_id bigint = (jdata->>'id')::bigint;
		_financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
		_ledger_id bigint = (jdata->>'ledger_id')::bigint;
		_debit integer = (jdata->>'debit')::integer;
		_amount numeric = (jdata->>'amount')::numeric;
		_contract_id bigint = (jdata->>'contract_id')::bigint;
		_staff_id bigint = (jdata->>'staff_id')::bigint;

		-- salary payroll-sheet
    	_payroll_sheet_id bigint = NULLIF(jdata->>'payroll_sheet_id','')::bigint;
	BEGIN

		/* we fill ledger with the accountingentry */
		SELECT accounting.upsert_ledger(
			_financing,
			_debit,
			111254,
			_amount,
			_contract_id,
			_staff_id,
			_ledger_id
		) INTO _ledger_id;

		-- insertion
		if _id is null then
			insert into accounting.payment_order_outgoing (
				financing,
				bank_account_id,
				cash_flow_article_id,
				amount,
				debit,
				description,
				payment_date,

				given_to,
				department_id,
				staff_id,
				staff_id_document,
				counterparty_id,
				contract_id,
				counterparty_bank_id,
				contract_text,
				ledger_id,
				
				created
			) values (
				_financing,
				(jdata->>'bank_account_id')::bigint,
				(jdata->>'cash_flow_article_id')::bigint,
				_amount,
				_debit,
				jdata->>'description',
				coalesce((jdata->>'payment_date')::date, current_date),

				jdata->>'given_to',
				(jdata->>'department_id')::bigint,
				_staff_id,
				(jdata->>'staff_id_document')::text,
				(jdata->>'counterparty_id')::bigint,
				_contract_id,
				(jdata->>'counterparty_bank_id')::bigint,
				jdata->>'contract_text',
				_ledger_id,
				
				jsonb_build_object(
					'user_id', _user_id,
					'date', coalesce(_created_date, LOCALTIMESTAMP(0))
				)
			) returning id into _id;

		-- update
		else
			update accounting.payment_order_outgoing poo SET
				financing = _financing,
				bank_account_id = (jdata->>'bank_account_id')::bigint,
				cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
				amount = _amount,
				debit = _debit,
				description = jdata->>'description',
				payment_date = coalesce((jdata->>'payment_date')::date, poo.payment_date),

				given_to = jdata->>'given_to',
				department_id = (jdata->>'department_id')::bigint,
				staff_id = _staff_id,
				staff_id_document = jdata->>'staff_id_document',
				counterparty_id = (jdata->>'counterparty_id')::bigint,
				counterparty_bank_id = (jdata->>'counterparty_bank_id')::bigint,
				contract_id = _contract_id,
				contract_text = jdata->>'contract_text',
				ledger_id = _ledger_id,
				
				created = CASE
    			    WHEN _created_date IS NOT NULL
    			    THEN jsonb_set(
    			             poo.created,
    			             '{date}',
    			             to_jsonb(_created_date)
    			         )
    			    ELSE poo.created
    			END,
				updated = jsonb_build_object(
					'user_id', _user_id,
					'date', LOCALTIMESTAMP(0)
				)
			where id = _id;
		end if;


		if given_to = 'payroll' and payroll_sheet_id is not null then
			perform accounting.handle_payroll_sheet_from_bank_and_cash (
				_payroll_sheet_id,
				_id,
				_financing,
				false
			);
		end if;

		return json_build_object(
			'msg', case when _id is null then 'created' else 'updated' end,
			'status', 200,
			'id', _id
		);
	end;
$BODY$;

------------------------------------------------------------------
------------------------------------------------------------------
------------------------------------------------------------------


SELECT * FROM payments.payroll_sheet ps



select * from payments.payroll_sheet_line



select * from accounting.cash_payment_order;



select * from accounting.ledger;



select * from payments.payroll_sheet_line;



CREATE OR REPLACE FUNCTION accounting.upsert_cash_payment_order(
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
    _financing accounting.budget_distribution_type = (jdata->>'financing')::accounting.budget_distribution_type;
    _left_in_cash numeric = 0;
    _debit integer = (jdata->>'debit')::integer;
    _amount numeric = (jdata->>'amount')::numeric;
    _contract_id bigint = (jdata->>'contract_id')::bigint;
    _staff_id bigint = (jdata->>'staff_id')::bigint;
    _ledger_id bigint = (jdata->>'ledger_id')::bigint;

    -- salary payroll-sheet
    _payroll_sheet_id bigint = NULLIF(jdata->>'payroll_sheet_id','')::bigint;
BEGIN

    /* Cash Amount Check VALIDATION */
    _left_in_cash = COALESCE((
        SELECT SUM(amount)
        FROM accounting.cash_receipt_order
        WHERE financing = _financing
    ), 0) - COALESCE((
        SELECT SUM(amount)
        FROM accounting.cash_payment_order
        WHERE financing = _financing
    ), 0);

    IF _left_in_cash < _amount THEN
        RAISE EXCEPTION 'Запрошенная сумма "%" превышает остаток на кассе "%"', _amount, _left_in_cash;
    END IF;

    /* we fill ledger with the accountingentry */
    SELECT accounting.upsert_ledger(
        _financing,
        _debit,
        111110,
        _amount,
        _contract_id,
        _staff_id,
        _ledger_id
    ) INTO _ledger_id;

    -- insertion
    IF _id IS NULL THEN
        INSERT INTO accounting.cash_payment_order (
            financing,
            cash_flow_article_id,
            amount,
            debit,
            description,
            based_on,

            given_to,
            department_id,
            staff_id,
            staff_id_document,
            counterparty_id,
            contract_id,
            contract_text,
            ledger_id,

            created
        ) VALUES (
            _financing,
            (jdata->>'cash_flow_article_id')::bigint,
            _amount,
            _debit,
            jdata->>'description',
            jdata->>'based_on',

            jdata->>'given_to',
            (jdata->>'department_id')::bigint,
            _staff_id,
            (jdata->>'staff_id_document')::text,
            (jdata->>'counterparty_id')::bigint,
            _contract_id,
            jdata->>'contract_text',
            _ledger_id,

            jsonb_build_object(
                'user_id', _user_id,
                'date', COALESCE(_created_date, LOCALTIMESTAMP(0))
            )
        ) RETURNING id INTO _id;

    -- update
    ELSE
        UPDATE accounting.cash_payment_order cpo SET
            financing = _financing,
            cash_flow_article_id = (jdata->>'cash_flow_article_id')::bigint,
            amount = _amount,
            debit = _debit,
            description = jdata->>'description',
            based_on = jdata->>'based_on',

            given_to = jdata->>'given_to',
            department_id = (jdata->>'department_id')::bigint,
            staff_id = _staff_id,
            staff_id_document = jdata->>'staff_id_document',
            counterparty_id = (jdata->>'counterparty_id')::bigint,
            contract_id = _contract_id,
            contract_text = jdata->>'contract_text',
            ledger_id = _ledger_id,

            created = CASE
                WHEN _created_date IS NOT NULL
                THEN jsonb_set(
                         cpo.created,
                         '{date}',
                         to_jsonb(_created_date)
                     )
                ELSE cpo.created
            END,
            updated = jsonb_build_object(
                'user_id', _user_id,
                'date', LOCALTIMESTAMP(0)
            )
        WHERE id = _id;
    END IF;

	if given_to = 'payroll' and payroll_sheet_id is not null then
		perform accounting.handle_payroll_sheet_from_bank_and_cash (
			_payroll_sheet_id,
			_id,
			_financing,
			true
		);
	end if;

    RETURN json_build_object(
        'msg', CASE WHEN _id IS NULL THEN 'created' ELSE 'updated' END,
        'status', 200,
        'id', _id
    );
END;
$BODY$;



------------------------------------------------------------------
------------------------------------------------------------------
------------------------------------------------------------------



select * from payments.payroll_sheet_line;


select * from accounting.ledger


select * from payments.payroll_sheet_line;


select * from accounting.ledger;


select * from commons.retention


CREATE OR REPLACE FUNCTION accounting.handle_payroll_sheet_from_bank_and_cash (
	_payroll_sheet_id bigint,
	_id bigint,
	_financing accounting.budget_distribution_type,
	_cash boolean
)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   
BEGIN

	/*
		The debits and credits are statically fixed
		So, only developers can change it.
	*/

	-- we upsert the ledgers
	with main as (
		select * 
		from (
			select
			    psl.id,
			    psl.staff_id,
			    case 
					when _cash is true then psl.paid_in_cash
					else psl.paid_in_bank
				end as amount,
				211510 as debit,
				case 
					when _cash is true then 111110
					else 111254
				end as credit
			from payments.payroll_sheet_line psl
			join payments.payroll_sheet ps
			    on psl.sheet_id = ps.id
			where ps.id = _payroll_sheet_id
		
			-- union all
		
			-- select
			--     psl.id,
			--     psl.staff_id,
			--     coalesce((rm.elem ->> 'summ')::numeric, 0) as amount,
			-- 	211510 as debit,
			-- 	case 
			-- 		when _cash is true then 211590
			-- 		else 211590
			-- 	end as credit
			-- from payments.payroll_sheet_line psl
			-- join payments.payroll_sheet ps
			--     on psl.sheet_id = ps.id
			-- left join lateral jsonb_array_elements(
			-- 	coalesce(
		 --        	(psl.calc_snapshot ->> 'retention_meta')::jsonb,
		 --        	'[]'::jsonb
		 --    	)
			-- )
			-- with ordinality as rm(elem, ordinality) on true
			-- where ps.id = _payroll_sheet_id
		) t 
		where amount > 0
	)
	select accounting.upsert_ledger_for_payroll_sheet(
		m.id,
		_financing,
		m.debit,
		m.credit,
		m.amount,
		m.staff_id
	)
	from main m;

	-- we update the sheet
 	update payments.payroll_sheet ps set
		status = 1,
		bank_or_cash_id = _id
	where id = _payroll_sheet_id;
END;
$BODY$;




------------------------------------------------------------------
------------------------------------------------------------------
------------------------------------------------------------------


select * from payments.payroll_sheet




drop function accounting.upsert_ledger_for_payroll_sheet





CREATE OR REPLACE FUNCTION accounting.upsert_ledger_for_payroll_sheet(
	_payroll_sheet_line_id bigint,
	_financing accounting.budget_distribution_type,
	_debit integer,
	_credit integer,
	_amount numeric,
	_staff_id bigint
)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

	-- we track history
	if (
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
		payroll_sheet_line_id
	) values (
		_financing,
		_debit,
		_credit,
		_amount,
		_staff_id,
		false,
		payroll_sheet_line_id
	); 
end;
$BODY$;





