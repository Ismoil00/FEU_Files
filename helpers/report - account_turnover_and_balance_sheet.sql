




CREATE OR REPLACE FUNCTION reports.get_account_turnover_and_balance_sheet(
	_financing accounting.budget_distribution_type,
	_date_from date,
	_date_to date,
	_account bigint,
	_limit integer DEFAULT 1000,
	_offset integer DEFAULT 0)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE	
	_result jsonb;
BEGIN
	  WITH filtered_ledger AS (
        SELECT 
            l.id,
            l.debit,
            l.credit,
            l.amount,
            l.created_date,
            cc.counterparty_id,
            cc.contract,
            cc.id as contract_id,
            c.name as counterparty_name
        FROM accounting.ledger l
        JOIN commons.counterparty_contracts cc ON cc.id = l.contract_id
        LEFT JOIN accounting.counterparty c ON cc.counterparty_id = c.id
        WHERE l.draft IS NOT TRUE
          AND (_financing IS NULL OR l.financing = _financing)
          AND (l.debit = _account OR l.credit = _account)
    ),

    expanded_movements AS (
        SELECT 
            l.id,
            l.debit AS account_id,
            l.contract_id,
            l.contract,
            l.counterparty_id,
            l.counterparty_name,
            l.created_date,
            l.amount AS debit_amount,
            0::numeric AS credit_amount
        FROM filtered_ledger l
        WHERE l.debit = _account
        
        UNION ALL
        
        SELECT 
            l.id,
            l.credit AS account_id,
            l.contract_id,
            l.contract,
            l.counterparty_id,
            l.counterparty_name,
            l.created_date,
            0::numeric AS debit_amount,
            l.amount AS credit_amount
        FROM filtered_ledger l
        WHERE l.credit = _account
    ),

    aggregated AS (
        SELECT 
            e.account_id,
            e.contract_id,
            e.contract,
            e.counterparty_id,
            e.counterparty_name,

            SUM(CASE WHEN e.created_date < _date_from THEN e.debit_amount ELSE 0 END) AS debit_before,
            SUM(CASE WHEN e.created_date < _date_from THEN e.credit_amount ELSE 0 END) AS credit_before,

            SUM(CASE WHEN e.created_date BETWEEN _date_from AND _date_to THEN e.debit_amount ELSE 0 END) AS debit_turnover,
            SUM(CASE WHEN e.created_date BETWEEN _date_from AND _date_to THEN e.credit_amount ELSE 0 END) AS credit_turnover
        FROM expanded_movements e
        GROUP BY e.account_id, e.contract_id, e.contract, e.counterparty_id, e.counterparty_name
    )

    SELECT jsonb_build_object(
        'account', _account,
        'organization', (
            SELECT department ->> 'tj'
            FROM commons.department 
            WHERE id = 12
        ),
		'period_start', to_char(_date_from, 'mm/yyyy'),
		'period_end', to_char(_date_to, 'mm/yyyy'),
        'data', COALESCE(jsonb_agg(res ORDER BY counterparty_id, contract_id), '[]'::jsonb)
    )
    INTO _result
    FROM (
        SELECT 
            a.counterparty_id,
            a.contract_id,
            a.contract AS contract_name,
            a.counterparty_name,
            -- Начальное сальдо
            abs(
                CASE 
                    WHEN LEFT(a.account_id::text, 1) = '1'
                        THEN (a.debit_before - a.credit_before)
                    ELSE (a.credit_before - a.debit_before)
                END
            ) AS opening_balance,

            a.debit_turnover,
            a.credit_turnover,

            -- Конечное сальдо (разделённое)
            CASE 
                WHEN LEFT(a.account_id::text, 1) = '1' THEN
                    abs(
                        (a.debit_before - a.credit_before 
                         + a.debit_turnover - a.credit_turnover)
                    )
                ELSE 0
            END AS closing_debit_balance,
            
            CASE 
                WHEN LEFT(a.account_id::text, 1) <> '1' THEN
                    abs(
                        (a.credit_before - a.debit_before 
                         + a.credit_turnover - a.debit_turnover)
                    )
                ELSE 0
            END AS closing_credit_balance

        FROM aggregated a
        WHERE a.debit_turnover != 0 
           OR a.credit_turnover != 0
    ) res
    LIMIT _limit
    OFFSET _offset;

    RETURN COALESCE(
        _result, 
        jsonb_build_object(
            'account', _account,
            'organization', null,
            'data', '[]'::jsonb
        )
    );
end;
$BODY$;










