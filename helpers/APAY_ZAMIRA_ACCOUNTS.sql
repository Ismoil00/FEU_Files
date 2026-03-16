











with m as (

select pension_number
from pension.pensioner
where pension_number in (
	
	
)
group by pension_number
having count(*) > 1
)

select array_agg(m.pension_number) from m







select pension.temp_account_insertion_api_ZAMIRA (
	'
		
		
	'
);





select 
	p.pension_number,
	-- su.user_id
	count(*) as amount
	-- su.fullname
from pension.pensioner p
left join (
	select 
		u.id as user_id, 
		concat_ws(' ', 
			s.lastname, 
			s.firstname, 
			s.middlename
		) as fullname
	from auth.user u
	join hr.staff s
		on u.staff_id = s.id
) su 
on (p.created->>'user_id')::uuid = su.user_id
where p.pension_number in (
	14787,10539,13656,10475,12960,13504,
	14112,10088,5784,13759,11372,14408,
	12951,12540,11181,14505,13999,
	13606,11219,14085,11835,10047,13226
)
and su.fullname = 'Шарифзода Замира Насимчон'
group by p.pension_number
having count(*) > 1;






select bank_account from pension.pensioner


create or replace function pension.temp_account_insertion_api_ZAMIRA (
	jdata jsonb
)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    _row jsonb;
	_count integer = 0;
	two_pension_numbers bigint[] = array[
		5784,
		10047,
		10088,
		11835,
		12540,
		13504,
		13606,
		13759,
		13999,
		14085,
		14112,
		14408,
		14787
	];
BEGIN

	FOR _row IN SELECT * FROM jsonb_array_elements(jdata) LOOP
		-- raise notice 'ROW %', _row;
		
		if exists (
			select 1 from pension.pensioner p
			left join auth.user u
				on (p.created->>'user_id')::uuid = u.id
			where p.pension_number = (_row->>'pension_number')::bigint
			and u.id = '36c13bbe-6f50-454b-af9b-acadc7520f53'::uuid
			and not (_row->>'pension_number')::bigint = any(two_pension_numbers)
			and p.bank_account is null
		) then
			_count = _count + 1;
			raise notice 'pension_number %', (_row->>'pension_number')::bigint;

			-- update pension.pensioner p set
			-- 	bank_account = (_row->>'account_number')::text,
			-- 	updated = coalesce(p.updated, p.created)
			-- where p.pension_number = (_row->>'pension_number')::bigint
			-- and not (_row->>'pension_number')::bigint = any(two_pension_numbers);

			
		end if;
		
	END LOOP;
	
		raise notice '_count %', _count;

END;
$BODY$;


/* THESE ONES HAVE 2 RECORDS ON THE TABLE */

select array_agg(pension_number) 
from pension.pensioner
where bank_account is null
and (created->>'user_id')::uuid = 
'36c13bbe-6f50-454b-af9b-acadc7520f53'::uuid



	Халимова Хурматбиби 
	Абушаев Шавкат 




select 
	id, lastname, firstname, middlename 
from hr.staff
where id = 244






