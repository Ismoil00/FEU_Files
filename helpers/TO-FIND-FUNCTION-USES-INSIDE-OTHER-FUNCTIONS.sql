





with search_name as (
	select 'accounting.upsert_ledger'::text as name  /* <-- PASTE FUNCTION NAME HERE */
),
parts as (
	select
		trim(name) as name,
		case when position('.' in trim(name)) > 0 then split_part(trim(name), '.', 2) else trim(name) end as bare_name,
		case when position('.' in trim(name)) > 0 then split_part(trim(name), '.', 1) else null end as schema_part
	from search_name
	where length(trim(name)) > 0
),
from_routines as (
	select
		n.nspname as object_schema,
		p.proname as object_name,
		case
			when p.prokind = 'f' and exists (select 1 from pg_trigger t where t.tgfoid = p.oid) then 'trigger function'
			when p.prokind = 'f' then 'function'
			when p.prokind = 'p' then 'procedure'
			when p.prokind = 'a' then 'aggregate'
			when p.prokind = 'w' then 'window function'
			else 'function'
		end as object_type,
		'pg_proc (body)' as reference_location
	from pg_proc p
	join pg_namespace n on n.oid = p.pronamespace
	cross join parts
	where p.prosrc is not null
	and (
		p.prosrc like '%' || parts.bare_name || '(%'
		or (parts.schema_part is not null and p.prosrc like '%' || parts.name || '(%')
		or p.prosrc like '%' || parts.bare_name || '%'
	)
),
from_views as (
	select
		schemaname as object_schema,
		viewname as object_name,
		'view' as object_type,
		'view definition' as reference_location
	from pg_views
	cross join parts
	where definition is not null
	and (
		definition like '%' || parts.bare_name || '(%'
		or (parts.schema_part is not null and definition like '%' || parts.name || '(%')
		or definition like '%' || parts.bare_name || '%'
	)
),
from_matviews as (
	select
		schemaname as object_schema,
		matviewname as object_name,
		'materialized view' as object_type,
		'matview definition' as reference_location
	from pg_matviews
	cross join parts
	where definition is not null
	and (
		definition like '%' || parts.bare_name || '(%'
		or (parts.schema_part is not null and definition like '%' || parts.name || '(%')
		or definition like '%' || parts.bare_name || '%'
	)
),
from_rules as (
	select
		schemaname as object_schema,
		tablename || ' (rule: ' || rulename || ')' as object_name,
		'rule' as object_type,
		'rule definition' as reference_location
	from pg_rules
	cross join parts
	where definition is not null
	and (
		definition like '%' || parts.bare_name || '(%'
		or (parts.schema_part is not null and definition like '%' || parts.name || '(%')
		or definition like '%' || parts.bare_name || '%'
	)
),
all_refs as (
	select * from from_routines
	union all
	select * from from_views
	union all
	select * from from_matviews
	union all
	select * from from_rules
)
select
	object_schema,
	object_name,
	object_type,
	reference_location
from all_refs
order by object_type, object_schema, object_name;









