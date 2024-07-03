with tablist as (
    select
        pg_namespace.nspname || '.' || pg_class.relname as full_table_name,
        pg_class.reltuples as rows_approx
    from pg_class
        join pg_namespace on pg_class.relnamespace = pg_namespace.oid
    where
        pg_class.relkind = 'r'
        and pg_namespace.nspname not in ('pg_catalog')
)
select
    full_table_name,
    rows_approx,
    pg_size_pretty(pg_relation_size(full_table_name)) as heap,
    pg_size_pretty(pg_table_size(full_table_name)) as heap_plus_indexes,
    pg_size_pretty(pg_total_relation_size(full_table_name)) as heap_plus_indexes_plus_toast
from tablist
order by rows_approx desc;
