copy (
select
    pg_relation_size(indclass.relname::text) as ix_bytes
    , pg_size_pretty(pg_relation_size(indclass.relname::text)) as ix_size
    , floor(100 * stanullfrac) as "null %"
    , typname as "type"
    , floor(log(tab.reltuples)) as "log(rows)"
    , indclass.relname as ix
    , pg_get_indexdef(pg_index.indexrelid,  0,  true) as "sql"
    , nspname as "schema"
    , tab.relname as "table"
    , attname as "column"

    , floor(100 * (pg_stat_get_numscans(pg_index.indexrelid))
                / (pg_stat_get_numscans(pg_index.indexrelid)
                   + pg_stat_get_numscans(tab.oid)
                   + 1)) -- avoid division by zero
        as "ix scan %"

    , floor(100 * (pg_stat_get_tuples_inserted(tab.oid)
                   + pg_stat_get_tuples_updated(tab.oid)
                   + pg_stat_get_tuples_deleted(tab.oid))
                
                / (pg_stat_get_tuples_inserted(tab.oid)
                   + pg_stat_get_tuples_updated(tab.oid)
                   + pg_stat_get_tuples_deleted(tab.oid)

                   + pg_stat_get_tuples_returned(tab.oid)
                   + 1)) -- avoid division by zero
        as "write queries %"

from
    pg_statistic
        join pg_class tab on starelid = tab.oid
            join pg_namespace ON tab.relnamespace = pg_namespace.oid
        join pg_attribute on pg_statistic.staattnum = pg_attribute.attnum
                         and pg_statistic.starelid = pg_attribute.attrelid
            join pg_type on atttypid = pg_type.oid
            join pg_index on pg_attribute.attnum = any(pg_index.indkey)
                         and pg_attribute.attrelid = pg_index.indrelid
                join pg_class indclass on pg_index.indexrelid = indclass.oid
where
    nspname not in ('archive')
    and not tab.relname ~ '^part_config|_old$|\d{2}_\d{2}_\d{4}$'
    and tab.relkind in ('r', 'm') -- table or materialized view
    and stanullfrac > .66
    -- and (
    --     tab.reltuples > 100 * 1000 * 1000 -- 100 million rows
    -- )
order by
    1 desc
)
to stdout with csv header;
