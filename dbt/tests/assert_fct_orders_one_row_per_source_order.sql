select 'count_mismatch' as problem
where (select count(*) from {{ ref('fct_orders') }}) <> (select count(*) from {{ ref('stg_orders') }})
