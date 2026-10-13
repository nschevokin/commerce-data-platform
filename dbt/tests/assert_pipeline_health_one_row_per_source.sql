{{ config(enabled=var('source_mode') == 'bronze') }}

select source_name, count(*) as n
from {{ ref('mart_pipeline_health') }}
group by source_name
having count(*) <> 1
union all
select 'release_id missing', count(*) from {{ ref('mart_pipeline_health') }} where release_id is null
having count(*) > 0
union all
select 'source count <> 7', count(*) from {{ ref('mart_pipeline_health') }}
having count(*) <> 7
