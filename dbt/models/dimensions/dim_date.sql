with bounds as (
  select min(purchased_at)::date as d0, max(purchased_at)::date as d1 from {{ ref('stg_orders') }}
)
select
  d::date                              as calendar_date,
  extract(year from d)::int            as year,
  extract(month from d)::int           as month,
  date_trunc('month', d)::date         as month_start,
  extract(isoyear from d)::int         as iso_year,
  extract(week from d)::int            as iso_week,
  extract(isodow from d)::int          as iso_weekday
from bounds, generate_series(bounds.d0, bounds.d1, interval '1 day') as d
