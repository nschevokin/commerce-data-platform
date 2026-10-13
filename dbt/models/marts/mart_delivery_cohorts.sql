select
  date_trunc('month', o.purchase_date)::date                          as purchase_month,
  coalesce(c.state, '(unknown)')                                      as customer_state,
  count(*)                                                            as delivered_orders,
  count(*) filter (where o.is_on_time is not null)                    as delivered_with_both_dates,
  count(*) filter (where o.is_on_time)                                as on_time_orders,
  count(*) filter (where o.is_on_time is null)                        as missing_date_orders,
  round(count(*) filter (where o.is_on_time)::numeric
        / nullif(count(*) filter (where o.is_on_time is not null), 0), 6) as on_time_rate,
  count(*) filter (where o.delivery_lead_time_seconds < 0)            as negative_lead_time_orders,
  percentile_cont(0.5) within group (order by o.delivery_lead_time_seconds)
    filter (where o.delivery_lead_time_seconds >= 0)                  as lead_time_p50_seconds,
  percentile_cont(0.9) within group (order by o.delivery_lead_time_seconds)
    filter (where o.delivery_lead_time_seconds >= 0)                  as lead_time_p90_seconds
from {{ ref('fct_orders') }} o
left join {{ ref('dim_customers') }} c using (customer_id)
where o.is_eligible and o.order_status = 'delivered'
group by 1, 2
