select seller_id, postal_prefix, postal_prefix_raw, city, state,
       postal_prefix is null as postal_prefix_unknown
from {{ ref('stg_sellers') }}
