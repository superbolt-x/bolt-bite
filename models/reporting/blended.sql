{{ config (
    alias = target.database + '_blended'
)}}

with spend_data as (select date, date_granularity, sum(spend) as spend from 
(select date, date_granularity, sum(spend) as spend from reporting.bite_facebook_ad_performance group by 1,2
union all
select date, date_granularity, sum(spend) as spend from reporting.bite_googleads_campaign_performance group by 1,2
union all
select date, date_granularity, sum(spend) as spend from reporting.bite_tiktok_ad_performance group by 1,2)
group by 1,2)

, sho_data as (select date, date_granularity, orders, subtotal_sales as revenue from reporting.bite_shopify_sales)

select
date, date_granularity,
coalesce(spend,0) as spend, orders, revenue
from sho_data left join spend_data using(date, date_granularity)
