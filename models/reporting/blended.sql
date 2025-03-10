{{ config (
    alias = target.database + '_blended'
)}}

with spend_data as (select channel, date, date_granularity, sum(spend) as spend from 
(select 'Meta' as channel, date, date_granularity, sum(spend) as spend from reporting.bite_facebook_ad_performance group by 1,2,3
union all
select 'Google Ads' as channel, date, date_granularity, sum(spend) as spend from reporting.bite_googleads_campaign_performance group by 1,2,3
union all
select 'TikTok' as channel, date, date_granularity, sum(spend) as spend from reporting.bite_tiktok_ad_performance group by 1,2,3)
group by 1,2,3)

, sho_data as (select 'Shopify' as channel, date, date_granularity, orders, subtotal_sales as revenue, first_orders, first_order_subtotal_sales as first_orders_revenue 
    from reporting.bite_shopify_sales)

select
channel, date, date_granularity,
coalesce(spend,0) as spend, orders, revenue, first_orders, first_orders_revenue
from sho_data left join spend_data using(date, date_granularity, channel)
