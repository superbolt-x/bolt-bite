{{ config (
    alias = target.database + '_blended'
)}}

with spend_data as 
    (select channel, date, date_granularity, coalesce(sum(spend),0) as spend 
    from 
        (select 'Meta' as channel, date, date_granularity, coalesce(sum(spend),0) as spend from {{ source('reporting', 'facebook_ad_performance') }} group by 1,2,3
        union all
        select 'Google Ads' as channel, date, date_granularity, coalesce(sum(spend),0) as spend from {{ source('reporting', 'googleads_campaign_performance') }} group by 1,2,3
        union all
        select 'TikTok' as channel, date, date_granularity, coalesce(sum(spend),0) as spend from {{ source('reporting', 'tiktok_ad_performance') }} group by 1,2,3)
        group by 1,2,3),

sho_data as 
    (select 'Shopify' as channel, date, date_granularity, 
        coalesce(sum(orders),0) as orders, coalesce(sum(subtotal_sales),0) as revenue, coalesce(sum(first_orders),0) as first_orders, coalesce(sum(first_order_subtotal_sales),0) as first_orders_revenue 
    from {{ source('reporting', 'bite_shopify_sales') }}
    group by 1,2,3)

select
    channel, date, date_granularity,
    spend, orders, revenue, first_orders, first_orders_revenue
from sho_data 
left join spend_data using(date, date_granularity, channel)
