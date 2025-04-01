{{ config (
    alias = target.database + '_blended'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
with initial_podcast_spend_data as
    (SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('gsheet_raw', 'podcast_spend') }} 
    ),

    initial_podcast_order_data as
    (SELECT *, order_date::date as date, {{ get_date_parts('date') }}
    FROM {{ source('shopify_base', 'shopify_orders') }} 
    ),
    
spend_data as 
    (select channel, date, date_granularity, 
        coalesce(sum(spend),0) as spend, coalesce(sum(paid_orders),0) as paid_orders, coalesce(sum(clicks),0) as clicks, coalesce(sum(impressions),0) as impressions,
        0 as orders, 0 as revenue, 0 as first_orders, 0 as first_orders_revenue
    from 
        ({%- for date_granularity in date_granularity_list %}
        select 'Meta' as channel, date, date_granularity, 
            coalesce(sum(spend),0) as spend, coalesce(sum(purchases),0) as paid_orders, coalesce(sum(link_clicks),0) as clicks, coalesce(sum(impressions),0) as impressions  
        from {{ source('reporting', 'facebook_ad_performance') }} 
        group by 1,2,3
        union all
        select 'Google Ads' as channel, date, date_granularity, 
            coalesce(sum(spend),0) as spend, coalesce(sum(purchases),0) as paid_orders, coalesce(sum(clicks),0) as clicks, coalesce(sum(impressions),0) as impressions 
        from {{ source('reporting', 'googleads_campaign_performance') }} 
        group by 1,2,3
        union all
        select 'TikTok' as channel, date, date_granularity, 
            coalesce(sum(spend),0) as spend, coalesce(sum(purchases),0) as paid_orders, coalesce(sum(clicks),0) as clicks, coalesce(sum(impressions),0) as impressions  
        from {{ source('reporting', 'tiktok_ad_performance') }} 
        group by 1,2,3
        union all
        select 'Bing' as channel, date, date_granularity, 
            coalesce(sum(spend),0) as spend, coalesce(sum(purchases),0) as paid_orders, coalesce(sum(clicks),0) as clicks, coalesce(sum(impressions),0) as impressions  
        from {{ source('reporting', 'bingads_campaign_performance') }} 
        group by 1,2,3
        union all
        select 'Podcast' as channel, '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
            coalesce(sum(spend),0) as spend, 0 as paid_orders, 0 as clicks, 0 as impressions
        from initial_podcast_spend_data
        group by 1,2,3
        union all
        select 'Podcast' as channel, '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
            0 as spend, count(distinct order_id) as paid_orders, 0 as clicks, 0 as impressions
        from initial_podcast_order_data
        where discount_code IN ('DIGEST','DARIN20','MAGNETIC','HEAL','GUNDRY','GENIUS','BALANCEDLES','DARIN','REALPOD','DRLYON','POW')
        group by 1,2,3
            {% if not loop.last %}UNION ALL
            {% endif %}
        {% endfor %})
    group by 1,2,3),

sho_data as 
    (select 'Shopify' as channel, date, date_granularity, 0 as spend, 0 as paid_orders, 0 as clicks, 0 as impressions,
        coalesce(sum(orders),0) as orders, coalesce(sum(subtotal_sales),0) as revenue, coalesce(sum(first_orders),0) as first_orders, coalesce(sum(first_order_subtotal_sales),0) as first_orders_revenue 
    from {{ source('reporting', 'shopify_sales') }}
    group by 1,2,3)

select
    channel, date, date_granularity,
    spend, paid_orders, clicks, impressions, orders, revenue, first_orders, first_orders_revenue
from 
    (select * from spend_data
    union all
    select * from sho_data)
    
