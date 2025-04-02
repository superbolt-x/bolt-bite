{{ config (
    alias = target.database + '_blended'
)}}
    
with podcast_spend_data as
    (SELECT DATE_TRUNC('day',date::date) as date, 'day' as date_granularity,
        coalesce(sum(spend),0) as spend 
    FROM {{ source('gsheet_raw', 'podcast_data') }} 
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('week',date::date) as date, 'week' as date_granularity,
        coalesce(sum(spend),0) as spend 
    FROM {{ source('gsheet_raw', 'podcast_data') }} 
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('month',date::date) as date, 'month' as date_granularity,
        coalesce(sum(spend),0) as spend 
    FROM {{ source('gsheet_raw', 'podcast_data') }} 
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('quarter',date::date) as date, 'quarter' as date_granularity,
        coalesce(sum(spend),0) as spend 
    FROM {{ source('gsheet_raw', 'podcast_data') }} 
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('year',date::date) as date, 'year' as date_granularity,
        coalesce(sum(spend),0) as spend 
    FROM {{ source('gsheet_raw', 'podcast_data') }} 
    GROUP BY 1,2
    ),

podcast_order_data as
    (SELECT DATE_TRUNC('day',order_date::date) as date, 'day' as date_granularity,
        count(distinct order_id) as paid_orders
    FROM {{ source('shopify_base', 'shopify_orders') }} 
    WHERE discount_code IN ('DIGEST','DARIN20','MAGNETIC','HEAL','GUNDRY','GENIUS','BALANCEDLES','DARIN','REALPOD','DRLYON','POW')
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('week',order_date::date) as date, 'week' as date_granularity,
        count(distinct order_id) as paid_orders
    FROM {{ source('shopify_base', 'shopify_orders') }} 
    WHERE discount_code IN ('DIGEST','DARIN20','MAGNETIC','HEAL','GUNDRY','GENIUS','BALANCEDLES','DARIN','REALPOD','DRLYON','POW')
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('month',order_date::date) as date, 'month' as date_granularity,
        count(distinct order_id) as paid_orders
    FROM {{ source('shopify_base', 'shopify_orders') }} 
    WHERE discount_code IN ('DIGEST','DARIN20','MAGNETIC','HEAL','GUNDRY','GENIUS','BALANCEDLES','DARIN','REALPOD','DRLYON','POW')
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('quarter',order_date::date) as date, 'quarter' as date_granularity,
        count(distinct order_id) as paid_orders
    FROM {{ source('shopify_base', 'shopify_orders') }} 
    WHERE discount_code IN ('DIGEST','DARIN20','MAGNETIC','HEAL','GUNDRY','GENIUS','BALANCEDLES','DARIN','REALPOD','DRLYON','POW')
    GROUP BY 1,2
    UNION ALL
    SELECT DATE_TRUNC('year',order_date::date) as date, 'year' as date_granularity,
        count(distinct order_id) as paid_orders
    FROM {{ source('shopify_base', 'shopify_orders') }} 
    WHERE discount_code IN ('DIGEST','DARIN20','MAGNETIC','HEAL','GUNDRY','GENIUS','BALANCEDLES','DARIN','REALPOD','DRLYON','POW')
    GROUP BY 1,2
    ),
    
paid_data as 
    (select channel, date, date_granularity, 
        coalesce(sum(spend),0) as spend, coalesce(sum(paid_orders),0) as paid_orders, coalesce(sum(clicks),0) as clicks, coalesce(sum(impressions),0) as impressions,
        0 as orders, 0 as revenue, 0 as first_orders, 0 as first_orders_revenue
    from 
        (
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
        select 'Podcast' as channel, date, date_granularity, 
            coalesce(sum(spend),0) as spend, 0 as paid_orders, 0 as clicks, 0 as impressions
        from podcast_spend_data
        group by 1,2,3
        union all
        select 'Podcast' as channel, date, date_granularity, 
            0 as spend, coalesce(sum(paid_orders),0) as paid_orders, 0 as clicks, 0 as impressions
        from podcast_order_data
        group by 1,2,3)
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
    (select * from paid_data
    union all
    select * from sho_data)
where date <= current_date
