{{ config (
    alias = target.database + '_tiktok_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
date,
date_granularity,
cost as spend,
impressions,
clicks,
conversions as purchases,
complete_payment_value as revenue,
web_add_to_cart_events as atc
FROM {{ ref('tiktok_performance_by_campaign') }}
