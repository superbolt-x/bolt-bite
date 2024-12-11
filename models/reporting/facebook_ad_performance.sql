{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* 'Adv' THEN 'Campaign Type: Adv+' ELSE campaign_type_default END as campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
CASE WHEN visual ~* 'Launch Creatives (Cost Non)' THEN 'Launch Creatives (Cost Non-Trial)' ELSE visual END as visual,
copy,
CASE WHEN format_visual ~* 'Video - Launch Creatives (Cost Non)' THEN 'Video - Launch Creatives (Cost Non-Trial)' ELSE format_visual END as format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue
FROM {{ ref('facebook_performance_by_ad') }}
