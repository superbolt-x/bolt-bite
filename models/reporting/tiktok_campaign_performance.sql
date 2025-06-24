{{ config (
    alias = target.database + '_tiktok_campaign_performance'
)}}

{%- set granularities = ['day', 'week', 'month', 'quarter', 'year'] -%}

with tiktok_data as (
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
        complete_payment_rate as revenue,
        web_add_to_cart_events as atc
        FROM {{ ref('tiktok_performance_by_campaign') }}
)

{% for granularity in granularities %}
, {{ granularity }}_agg_gmv as (
    select 
        campaign_name,
        campaign_id,
        '(not set)' as campaign_status,
        '(not set)' as campaign_type_default,
        date_trunc('{{ granularity }}', date) as date,
        '{{ granularity }}' as date_granularity,
        sum(spend) as spend,
        sum(0) as impressions,
        sum(0) as clicks,
        sum(0) as video_views,
        sum(purchases) as purchases,
        sum(revenue) as revenue,
        sum(0) as atc
    from {{ source('gsheet_raw','tiktok_gmv_insights') }}
    group by 
        campaign_name,
        campaign_id,
        date_trunc('{{ granularity }}', date)
)
{% endfor %}

select * from day_agg_gmv
{% for granularity in granularities[1:] %}
union all
select * from {{ granularity }}_agg_gmv
{% endfor %}
union all
select * from tiktok_data
