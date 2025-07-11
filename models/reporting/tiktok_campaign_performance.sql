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
        complete_payment as purchases,
        total_complete_payment_rate as revenue,
        web_event_add_to_cart as atc
        FROM {{ ref('tiktok_performance_by_campaign') }}
)

{% for granularity in granularities %}
, {{ granularity }}_agg_gmv as (
    select 
        campaign_name,
        campaign_id,
        null as campaign_status,
        null as campaign_type_default,
        date_trunc('{{ granularity }}', date) as date,
        '{{ granularity }}' as date_granularity,
        sum(spend) as spend,
        sum(0) as impressions,
        sum(0) as clicks,
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
