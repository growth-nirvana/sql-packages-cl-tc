  /* Your SQL content here */
/* 
    Change these variables so that the <CONNECTOR SERVICE> portion matches the connector service
    that you are working with. For example if you are creating a model for google_ads_grant then replace the text
    and <> signs with google_ads_grant
    
    Change these variables so that the <MODEL NAME> portion matches the model name that you are working with
    If you are creating a model called ad_report__google, you will change the <MODEL NAME> with ad_report__google

    Updated this comment to test that the UI would show it.
*/

{% assign dimensions = vars.google_ads_search.models.google_commercial_search__device_report.dimensions %}
{% assign metrics = vars.google_ads_search.models.google_commercial_search__device_report.metrics %}
{% assign account_id = vars.google_ads_search.account_ids %}
{% assign active = vars.google_ads_search.active %}
{% assign table_active = vars.google_ads_search.models.google_commercial_search__device_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = 'google_commercial_search__device_report' %}
{% assign source_dataset_id = vars.google_ads_search.source_dataset_id %}
{% assign conversions = vars.google_ads_search.conversions %}
{% assign number_of_accounts = vars.google_ads_search.account_ids | size %}
{% assign micro_denominator = 1000000 %}

CREATE OR REPLACE TABLE 
    `{{dataset_id}}`.`{{table_id}}` (
    {% for dimension in dimensions %}
        {% unless forloop.first %}
            , 
        {% endunless %}
        `{{dimension.name}}` {{dimension.type}} OPTIONS (description = '[db_field_name = {{dimension.name}}]') 
    {% endfor %}
    {% for metric in metrics %}
        , `{{metric.name}}` {{metric.type}}  OPTIONS (description = '[db_field_name = {{metric.name}}]') 
    {% endfor %}
    )
{% if active and table_active %}
    AS(
        with
        /* 
            Add as many CTEs here as needed to keep the code readable. 
        */
        sync_info as (
            select
              max(datetime(_time_extracted, "{{ vars.timezone }}")) as max_synced_at
              , max(date) as max_data_date
            from {{source_dataset_id}}.demo_device_report
            {% if number_of_accounts > 0 %}
                where customer_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}, {% endunless %}{{id}}
                    {% endfor %}
                )
            {% endif %}
        )

        , campaigns as(
            select
                *
            from
                {{source_dataset_id}}.campaign_history
            qualify rank() over(partition by id order by updated_at desc) = 1
            and row_number() over(partition by id, updated_at) = 1
        )
        
        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            qualify rank() over(partition by id order by updated_at desc) = 1
            and row_number() over(partition by id, updated_at) = 1      
        )
        
        , base_report as(
            select
                date
                , campaign_id
                , device
                , sum(impressions) as impressions
                , sum(safe_divide(cost_micros, {{ micro_denominator }})) as cost
                , sum(clicks) as clicks
                , sum(conversions) as conversions   
                , sum(conversions_value) as conversions_value
                , sum(all_conversions) as all_conversions
                , sum(all_conversions_value) as all_conversions_value
                , sum(view_through_conversions) as view_through_conversions
            from
                {{source_dataset_id}}.demo_device_report
            {% if number_of_accounts > 0 %}
                where customer_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}, {% endunless %}{{id}}
                    {% endfor %}
                )
            {% endif %}
            group by
                1,2,3
            order by
                date asc
        )
        
        , custom_conversions as(
            select
                date
                , campaign_id
                , device
                , conversion_action_name
                , sum(all_conversions) as all_conversions
                , sum(all_conversions_value) as all_conversions_value
                , sum(conversions) as conversions
                , sum(conversions_value) as conversions_value
                , sum(view_through_conversions) as view_through_conversions
            from
                {{source_dataset_id}}.demo_device_report_custom_conversions
            {% if number_of_accounts > 0 %}
                where customer_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}, {% endunless %}{{id}}
                    {% endfor %}
                )
            {% endif %}
            group by
                1,2,3,4
        )
        
        , pivots as(
            select
                *
            from
                custom_conversions
            pivot(
                sum(all_conversions) as all_conversions
                , sum(all_conversions_value) as all_conversions_value
                , sum(conversions) as conversions
                , sum(conversions_value) as conversions_value
                for conversion_action_name in (
                    {% for conversion in conversions %}
                        {% unless forloop.first %}
                            ,
                        {% endunless %}
                        "{{conversion.event_name}}" {{ conversion.output_name }}
                    {% endfor %}
                )
            )
        )
        
        , api as(
            select
                base_report.date
                , accounts.id as account_id
                , accounts.descriptive_name as account_name
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                , base_report.campaign_id
                , campaigns.name as campaign_name
                , case
                    when base_report.device = 'MOBILE' then 'Mobile'
                    when base_report.device = 'DESKTOP' then 'Desktop'
                    when base_report.device = 'TABLET' then 'Tablet'
                    when base_report.device = 'CONNECTED_TV' then 'Connected TV'
                    when base_report.device = 'OTHER' then 'Other'
                    else INITCAP(base_report.device)
                end as device
                , base_report.impressions
                , base_report.cost
                , base_report.clicks
                , base_report.conversions
                , base_report.conversions_value
                , base_report.all_conversions
                , base_report.all_conversions_value
                , base_report.view_through_conversions
                {% for conversion in conversions %}
                    , pivots.conversions_{{conversion.output_name}}
                    , pivots.conversions_value_{{conversion.output_name}}
                    , pivots.all_conversions_{{conversion.output_name}}
                    , pivots.all_conversions_value_{{conversion.output_name}}
                {% endfor %}
            from
                base_report
            left join
                pivots
            on
                base_report.date = pivots.date
            and
                base_report.campaign_id = pivots.campaign_id
            and
                base_report.device = pivots.device
            left join
                campaigns
            on
                base_report.campaign_id = campaigns.id
            left join
                accounts
            on
                campaigns.customer_id = accounts.id
            left join
                sync_info
            on
                true
        )
    
        select
            {% for dimension in dimensions %}
                {% unless forloop.first %}
                    , 
                {% endunless %}
                CAST({{dimension.expression}} as {{dimension.type}}) as `{{dimension.name}}`
            {% endfor %}
            {% for metric in metrics %}
                , CAST({{metric.expression}} as {{metric.type}}) as `{{metric.name}}`
            {% endfor %}
        from
            api
            {% if number_of_accounts > 0 %}
                where account_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}
                            , 
                        {% endunless %}
                        {{id}}
                    {% endfor %}
                )
            {% endif %}
        group by
            {% for dimension in dimensions %}
                {% unless forloop.first %}
                    , 
                {% endunless %}
                {{forloop.index}}
            {% endfor %}
    )
{% endif %}
;
