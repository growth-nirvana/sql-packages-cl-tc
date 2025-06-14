{% assign dimensions = vars.google_ads_search.models.google_commercial_search__campaign_report_custom_conversions.dimensions %}
{% assign metrics = vars.google_ads_search.models.google_commercial_search__campaign_report_custom_conversions.metrics %}
{% assign account_id = vars.google_ads_search.account_ids %}
{% assign active = vars.google_ads_search.active %}
{% assign table_active = vars.google_ads_search.models.google_commercial_search__campaign_report_custom_conversions.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.google_ads_search.models.google_commercial_search__campaign_report_custom_conversions.table_id %}
{% assign source_table_id = vars.google_ads_search.models.google_commercial_search__campaign_report_custom_conversions.source_table_id %}
{% assign source_dataset_id = vars.google_ads_search.source_dataset_id %}
{% assign number_of_accounts = vars.google_ads_search.account_ids | size %}
{% assign source_table_id = 'campaign_report_custom_conversions' %}

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

        campaigns as(
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
        
        , report as(
            select
                date
                , customer_id as account_id
                , id as campaign_id
                , name as campaign_name
                , conversion_action_name as conversion_action_name
                , sum(all_conversions) as all_conversions
                , sum(all_conversions_value) as all_conversions_value
                , sum(conversions) as conversions
                , sum(conversions_value) as conversions_value
            from
                {{source_dataset_id}}.{{source_table_id}}
            {% if number_of_accounts > 0 %}
                where customer_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}, {% endunless %}{{id}}
                    {% endfor %}
                )
            {% endif %}
            group by
                1,2,3,4,5
        )
        /* 
            This is caled API (case insensitive) because we are establishing what the user
            of this package can and cannot reference from the fivetran table. Because the can add
            anything that they want to the config we have to limit what they can access so that we can 
            make sure that everything is always present from what they need.
        */
        , api as(
            select
                report.date
                
                /* ************************************
                    Foreign Keys Section
                ************************************ */
                , report.account_id
                , report.campaign_id
                
                /* ************************************
                    Accounts Section
                ************************************ */
                , accounts.descriptive_name as account_name
                
                /* ************************************
                    Campaigns Section
                ************************************ */
                , campaigns.name as campaign_name
                , campaigns.advertising_channel_type
                , report.conversion_action_name
                
                /* ************************************
                    Metrics Section
                ************************************ */
                , report.all_conversions
                , report.all_conversions_value
                , report.conversions
                , report.conversions_value
            from
                report
            left join
                campaigns
            on
                report.campaign_id = campaigns.id
            left join
                accounts
            on
                report.account_id = accounts.id
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
