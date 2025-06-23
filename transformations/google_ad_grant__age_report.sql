-- google_ad_grant__age_report
/* 
    Change these variables so that the <CONNECTOR SERVICE> portion matches the connector service
    that you are working with. For example if you are creating a model for google_ads_grant then replace the text
    and <> signs with google_ads_grant
    
    Change these variables so that the <MODEL NAME> portion matches the model name that you are working with
    If you are creating a model called ad_report__google, you will change the <MODEL NAME> with ad_report__google
*/
{% assign dimensions = vars.google_ads_grant.models.google_ad_grant__age_report.dimensions %}
{% assign metrics = vars.google_ads_grant.models.google_ad_grant__age_report.metrics %}
{% assign account_id = vars.google_ads_grant.account_ids %}
{% assign active = vars.google_ads_grant.active %}
{% assign table_active = vars.google_ads_grant.models.google_ad_grant__age_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.google_ads_grant.models.google_ad_grant__age_report.table_id %}
{% assign source_dataset_id = vars.google_ads_grant.source_dataset_id %}
{% assign conversions = vars.google_ads_grant.conversions %}
{% assign number_of_accounts = vars.google_ads_grant.account_ids | size %}
{% assign micro_denominator = 1000000 %}
{% assign source_table_id = 'age_report' %}


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
              max(datetime(_gn_synced, "{{ vars.timezone }}")) as max_synced_at
              , max(date) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
            {% if number_of_accounts > 0 %}
                where customer_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}, {% endunless %}{{id}}
                    {% endfor %}
                )
            {% endif %}
        )

        , age_criteria as(
            select
                criterion_id as ad_group_criterion_criterion_id
                , age_range_type as criteria
            from
                {{source_dataset_id}}.{{source_table_id}}
            group by
                1
                , 2
        )
        , campaigns as(
            select
                *
            from
                {{source_dataset_id}}.campaign_history
            where _fivetran_active = true
        )
        
        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            qualify rank() over(partition by id order by updated_at desc) = 1
            and row_number() over(partition by id, updated_at) = 1      
        )
        
        , ad_groups as(
            select
                *
            from
                {{source_dataset_id}}.ad_group_history
            qualify rank() over(partition by id order by updated_at desc) = 1
            and row_number() over(partition by id, updated_at) = 1 
        )
        
        , ads as(
            select
                *
            from
                {{source_dataset_id}}.ad_history
            qualify rank() over(partition by id, ad_group_id order by updated_at desc) = 1
            and row_number() over(partition by id, ad_group_id, updated_at) = 1 
        )
        
        , report as(
            select
                date
                , ad_group_id
                , criterion_id as ad_group_criterion_criterion_id
                , sum(impressions) as impressions
                , sum(safe_divide(cost_micros, {{ micro_denominator }})) as cost
                , sum(clicks) as clicks
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
                1,2,3
            order by
                date asc
        )
        
        , pivots as(
            select
                *
            from
                (
                    select
                        date
                        , ad_group_id
                        , criterion_id as ad_group_criterion_criterion_id
                        , all_conversions -- This is what we are going to sum in our pivot table
                        , all_conversions_value  -- This is what we are going to sum in our pivot table
                        , conversions  -- This is what we are going to sum in our pivot table
                        , conversions_value  -- This is what we are going to sum in our pivot table
                        , conversion_action_name
                    from
                        {{source_dataset_id}}.{{source_table_id}}_custom_conversions
                )
            pivot(
                /* 
                    We need to toggle these. For instance we may not always want all of these columns by
                    default because it may be confusing for the user. Perhaps we only typically want 
                    all_conversions and all_conversions_value
                */
                sum(all_conversions) as all_conversions
                , sum(all_conversions_value) as all_conversions_value
                , sum(conversions) as conversions
                , sum(conversions_value) as conversions_value
                for conversion_action_name in (
                -- Need to create a loop here for conversion action names and their aliases
                    {% for conversion in conversions %}
                        {% unless forloop.first %}
                            ,
                        {% endunless %}
                        "{{conversion.event_name}}" {{ conversion.output_name }}
                    {% endfor %}
                )
        
            )
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
                , accounts.id as account_id
                , accounts.descriptive_name as account_name
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                , campaigns.id as campaign_id
                , campaigns.name as campaign_name
                , report.ad_group_id
                , ad_groups.name as ad_group_name
                , report.ad_group_criterion_criterion_id
                , report.impressions
                , report.cost
                , report.clicks
                , report.conversions
                , report.conversions_value
                , age_criteria.criteria
                , (
                    INITCAP(
                      REPLACE (
                        REPLACE (
                          age_criteria.criteria
                          , 'AGE_RANGE_'
                          , ''
                        )
                        , '_'
                        , '-'
                      )
                    )
                 ) as age_range
                {% for conversion in conversions %}
                    /* 
                        We need to toggle these. For instance we may not always want all of these columns by
                        default because it may be confusing for the user. Perhaps we only typically want 
                        all_conversions and all_conversions_value
                    */
                    , pivots.conversions_{{conversion.output_name}}
                    , pivots.conversions_value_{{conversion.output_name}}
                    , pivots.all_conversions_{{conversion.output_name}}
                    , pivots.all_conversions_value_{{conversion.output_name}}
                {% endfor %}
            from
                report
            left join
                pivots
            on
                report.date = pivots.date
            and
                report.ad_group_id = pivots.ad_group_id
            and
                report.ad_group_criterion_criterion_id = pivots.ad_group_criterion_criterion_id
            left join
                ad_groups
            on
                report.ad_group_id = ad_groups.id
            left join
                campaigns
            on
                ad_groups.campaign_id = campaigns.id
            left join
                accounts
            on
                campaigns.customer_id = accounts.id
            left join
                age_criteria
            on
                report.ad_group_criterion_criterion_id = age_criteria.ad_group_criterion_criterion_id
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
