-- google_ad_grant__ad_report
{% assign dimensions = vars.google_ads_grant.models.google_ad_grant__ad_report.dimensions %}
{% assign metrics = vars.google_ads_grant.models.google_ad_grant__ad_report.metrics %}
{% assign account_id = vars.google_ads_grant.account_ids %}
{% assign active = vars.google_ads_grant.active %}
{% assign table_active = vars.google_ads_grant.models.google_ad_grant__ad_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.google_ads_grant.models.google_ad_grant__ad_report.table_id %}
{% assign source_dataset_id = vars.google_ads_grant.source_dataset_id %}
{% assign conversions = vars.google_ads_grant.conversions %}
{% assign number_of_accounts = vars.google_ads_grant.account_ids | size %}
{% assign micro_denominator = 1000000 %}
{% assign source_table_id = 'ad_report' %}
{% assign ad_group_delimiter = vars.google_ads_grant.delimiters.ad_group %}
{% assign campaign_delimiter = vars.google_ads_grant.delimiters.campaign %}


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
        
        sync_info as (
            select
              max(current_datetime()) as max_synced_at
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
                source.date
                , source.ad_id
                , source.ad_group_id
                , source.campaign_id
                , split(regexp_replace(final_urls, r"\[|\]|https:\/\/|www\.|http:\/\/", ''), '/')[safe_ordinal(1)] as final_url
                , sum(safe_divide(source.cost_micros, {{ micro_denominator }})) as cost
                , sum(source.clicks) as clicks
                , sum(source.impressions) as impressions
                , sum(source.conversions) as conversions
                , sum(source.conversions_value) as conversions_value
            from
                {{source_dataset_id}}.{{source_table_id}} as source
            left join
                ads
            on
                source.ad_id = ads.id
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
        
        , pivots as(
            select
                *
            from
                (
                    select
                        date
                        , ad_id
                        , ad_group_id
                        , campaign_id
                        , all_conversions -- This is what we are going to sum in our pivot table
                        , all_conversions_value  -- This is what we are going to sum in our pivot table
                        , conversions  -- This is what we are going to sum in our pivot table
                        , conversions_value  -- This is what we are going to sum in our pivot table
                        , conversion_action_name
                    from
                        {{source_dataset_id}}.ad_performance_report_conversion_stats
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
                        {% if forloop.first %}
                            "{{conversion.event_name}}" {{ conversion.output_name }}
                        {% else %}
                            , "{{conversion.event_name}}" {{ conversion.output_name }}
                        {% endif %}
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
                , report.ad_id
                , report.ad_group_id
                , ad_groups.name as ad_group_name
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                , report.campaign_id
                , campaigns.name as campaign_name
                , accounts.descriptive_name as account_name
                , accounts.id as account_id
                , campaigns.advertising_channel_type
                , report.clicks
                , report.cost
                , report.impressions
                , report.conversions
                , report.conversions_value
                , report.final_url
                , '' as path_1
                , '' as path_2
                , '' as description
                , '' as headline
                , (select string_agg(part, "/") from unnest([final_url, lower(''), lower('')]) part) as full_display_url
                , campaigns.start_date as campaign_start_date
                , campaigns.end_date as campaign_end_date
                
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(1)]) as campaign_pos_1
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(2)]) as campaign_pos_2
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(3)]) as campaign_pos_3
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(4)]) as campaign_pos_4
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(5)]) as campaign_pos_5
                
                
                , trim(split(ad_groups.name, '{{ad_group_delimiter}}')[safe_ordinal(1)]) as ad_group_pos_1
                , trim(split(ad_groups.name, '{{ad_group_delimiter}}')[safe_ordinal(2)]) as ad_group_pos_2
                , trim(split(ad_groups.name, '{{ad_group_delimiter}}')[safe_ordinal(3)]) as ad_group_pos_3
                , trim(split(ad_groups.name, '{{ad_group_delimiter}}')[safe_ordinal(4)]) as ad_group_pos_4
                , trim(split(ad_groups.name, '{{ad_group_delimiter}}')[safe_ordinal(5)]) as ad_group_pos_5
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
                report.ad_id = pivots.ad_id
            and
                report.ad_group_id = pivots.ad_group_id
            and
                report.campaign_id = pivots.campaign_id
            left join
                campaigns
            on
                report.campaign_id = campaigns.id
            left join
                ad_groups
            on
                report.ad_group_id = ad_groups.id
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
