## Report Structure
The Google Report section consists of 3 possible sections

- Google Ad Grant
- Google Commercial Search
- Google Display

The first section typically has its own customer id within Google Ads, but the second two typically share the same customer id but are separated using a filter

### Data Sources
#### Google Ad Grant

* google_ad_grant__ad_report
* google_ad_grant__age_report
* google_ad_grant__campaign_report
* google_ad_grant__campaign_report_custom_conversions
* google_ad_grant__device_report

#### Google Commercial Search & Google Display
These two sections share the same data sources. They are separated only by a filter using the word display in the campaign name.

* google_commercial_search__ad_report
* google_commercial_search__campaign_report
* google_commercial_search__campaign_report_custom_conversions
* google_commercial_search__city_report
* google_commercial_search__device_report
* google_commercial_search__keyword_report

