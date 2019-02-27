select
distinct pcad_document_id as doc_id,
pcad_listing_url as url,
sum(a.clicks) as clicks,
sum(a.impressions) as impressions,
sum(a.spend) as spend

from

outbrain_federated.pcad_ad,
(select
obad_ad_id as ad_id,
obad_num_billed_clicks as clicks,
obad_num_impressions as impressions,
obad_cost_per_click_total as spend

from

outbrain_dwh.obad_ad_traffic_d

where

obad_campaign_id = 'my_campaign_id' and
obad_stats_date = 'my_date'

order by clicks desc

limit 20)a

where

a.ad_id = pcad_id

group by 1,2

having clicks > 99

order by clicks desc 

limit 3
