select
obcm_campaign_id,
obcm_advertiser_id

from

outbrain_dwh.obcm_campaign_traffic_d

where 
obcm_campaign_id in campaign_id_list and
obcm_stats_date = 'my_date' and
obcm_num_billed_clicks>99

limit 999999
