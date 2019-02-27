import os
from dateutil.relativedelta import relativedelta
from datetime import date
from SQLFunctions import get_campaign_ids,get_campaign_urls,LoadToSQL
from TwitterFunctions import get_twitter_data

#Set date as yesterday's date
my_date = date.today() - relativedelta(days=1)
#Get active sandbox campaigns
campaign_advertiser_list = get_campaign_ids(my_date)
#Define filenames to be loaded into MySQL tables
sbsa_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_sbsa_data.txt'
sbsa_file = open(sbsa_filename,'w')
sbsi_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_sbsi_data.txt'
sbsi_file = open(sbsi_filename,'a')
#Iterate through list of active sandbox campaigns to retrieve Outbrain data and Twitter data for top URL's
for campaign_advertiser in campaign_advertiser_list:
    my_campaign_id = campaign_advertiser[0]
        my_advertiser_id = campaign_advertiser[1]
        #Get OB data for top url's in a campaign
        ob_dict = get_campaign_urls(my_date,my_campaign_id)
        doc_id_list = ob_dict.keys()
        for my_doc_id in doc_id_list:
        	my_url = ob_dict[my_doc_id][0]
                #Get social activity for top url's
                twitter_dict = get_twitter_data(my_date,my_advertiser_id,my_campaign_id,my_doc_id,my_url,sbsi_file)
                sbsa_file.write(str(my_date) + '\t' + my_advertiser_id + '\t' + my_campaign_id + '\t' + my_doc_id + '\t' + my_url + '\t' + str(ob_dict[my_doc_id][1]) + '\t' + str(ob_dict[my_doc_id][2]) + '\t' + str(ob_dict[my_doc_id][3]) + '\t' + str(twitter_dict['tweets']) + '\t' + str(twitter_dict['retweets']) + '\t' + str(twitter_dict['followers']) + '\t' + str(twitter_dict['influencers']) + '\n')
#Load Outbrain data and Twitter data into MySQL tables
LoadToSQL(my_date,'sbsa')
LoadToSQL(my_date,'sbsi')

#Delete yesterday's files from local directory
for campaign_advertiser in campaign_advertiser_list:
        my_campaign_id = campaign_advertiser[0]
	sql_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_' + my_campaign_id + '_top_urls.sql'
	txt_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_' + my_campaign_id + '_top_urls.txt'
	try:
		os.remove(sql_filename)
		os.remove(txt_filename)
	except:
		continue
