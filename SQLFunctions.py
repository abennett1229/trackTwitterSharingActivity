import os

#Read in a file and return a two-dimensional list representation
def file_to_matrix(filename):
    #Read in the file
    result_str = ''
    f = open(filename)
    result_str = f.read()
    f.close()
    #Initialize a new matrix to store the results
    result_matrix = []
    #Split rows into a list
    for row in result_str.split('\n'):
        #Filter out any last null rows
        if (row != ''):
            new_row = []
                for entry in row.split('\t'):
                    new_row.append(entry)
                        result_matrix.append(new_row)
    return result_matrix

#Default credentials for sandbox SQL
def get_sandbox_credentials():
    mode = 'sql'
    user = 'outbrain_sandbox'
    password = 'xxxxxxxxxxxxxxxx'
    host = 'mysql-xxxxxxxxxxxxxxxxxx.outbrain.com'
    port = 'xxxx'
    return {'mode':mode,'user':user,'password':password,'host':host,'port':port}

#Default credentials for datawarehouse SQL
def get_datawarehouse_credentials():
    mode = 'sql'
    user = 'ob_reader'
    password = 'xxxxxxxxxxxxxxxx'
    host = 'mysql-xxxxxxxxxxxxxxxxxxx.outbrain.com'
    port = 'xxxx'
    return {'mode':mode,'user':user,'password':password,'host':host,'port':port}

#Get all active sandbox campaigns in list of tuples (campaign/advertiser)
def get_campaign_ids(my_date):
	#Get sandbox campaigns from outbrain_sandbox
	sb_campaign_list = []
	sb_campaign_output_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_sb_campaigns.txt'
	sb_campaign_query_filename = '/home/abennett/sandbox/social/GetSBCampaigns.sql'
	my_credentials = get_sandbox_credentials()
	sb_campaign_query_str = "mysql --user=%s --password=%s --host=%s --port=%s < %s > %s" % (my_credentials['user'],my_credentials['password'], my_credentials['host'], my_credentials['port'], sb_campaign_query_filename,sb_campaign_output_filename)
	os.popen(sb_campaign_query_str).read()
	mx_sb_campaigns = file_to_matrix(sb_campaign_output_filename)
	for row in mx_sb_campaigns:
		campaign_id = row[0]
		if campaign_id != "sbpa_campaign_id":
			sb_campaign_list.append(campaign_id)
	str_sb_campaign_list = str(sb_campaign_list)
	str_sb_campaign_list = str_sb_campaign_list.replace('[','(')
	str_sb_campaign_list = str_sb_campaign_list.replace(']',')')
	#Filter out inactive campaigns
	my_credentials = get_datawarehouse_credentials()
	active_campaign_advertiser_list = []
	active_campaign_output_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_sb_active_campaigns.txt'
	active_campaign_query_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_ActiveCampaigns.sql'
	active_campaign_query_file = open(active_campaign_query_filename,'w')
	active_campaign_query_template_filename = '/home/abennett/sandbox/social/GetActiveCampaigns.sql'
	active_campaign_query_template_file = open(active_campaign_query_template_filename)
	active_campaign_query_content = active_campaign_query_template_file.read()
	active_campaign_query_content = active_campaign_query_content.replace('campaign_id_list',str_sb_campaign_list)
	active_campaign_query_content = active_campaign_query_content.replace('my_date',str(my_date))
	active_campaign_query_file.write(active_campaign_query_content)
	active_campaign_query_file.close()
	active_campaign_query_str = "mysql --user=%s --password=%s --host=%s --port=%s < %s > %s" % (my_credentials['user'],my_credentials['password'], my_credentials['host'], my_credentials['port'], active_campaign_query_filename,active_campaign_output_filename)
        os.popen(active_campaign_query_str).read()
	mx_active_campaigns = file_to_matrix(active_campaign_output_filename)
	for row in mx_active_campaigns:
		campaign_id = row[0]
		advertiser_id = row[1]
		if campaign_id != 'obcm_campaign_id':
			active_campaign_advertiser_list.append((campaign_id,advertiser_id))
	return active_campaign_advertiser_list	

#Get the top 3 URL's for each active sandbox campaign by number of clicks
def get_campaign_urls(my_date,my_campaign_id):
	my_credentials = get_datawarehouse_credentials()
	url_output_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + "_" + my_campaign_id + "_top_urls.txt"
	url_query_template_filename = '/home/abennett/sandbox/social/GetTopURLs.sql'
	url_query_template_file = open(url_query_template_filename)
	url_query_content = url_query_template_file.read()
 	url_query_content = url_query_content.replace('my_campaign_id',my_campaign_id)
	url_query_content = url_query_content.replace('my_date',str(my_date))
	url_query_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_' + my_campaign_id + '_top_urls.sql'
	url_query_file = open(url_query_filename,'w')
	url_query_file.write(url_query_content)
	url_query_file.close()
	query_str = "mysql --user=%s --password=%s --host=%s --port=%s < %s > %s" % (my_credentials['user'],my_credentials['password'], my_credentials['host'], my_credentials['port'], url_query_filename, url_output_filename)
        os.popen(query_str).read()
	url_dict = {}
	mx_urls =  file_to_matrix(url_output_filename)
	for row in mx_urls:
		doc_id = row[0]
		url = row[1]
		clicks = row[2]
		impressions = row[3]
		spend = row[4]
		if doc_id == 'doc_id':
			continue
		url_dict[doc_id] = [url,clicks,impressions,spend]
	return url_dict

#Load data from text file into MySQL table
def LoadToSQL(my_date,table_prefix):
	input_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_' + table_prefix + '_data.txt'
	query_filename = '/home/abennett/sandbox/social/' + str(my_date).replace('-','') + '_load_' + table_prefix + '.sql'
	query_file = open(query_filename,'w')
	my_credentials = get_sandbox_credentials()
	query_template_filename = '/home/abennett/sandbox/social/load_' + table_prefix + '.sql'
	query_template_file = open(query_template_filename)
	query_content = query_template_file.read()
	query_content = query_content.replace('filename',input_filename)
	query_file.write(query_content)
	query_file.close()
	query_str = "mysql --user=%s --password=%s --host=%s --port=%s < %s" % (my_credentials['user'],my_credentials['password'], my_credentials['host'], my_credentials['port'], query_filename)
	os.popen(query_str).read()

		
