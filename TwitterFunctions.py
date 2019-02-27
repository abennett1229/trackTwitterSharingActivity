from dateutil.relativedelta import relativedelta
import datetime
from datetime import date
from TwitterAPI import TwitterAPI 
from URLFunctions import get_url_destination

#Get number of tweets, retweets, followers, and top (most followed) tweeters for a URL
def get_twitter_data(my_date,my_advertiser_id,my_campaign_id,my_doc_id,my_url,sbsi_file):
	next_day = my_date + relativedelta(days=1)
	try:
		my_url = get_url_destination(my_url)
	except:
		pass
	SEARCH_TERM = '%s since:%s' % (my_url,my_date)
	CONSUMER_KEY = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
	CONSUMER_SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
	ACCESS_TOKEN_KEY = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
	ACCESS_TOKEN_SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxx'
	api = TwitterAPI(
	    CONSUMER_KEY,
	    CONSUMER_SECRET,
	    ACCESS_TOKEN_KEY,
	    ACCESS_TOKEN_SECRET)
	iter_count = 0
	max_id = 0
	tweets = 0
	retweets = 0
	followers = 0
	user_followers_list = []
	top_users_list = []
    #Iterate up to 499 times to get all of yesterday's tweets of the URL, starting with the most recent (max 100 results at a time)
	for n in range(1,500):
		if max_id == 0: 
			r = api.request('search/tweets', {'q': SEARCH_TERM,'count':100,'result_type':'recent','until': next_day})
		else:
			r = api.request('search/tweets', {'q': SEARCH_TERM,'count':100,'result_type':'recent','max_id': max_id, 'until': next_day})
		url_list = []
		id_list = []
		for item in r:
	    		if 'entities' in item:
				id = item['id']
				retweets = retweets + item['retweet_count']
				followers = followers + item['user']['followers_count']
				user_followers = (item['user']['screen_name'],item['user']['followers_count'])	
				id_list.append(id)
				url_dict = item['entities']['urls'][0]
				expanded_url = url_dict['expanded_url']
				try:
                			expanded_url = get_url_destination(expanded_url)
        			except:
                			pass
				url_list.append(expanded_url)
				user_followers_list.append(user_followers)
		try:
			max_id = min(id_list) -1
			iter_count = iter_count + 1
		except:
			break
		tweets = tweets + len(url_list)
		if len(url_list) < 100:
			break
	if iter_count > 0:
        #Get top 5 tweeters by number of followers
		influencers_list = []
		sorted_user_followers_list = sorted(user_followers_list,key=lambda x: x[1],reverse=True)
		for n in range(0,5):
			try:
                i_name = sorted_user_followers_list[n][0]
				i_name = i_name.encode('ascii','ignore')
				i_followers = sorted_user_followers_list[n][1]
				influencers_list.append(i_name)
				sbsi_file.write(str(my_date) + '\t' + my_advertiser_id + '\t' + my_campaign_id + '\t' + my_doc_id + '\t' + my_url + '\t' + i_name + '\t' + str(i_followers) + '\n')
			except:
				continue
		influencers_list = str(influencers_list).replace('[','')
		influencers_list = influencers_list.replace(']','')
		return {'tweets': tweets,'retweets': retweets,'followers': followers, 'influencers': influencers_list}
	else:
		return {'tweets': 0,'retweets': 0,'followers': 0, 'influencers': 'null'}	



