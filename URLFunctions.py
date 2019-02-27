import urllib2

def get_url_destination(my_url):
	response = urllib2.urlopen(my_url)
	url_destination = response.url
	return url_destination
