#a super bare-bones method to track rankings in Amazon Search results

from bs4 import BeautifulSoup
from urllib2 import urlopen
# import pandas as pd
import re
import lxml
import sys
import requests
import pymongo

reload(sys)
sys.setdefaultencoding("utf-8")

from endpoints_new7 import *


def Amz_Search_Results(endpoint):
#opens and parses the page
	amz = urlopen(endpoint)
	soup = BeautifulSoup(amz, "html.parser")
	#searches for the li tag with results items in it
	findone = BeautifulSoup(str(soup.find_all("li", class_="s-result-item")))
	results = []
	#find only the <id tag> with the results in them
	for b in findone.find_all('li'):
	    try:
	    	#pulls only the result_number and asin
	        x = b['id'], b['data-asin']
	        results.append(x)
	    except Exception:
	        pass
	return results

#accounts for an alternate web page format
def Amz_Search_Results_alt(endpoint):
    amz = urlopen(endpoint)
    soup = BeautifulSoup(amz, "lxml")
    findtwo = BeautifulSoup(str(soup.find_all("div", attrs={"id": "mainResults"})))
    results = []
    for b in findtwo.find_all('div'):
        try:
            x = b['id'], b['name']
            results.append(x)
        except Exception:
            pass
    return results

#pulls data from bestseller pages which are javascript pages
#pulls via AJAX request and parses resonse. More URLS but fastser scrape
def Amz_bestseller(endpoint):
    r = requests.get(endpoint)
    soup = BeautifulSoup(str(r.text), "html")
    findtwo = BeautifulSoup(str(soup.find_all("div", attrs={"class": "zg_itemImmersion"})))
    rank = BeautifulSoup(str(findtwo.find_all("span", attrs={"class": "zg_rankNumber"})))
    find_rank = rank.text
    rank_list = map(int, re.findall('\d+', find_rank))
    title_list = []
    for tag in findtwo.find_all('img'):
        title_list.append(tag.get('title'))
    asin_list = []
    for b in findtwo.find_all("span", attrs={"class": "asinReviewsSummary acr-popover"}):
        x = b['name']
        asin_list.append(x)
    final_result = zip(rank_list, asin_list, title_list)
    return final_result

#cycles through the bestseller request URLS
def run_spider_bestseller(item_search):
    results_dict = {}
    #runs through the list of endpoints
    for key, value in item_search.iteritems():
        results = []
        for url in value:
            try:
                x = Amz_bestseller(url)
                # if x == []:
                #     x = Amz_Search_Results_alt(url)
                results.extend(x)
            except Exception:
                pass
            results_dict[key] = results
    return results_dict

#cycles through the Category and Search URLS
def run_spider_run(item_search):
    results_dict = {}
    #runs through the list of endpoints
    for key, value in item_search.iteritems():
        results = []
        for url in value:
            try:
                x = Amz_Search_Results(url)
                #tests to see if the function is not pulling what we need
                #if so, then it uses the alternate func
                if x == []:
                    x = Amz_Search_Results_alt(url)
                results.extend(x)
            except Exception:
                pass
            results_dict[key] = results
    return results_dict

def format_data(data_name, output):
	#converts the dictionary of results into a dataframe just so we can turn it
	#and easily output as csv file so others can play with it
	df = pd.DataFrame.from_dict(data_name, orient='index')
	new_df = df.transpose()
	new_df.to_csv('title.csv')
  
result_bestseller = run_spider_bestseller(Best_Amish)
conn_1 = pymongo.MongoClient()
db = conn_1.amzdb
collection_1 = db.amish_bestseller
collection_1.insert(result_bestseller)

result_category = run_spider_bestseller(Category_Amish)
conn_2 = pymongo.MongoClient()
db = conn_2.amzdb
collection_2 = db.amish_category
collection_2.insert(result_category)

result_search = run_spider_bestseller(Search_Amish)
conn_3 = pymongo.MongoClient()
db = conn_3.amzdb
collection_3 = db.amish_search
collection_3.insert(result_search)
