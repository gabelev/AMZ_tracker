#a super bare-bones method to track rankings in Amazon Search results
from bs4 import BeautifulSoup
from urllib2 import urlopen
import csv
import pandas as pd
#put the endpoints of AMZ search pages you want to collect from
#only pulling the first 3 pages of the searches
from endpoints_2 import *

def Amz_Search_Results(endpoint):
#opens and parses the page
	amz = urlopen(endpoint)
	soup = BeautifulSoup(amz, "lxml")
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
	new_df.to_csv('new_output.csv')

#yes, we are doing our test case on Amish Romance Novels            
final_result = run_spider_run(Category_Amish)
format_data(final_result, 'output.csv')


