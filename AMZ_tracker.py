from bs4 import BeautifulSoup
import urllib2
import csv
# import pandas as pd
import time
import lxml
import sys
import requests
import re
from retry import retry as _retry
import pymongo

#insert curated endpoints here
from endpoints import Best_Amish, Category_Amish, Search_Amish

reload(sys)
sys.setdefaultencoding("utf-8")

prh_asin = ['xyz', 'abc']

def iter_elements_extract_or_skip_attributes(elements, *attributes):
    for element in elements:
        values = tuple(element.get(att, None) for att in attributes)
        if all(values):
            yield values

def check_asin(result_list):
    new_result = [item for item in result_list if item[1] in prh_asin]
    return new_result

def remove_prefix(s, prefix):
    assert s.startswith(prefix)
    return s[len(prefix):]

def convert_index(result_list):
    getidx = lambda s: 1 + int(remove_prefix(s, 'result_'))
    return [(getidx(s), asin) for s, asin in result_list]

@_retry(urllib2.URLError, tries=4, delay=3, backoff=2)
def urlopen_with_retry(url):
    return urllib2.urlopen(url)

@_retry(requests.ConnectionError, tries=4, delay=3, backoff=2)
def request_with_retry(url):
    return requests.get(url)

def Amz_Search_Results(endpoint):
    amz = urlopen_with_retry(endpoint)
    soup = BeautifulSoup(amz, "lxml")
    findone = BeautifulSoup(str(soup.find_all("li", class_="s-result-item")))
    elements = findone.find_all('li')
    iter_data = iter_elements_extract_or_skip_attributes(elements, 'id', 'data-asin')
    return [item for item in iter_data if item[1] in prh_asin]

def Amz_Search_Results_alt(endpoint):
    amz = urlopen_with_retry(endpoint)
    soup = BeautifulSoup(amz, "lxml")
    findone = BeautifulSoup(str(soup.find_all("div", attrs={"id": "mainResults"})))
    elements = findone.find_all('div')
    iter_data = iter_elements_extract_or_skip_attributes(elements, 'id', 'name')
    return [item for item in iter_data if item[1] in prh_asin]

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
    final_result = check_asin(zip(rank_list, asin_list, title_list))
    return final_result

def run_spider_bestseller(item_search):
    results_dict = {}
    #runs through the list of endpoints
    for key, value in item_search.iteritems():
        results = []
        for url in value:
            try:
                time.sleep(0.5)
                x = Amz_bestseller(url)
                results.extend(x)
            except Exception as e:
                 print e
            results_dict[key] = results
    return results_dict

def run_spider_run(item_search):
    results_dict = {}
    for key, value in item_search.iteritems():
        results = []
        for url in value:
            time.sleep(0.5)
            x = Amz_Search_Results(url)
            if x == []:
                time.sleep(0.5)
                x = Amz_Search_Results_alt(url)
            results.extend(convert_index(x))
            results_dict[key] = results
    return results_dict

def format_data(data_name, output):
    df = pd.DataFrame.from_dict(data_name, orient='index')
    new_df = df.transpose()
    new_df.to_csv('Bestseller_Amish_debug.csv')

if __name__ == "__main__":

    conn_1 = pymongo.MongoClient()
    db = conn_1.amzdb
    collection_1 = db.amish_bestseller
    result_bestseller = run_spider_bestseller(Best_Amish)
    collection_1.insert(result_bestseller)

    conn_2 = pymongo.MongoClient()
    db2 = conn_2.amzdb
    collection_2 = db2.amish_category
    result_category = run_spider_run(Category_Amish)
    collection_2.insert(result_category)

    conn_3 = pymongo.MongoClient()
    db3 = conn_3.amzdb
    collection_3 = db3.amish_search
    result_search = run_spider_run(Search_Amish)
    collection_3.insert(result_search)
