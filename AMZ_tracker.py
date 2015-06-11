from bs4 import BeautifulSoup
import urllib2
import csv
import pandas as pd
import time
import lxml
import datetime
import pickle
import requests
import re
import sys
import os
import pymongo

from retry import retry as _retry
from endpoints import Search_Business, Search_Amish, Category_Amish, Best_Amish
from asins import business_search_asins, business_tracking, amish_asin

reload(sys)
sys.setdefaultencoding("utf-8")

# def url_builder(page_range, terms):
#     base_url = 'http://www.amazon.com/s/rh=%2Ck%3A'
#     page = '&page='
#     plus = '+'
#     search_terms = plus.join(terms.split())
#     return [base_url + search_terms + page + str(i) for i in range(1, page_range)]

def iter_elements_extract_or_skip_attributes(elements, *attributes):
    for element in elements:
        values = tuple(element.get(att, None) for att in attributes)
        if all(values):
            yield values

def check_asin(result_list, isbn_set):
    new_result = [item for item in result_list if item[1] in isbn_set]
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

def Amz_Search_Results(endpoint, isbn_set):
    amz = urlopen_with_retry(endpoint)
    soup = BeautifulSoup(amz, "lxml")
    findone = BeautifulSoup(str(soup.find_all("li", class_="s-result-item")))
    elements = findone.find_all('li')
    iter_data = iter_elements_extract_or_skip_attributes(elements, 'id', 'data-asin')
    return [item for item in iter_data if item[1] in isbn_set]

def Amz_Search_Results_alt(endpoint, isbn_set):
    amz = urlopen_with_retry(endpoint)
    soup = BeautifulSoup(amz, "lxml")
    findone = BeautifulSoup(str(soup.find_all("div", attrs={"id": "mainResults"})))
    elements = findone.find_all('div')
    iter_data = iter_elements_extract_or_skip_attributes(elements, 'id', 'name')
    return [item for item in iter_data if item[1] in isbn_set]

def Amz_bestseller(endpoint, isbn_set):
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
    final_result = check_asin(zip(rank_list, asin_list, title_list), isbn_set)
    return final_result

def run_spider_run(item_search, isbn_set):
    results_dict = {}
    for key, value in item_search.iteritems():
        results = []
        for url in value:
            # try:
            time.sleep(0.5)
            x = Amz_Search_Results(url, isbn_set)
            if x == []:
                time.sleep(0.5)
                x = Amz_Search_Results_alt(url, isbn_set)
            results.extend(convert_index(x))
            results_dict[key] = results
    return results_dict

def run_spider_bestseller(item_search, isbn_set):
    results_dict = {}
    for key, value in item_search.iteritems():
        results = []
        for url in value:
            time.sleep(0.5)
            x = Amz_bestseller(url, isbn_set)
            results.extend(x)
            results_dict[key] = results
    return results_dict

def reformat_data_add_column(data_name, output_name, rank_type, tracking_asin, database_collection):
    df = pd.DataFrame.from_dict(data_name, orient='index')
    data = df.transpose()
    results = []
    today = str(datetime.datetime.now())
    for index, row in data.iteritems():
        for old_cell in row:
            if pd.notnull(old_cell):
                cell = str(old_cell)
                cell = cell.split(',')
                rank = cell[0][1:]
                number = cell[1][2:-2]
                tracking = 'no'
                if number in tracking_asin:
                    tracking = 'yes'
                x = index, rank, number, rank_type, today, tracking
                results.append(x)
    name = output_name + "_" + today + '.csv'
    myfile = open(name, 'wb')
    wr = csv.writer(myfile, delimiter='|', quoting= csv.QUOTE_NONE)
    head = ('Terms', 'Rank', 'ASIN/ISBN', 'Rank Type', 'Date/time Collected', 'tracking')
    wr.writerow(head)
    for row in results:
        wr.writerow(row)
    print("----- MSG: CSV output")
    results_d = {}
    results_d[today[0:10]] = results
    database_collection.insert(results_d)
    print("----- MSG: DATABASE Write")
    print("----- MSG: NEXT PROCESS STARTING")

def reformat_data_add_column_bestseller(data_name, output_name, rank_type, tracking_asin, database_collection):
    df = pd.DataFrame.from_dict(data_name, orient='index')
    data = df.transpose()
    results = []
    today = str(datetime.datetime.now())
    for index, row in data.iteritems():
        for old_cell in row:
            if pd.notnull(old_cell):
                cell = str(old_cell)
                cell = cell.split(',')
                rank = cell[0][1:]
                number = cell[1][2:-1]
                tracking = 'no'
                if number in tracking_asin:
                    tracking = 'yes'
                x = index, rank, number, rank_type, today, tracking
                results.append(x)
    name = output_name + "_" + today + '.csv'
    myfile = open(name, 'wb')
    wr = csv.writer(myfile, delimiter='|', quoting= csv.QUOTE_NONE)
    head = ('Terms', 'Rank', 'ASIN/ISBN', 'Rank Type', 'Date/time Collected', 'tracking')
    wr.writerow(head)
    for row in results:
        wr.writerow(row)
    print("----- MSG: CSV output")
    results_d = {}
    results_d[today[0:10]] = results
    database_collection.insert(results_d)
    print("----- MSG: DATABASE Write")
    print("----- MSG: NEXT PROCESS STARTING")

def make_pickle(data_name, output_name):
    today = str(datetime.datetime.now())[0:10]
    name = output_name + today + ".p"
    pickle.dump( data_name, open( name, "wb" ) )
    print("----- MSG: python output")

def business_search_runner():
    print("----MSG: Running Business Search")
    business_result = run_spider_run(Search_Business, business_tracking)
    make_pickle(business_result, "business_search")
    reformat_data_add_column(business_result, 'business_search', 'search', business_tracking, collection_business_search)

def amish_search_runner():
    print("----MSG: Running Amish Search")
    amish_search_result = run_spider_run(Search_Amish, amish_asin)
    make_pickle(amish_search_result, "amish_search")
    reformat_data_add_column(amish_search_result, 'amish_search', 'search', amish_asin, collection_amish_search)

def amish_category_runner():
    print("----MSG: Running Amish Category")
    amish_category_result = run_spider_run(Category_Amish, amish_asin)
    make_pickle(amish_category_result, "amish_category")
    reformat_data_add_column(amish_category_result, 'amish_category', 'category', amish_asin, collection_amish_category)    

def amish_bestseller_runner():
    print("----MSG: Running Amish Bestseller")
    amish_bestseller_result = run_spider_bestseller(Best_Amish, amish_asin)
    make_pickle(amish_bestseller_result, "amish_best")
    reformat_data_add_column_bestseller(amish_bestseller_result, 'amish_bestseller', 'bestseller', amish_asin, collection_amish_bestseller)

def all_runner():
    print("----MSG: Running All")
    business_result = run_spider_run(Search_Business, business_tracking)
    make_pickle(business_result, "business_search")
    reformat_data_add_column(business_result, 'business_search', 'search', business_tracking, collection_business_search)

    amish_category_result = run_spider_run(Category_Amish, amish_asin)
    make_pickle(amish_category_result, "amish_category")
    reformat_data_add_column(amish_category_result, 'amish_category', 'category', amish_asin, collection_amish_category)

    amish_search_result = run_spider_run(Search_Amish, amish_asin)
    make_pickle(amish_search_result, "amish_search")
    reformat_data_add_column(amish_search_result, 'amish_search', 'search', amish_asin, collection_amish_search)

    amish_bestseller_result = run_spider_bestseller(Best_Amish, amish_asin)
    make_pickle(amish_bestseller_result, "amish_best")
    reformat_data_add_column_bestseller(amish_bestseller_result, 'amish_bestseller', 'bestseller', amish_asin, collection_amish_bestseller)


if __name__ == "__main__":

    home_msg = "----- MISMI: AMAZON TRACKING TOOL \n ---- by G.Levine"
    db_msg = "----- MSG: DATABASE ACTIVE - MongoDB"
    last_msg = "----- AMAZON TRACKING Complete. Have a nice day."

    print(home_msg)

    conn = pymongo.MongoClient()
    db = conn.amzdb
    collection_amish_bestseller = db.amish_bestseller
    collection_amish_category = db.amish_category
    collection_amish_search = db.amish_search
    collection_business_search = db.business_search

    print(db_msg)
    print("Which tracker would you like to run?")
    print("All, Business Search, All Amish, Amish Search, Amish Category, Amish Bestseller?")
    action = raw_input("> ")
    if action == "All":
        all_runner()
    if action == "Business Search":
        business_search_runner()
    if action == "Amish Search":
        amish_search_runner()
    if action == "Amish Category":
        amish_category_runner()
    if action == "Amish Bestseller":
        amish_bestseller_runner()
    if action == "All Amish":
        print("Running through all Amish")
        amish_category_runner()
        amish_search_runner()
        amish_bestseller_runner()
    else:
        print("please input again")

    print(last_msg)
