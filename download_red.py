from lxml import html  
import csv
import requests
from time import sleep
import time
import datetime
import re
import argparse
from string import Template
import random
import os.path
import os
import sys

# http header for querying redfin, so that it is not recognized as bot
kHeaders = {'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 'accept-encoding':'gzip, deflate, sdch, br', 'accept-language':'en-GB,en;q=0.8,en-US;q=0.6,ml;q=0.4', 'cache-control':'max-age=0', 'upgrade-insecure-requests':'1', 'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}

# the template for creating quries to get the listing of sold homes
# TODO: a better way is to pass the params to requests library as a dict
# see http://docs.python-requests.org/zh_CN/latest/user/quickstart.html
kSoldQueryTemplate = Template("https://www.redfin.com/stingray/api/gis-csv?al=1&market=sanfrancisco&num_homes=$max_home_num&ord=redfin-recommended-asc&page_number=1&region_id=$region_id&region_type=$region_type&sold_within_days=$sold_within_days&sp=true&status=9&uipt=$home_type&v=8")

# 5000 is large enough for sold_within_days <=90
kMaxHomeNum = 5000
kCityRegionType = 6
kSingleAndTownHomeType = "1,3"

kCityNameRedfinIdMap = {"San Jose":17420, "Milpitas":12204, "Campbell":2673, "Cupertino":4561, "Los Gatos":11234, "Mountain View":12739, "Santa Clara":17675, "Saratoga":17960, "Sunnyvale":19457}

kHttpOk = 200


# return response code and content as a string
# when exception happens, we fail to get any response from the server. 
# so the response object is not initialized, and there is no status code.
# -1 is returned.
def get_response(url):
    try:
        # timeout for connect() and read(), in seconds
        response = requests.get(url, headers=kHeaders, verify=False, timeout=(4, 10))
    except requests.exceptions.Timeout:
        # TODO: Maybe set up for a retry, or continue in a retry loop
        print "query", url, "timeout"
        return -1, ""
    except requests.exceptions.RequestException as e:
        # TODO: Maybe better logic to handle exceptions
        print url, "error:", e
        return -1, ""
    return response.status_code, response.text


def create_query_url(max_home_num, region_id, region_type, sold_within_days, home_type):
    """
    creates a url to query redfin api. the response is of csv format. an example:
https://www.redfin.com/stingray/api/gis-csv?al=1&market=sanfrancisco&num_homes=350&ord=redfin-recommended-asc&page_number=1&region_id=12204&region_type=6&sold_within_days=30&sp=true&status=9&uipt=1,3&v=8
    explanation of some parameters:
    num_homes: max number of homes returned in the response
    region_id: the redfin id of a geo place. in the above example, 12204 refers to Milpitas
    region_type: the type of a geo place. in the above example, 6 refers to city
    sold_within_days: self explained
    uipt: type of homes. in the above example, 1 refers to single family, 3 refers to town house. also 2 refers condo
    """
    return kSoldQueryTemplate.substitute(max_home_num=max_home_num, region_id=region_id, region_type=region_type, sold_within_days=sold_within_days, home_type=home_type)


# helper function to get a string representation of current time. used for appending current time to file/folder names
def current_time_to_str():
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


# helper function to get the csv file name that stores the home list of the given city
def get_home_list_csv_name(city_name):
    return str(kCityNameRedfinIdMap[city_name]) + ".csv"


# helper function to get the redfin id of a given home url, i.e., the last part of url    
def get_home_id(home_url):
    return home_url.split('/')[-1]


# download sold home lists for each city in kCityNameRedfinIdMap, and saved them as csv files to the given directory
def download_sold_home_list(sold_within_days, saved_dir):
    # initialize the seed of random time generator
    random.seed(time.time())
    for one_city in kCityNameRedfinIdMap:
        print "downloading list for ", one_city
        query_url = create_query_url(max_home_num=kMaxHomeNum, region_id=kCityNameRedfinIdMap[one_city], region_type=kCityRegionType, sold_within_days=sold_within_days, home_type=kSingleAndTownHomeType)
        print "query url:", query_url
        
        code, content = get_response(query_url)
        if code != kHttpOk:
            print "query failed with code:", code, "got response:", response
        # TODO maybe better to also check response content is a valid csv file
        
        o_file_path = os.path.join(saved_dir, get_home_list_csv_name(one_city))
        print "save to", o_file_path
        with open(o_file_path, 'w') as o_file:
            o_file.write(content)
        
        # add a sleep time ranging from 10 - 30 seconds to mimic human behavior
        sleep_time_sec = 10 + random.random() * 20
        print "sleep for", sleep_time_sec, "seconds"
        sleep(sleep_time_sec)

    
kSoldHomesDirTemplate = Template("sold_in${days}days_from_${timestamp}")


# goes over each house listing file (csv) in the given directory, and downloads the page of each house listing,
# and saves the page with the name of the home's redfin id (last part of url).
def download_house_pages(dir_name):
    # states
    num_fail = 0
    num_skipped = 0
    total_urls = 0
    
    # timing
    t1 = time.time()
    
    for one_city in kCityNameRedfinIdMap:
        i_file_name = os.path.join(dir_name, get_home_list_csv_name(one_city))
        print "downloading homes in", i_file_name
        t, f, s = download_house_pages_in_file(dir_name, i_file_name)
        num_fail += f
        total_urls += t
        num_skipped += s
    
    print "total number of pages:", total_urls, "; pages failed:", num_fail, "; pages skipped:", num_skipped
    print "takes", (time.time() - t1) / 3600, "hours"


kUrlColumn = 20


# download the url pages of all home listings in the given file name, and save them in the given directory.
# home page are saved as the home's redfin id        
def download_house_pages_in_file(dir_name, file_name, skip_downloaded=True):
    url_list = []
    with open(file_name, 'rb') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if len(row) > kUrlColumn:
                # if the csv file is large, the end of the file will have some
                # comments. skip them.
                url_list.append(row[kUrlColumn])
        # remove the column header
        url_list = url_list[1:]
    print len(url_list), "pages to be downloaded"
    
    num_fail = 0
    num_skipped = 0
    # start download
    for one_url in url_list:
        home_id = get_home_id(one_url)
        o_file_name = os.path.join(dir_name, home_id)
        if skip_downloaded and os.path.isfile(o_file_name):
            print o_file_name, "is downloaded. skip."
            num_skipped += 1
            continue
        
        print "downloading:", one_url
        content = download_and_sleep(one_url)
        if not content:
            print "download failed for", one_url
            num_fail += 1
        else:
            with open(o_file_name, 'w') as o_file:
                # use the proper encoding
                o_file.write(content.encode('utf-8'))
                
    print "home list", file_name, "pages failed:", num_fail, "; pages skipped:", num_skipped
    return len(url_list), num_fail, num_skipped

        
# download a url and sleep for a while to mimic human behavior. 
# return downloaded content, return empty string when error occurs
def download_and_sleep(url):
    code, content = get_response(url)
    if code != kHttpOk:
        print url, ": returned with error:", code, content
        content = ""
    # TODO: might make sense to add retry logic here
    
    # adding a random sleep time to mimic human behavior
    sleep_time_sec = 5 + random.random() * 10
    print "sleeping for ", sleep_time_sec, "secs..."
    sleep(sleep_time_sec)    
    return content


if __name__ == "__main__":
    """
    # download sold homes data to a folder
    sold_within_days = 90
    
    dir_name = kSoldHomesDirTemplate.substitute(days=sold_within_days, timestamp=current_time_to_str())
    print "will save data to", dir_name
    
    os.mkdir(dir_name)
    download_sold_home_list(sold_within_days, dir_name)
    """
    
    download_house_pages("sold_in90days_from_2018-02-11-23-20-20")
    
