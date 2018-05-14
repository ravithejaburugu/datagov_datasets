# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 15:50:27 2018

@author: RAVITHEJA
"""

import requests
import json
import urllib2
import os.path
import pandas as pd
from time import sleep
from config import mongo_config  # argument_config
from MongodbConnector import mongodbConnector


root_folder = os.path.abspath(
                os.path.join(
                os.path.dirname('__file__'), '',
                'datagov_source_datasets'))

try:
    mongo = mongodbConnector()
except:
    raise


def fetchGovData(domain, dataset_count, datasets_colln):
    url = "http://catalog.data.gov/api/3/action/package_list?" + \
          "q=" + domain + "&rows=" + str(dataset_count)
    r = requests.get(url)

    json_resp = r.json()
    results = json_resp["result"]["results"]

    datasets_count = datasets_colln.count()

    if datasets_count > 0:
        mongo.bulk_mongo_update(datasets_colln, results)
    else:
        mongo.bulk_mongo_insert(datasets_colln, results)


def extractFromJSON(domain, datasets_colln):
    datasets_cursor = datasets_colln.find()

    for dataset in datasets_cursor:
        
        os.chdir(root_folder)
        if not os.path.isdir(domain):
            os.mkdir(domain)
        os.chdir(domain)
        
        dataset_name = dataset["name"][:60]

        if not os.path.isdir(dataset_name):
            os.mkdir(dataset_name)
        os.chdir(dataset_name)

        for res in dataset["resources"]:
            res_format = res["format"]
            res_url = res["url"]

            file_name = (dataset_name + "." + res_format).lower()
            print file_name

            try:
                if not os.path.isfile(file_name):
                    print "Downloading..."
                    sleep(3)
                    resp = urllib2.urlopen(res_url)
                    resp_content = resp.read()

                    print "Writing..."
                    with open(file_name, 'wb') as res_file:
                        res_file.write(resp_content)
            except:
                # raise
                print "Errorrrrrr..."
                continue


def main():

    datasets_colln = mongo.initialize_mongo('datasets')

    domains = ["health"]  # , "finance"]

    for d in domains:
        # fetchGovData(d, 1000, datasets_colln)
        extractFromJSON(d, datasets_colln)


if __name__ == "__main__":
    main()
