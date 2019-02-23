# ETL UTILITIES #

import json
from pprint import pprint
import pandas as pd
import os
import pymysql  #needed by sqlalchemy
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()


# RETURN THE CONTENTS OF A JSON FILE, 
# path and file structure for ETL project, - 
# to adapt with variable file name & directory.
def load_json_file():
    file_base = 'source_data/mortality'
    file_year = '2015'
    file_name = '_codes.json'
    try:
        with open(file_base+"/"+file_year+file_name) as f:
            json_data = json.load(f)
    except:
        return "Error loading file "+ file_base+"/"+file_year+file_name
    return json_data


def getDataFile(year):
    file_base='source_data/mortality'
    file_year=str(year)
    dfile_name='_data.csv'
    data= pd.read_csv(file_base+"/"+file_year+dfile_name, nrows=10**1, dtype=object)
    return data



#CONNECT TO THE MORTALITY DB  AND USE
#to add error handling for no connection or no such db
def connect_mortality():
    engine = create_engine("mysql://root:myS@localhost:3306/mortality?charset=utf8mb4")
    #make sure to have exisitng database 
    q_use="USE mortality;"
    engine.execute(q_use)
    return engine