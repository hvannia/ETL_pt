import json
from pprint import pprint
import pandas as pd
import os
import pymysql  #needed by sqlalchemy
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()

import etlUtilities as etlU
json_data=etlU.load_json_file()

mortality_engine=etlU.connect_mortality()

keepcols=[
 'age_recode_52',
 'age_recode_27',
 'age_recode_12',
 'infant_age_recode_22']

#CREATE A TABLE PER CODE TYPE in keepcols
for k in keepcols:
    q_cscodes= " CREATE TABLE  IF NOT EXISTS "+k+"( ckey VARCHAR(255) NOT NULL PRIMARY KEY, cvalue VARCHAR(1024) ); "
    mortality_engine.execute(q_cscodes)  

#CREATE PERSON TABLE 
q_cperson="CREATE TABLE  IF NOT EXISTS person( id INT PRIMARY KEY NOT NULL, "
for c in keepcols:
    q_cperson+=("fk_"+c+ " VARCHAR(255) DEFAULT '', ")
    q_cperson+=" CONSTRAINT cfk_"+c+" FOREIGN KEY (fk_"+c+") REFERENCES "+c+"(ckey) ON DELETE NO ACTION ON UPDATE CASCADE,"
q_cperson=q_cperson[:-1]
q_cperson+=");"
mortality_engine.execute(q_cperson)


# CREATE MAPPED PERSON TABLE ( map values instead of codes )
allcols=['resident_status',
 'education_1989_revision',
 'education_2003_revision',
 'education_reporting_flag',
 'month_of_death',
 'sex',
 'detail_age_type',
 'detail_age',
 'age_recode_52',
 'age_recode_27',
 'age_recode_12',
 'infant_age_recode_22',
 'place_of_death_and_decedents_status',
 'marital_status',
 'day_of_week_of_death',
 'current_data_year',
 'injury_at_work',
 'manner_of_death',
 'method_of_disposition',
 'autopsy',
 'activity_code',
 'place_of_injury_for_causes_w00_y34_except_y06_and_y07_',
 'icd_code_10th_revision',
 '358_cause_recode',
 '113_cause_recode',
 '130_infant_cause_recode',
 '39_cause_recode',
 'number_of_entity_axis_conditions',
 'entity_condition_1',
 'entity_condition_2',
 'number_of_record_axis_conditions',
 'record_condition_1',
 'record_condition_2',
 'race',
 'bridged_race_flag',
 'race_imputation_flag',
 'race_recode_3',
 'race_recode_5',
 'hispanic_origin',
 'hispanic_originrace_recode']

q_fullperson="CREATE TABLE  IF NOT EXISTS mapped_person( id INT PRIMARY KEY NOT NULL, "
for k in allcols:
    q_fullperson+= k+" VARCHAR(1024), "
q_fullperson=q_fullperson[:-2]
q_fullperson+=");"