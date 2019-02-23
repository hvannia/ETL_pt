import json
from pprint import pprint
import pandas as pd
import os
import pymysql  #needed by sqlalchemy
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()

import etlUtilities as etlU
json_data = etlU.load_json_file()

mortality_engine = etlU.connect_mortality()

#LAODING pcodes TABLE WITH QUERIES  // add error handling
keepcols=[
 'age_recode_52',
 'age_recode_27',
 'age_recode_12',
 'infant_age_recode_22']
for k in keepcols:
    thistable=json_data.get(k)
    thiskeys=list(thistable.keys())
    thisvars=list(thistable.values())
    df=pd.DataFrame(thisvars,thiskeys, dtype=object)
    df.head()
    df.reset_index(inplace=True)
    df.columns=['ckey','cvalue']
    #print(df)
    df.to_sql(k, mortality_engine,  if_exists='append', index=False)



#PERSON TABLE with age data   // add error handling 

data_2015=etlU.getDataFile('2015')

df=pd.DataFrame({'fk_age_recode_52':list(data_2015['age_recode_52']), 
                 'fk_age_recode_27':list(data_2015['age_recode_27']),
                 'fk_age_recode_12':list(data_2015['age_recode_12']),
                 'fk_infant_age_recode_22':list(data_2015['infant_age_recode_22'])})
df.index.names=['id']
df.to_sql('person', mortality_engine,  if_exists='append', index=True)




#EOF
