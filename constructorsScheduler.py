#%%
import requests
import pandas as pd
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime
import time
import numpy as np
from google.oauth2 import service_account
#%%
credentials = service_account.Credentials.from_service_account_file(
    'formula1_key.json',
)
print(credentials)
#%%
print("getting current year >>>")
time.sleep(1)
current_year = datetime.now().year
print(current_year)
time.sleep(1)
#%%
print("creating url for the current year >>>")
time.sleep(1)
url = "http://ergast.com/api/f1/{}/constructors.json".format(current_year)
print(url)
time.sleep(1)
#%%
payload={}
headers = {}
print("getting data from the api >>>")
response = requests.request("GET", url, headers=headers, data=payload)
time.sleep(2)
data = json.loads(response.text)
print(data)
time.sleep(1)
# %%
print("extracting relevant data from json response >>>")
time.sleep(1)
constructorsJson = data['MRData']['ConstructorTable']['Constructors']
print(constructorsJson)
time.sleep(1)
# %%
print("converting data into dataframe >>>")
time.sleep(1)
constructors = pd.DataFrame(constructorsJson)
print(constructors)
time.sleep(1)
# %%
print("dropping url column from api data")
time.sleep(1)
constructors.drop(columns=['url'],inplace=True)
print(constructors)
time.sleep(1)
# %%
print("getting data from big query to perform checks >>>")
time.sleep(1)
sql = """
SELECT * FROM `formulaonedw.f1Entities.constructorDetails` 
"""
existingConstructors = pandas_gbq.read_gbq(
    sql,
    project_id="formulaonedw",
    credentials=credentials
    
    # Set the dialect to "legacy" to use legacy SQL syntax. As of
    # pandas-gbq version 0.10.0, the default dialect is "standard".
)
print(existingConstructors)
time.sleep(1)
# %%
print("checking for new columns >>>")
mergedConstructors = constructors.merge(existingConstructors, on = "constructorId",how = "left",indicator=True)
mergedConstructors.drop(columns=['name_y','nationality_y'],inplace=True)
mergedConstructors.rename(columns={'name_x':'name','nationality_x':'nationality'},inplace=True)
print(mergedConstructors)
# %%
appender = mergedConstructors[mergedConstructors['_merge'] == "left_only"]
print(appender)
time.sleep(1)
# %%
if appender.empty:
    print("no data to append , successfully executed")
else :
    print(" data found, appending data into warehouse >>>")
    appender.drop(columns=['_merge'],inplace=True)
    time.sleep(1)
    pandas_gbq.to_gbq(appender, "f1Entities.constructorDetails", project_id="formulaonedw",if_exists='append',credentials=credentials)
    time.sleep(1)
#%%