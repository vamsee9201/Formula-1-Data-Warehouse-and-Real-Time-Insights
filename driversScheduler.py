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
print("retrieving credentials for the big query account >>>")
time.sleep(1)
credentials = service_account.Credentials.from_service_account_file(
    'formula1_key.json',
)
print(credentials)
time.sleep(1)
#%%
print("getting current year >>>")
time.sleep(1)
current_year = datetime.now().year
print(current_year)
time.sleep(1)
#%%
print("creating url for the current year >>>")
time.sleep(1)
url = "http://ergast.com/api/f1/{}/drivers.json".format(current_year)
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
print("getting relevant data from json response >>>")
time.sleep(1)
driversJson = data['MRData']['DriverTable']['Drivers']
print(driversJson)
time.sleep(1)
# %%
print("converting json data into data frame >>>")
time.sleep(1)
drivers = pd.DataFrame(driversJson)
print(drivers)
time.sleep(1)
# %%
print("dropping the url column >>>")
time.sleep(1)
drivers.drop(columns=['url'],inplace=True)
print(drivers)
time.sleep(1)
# %%
print("converting driver date of birth to datetime format >>>")
time.sleep(1)
format = "%Y-%m-%d"
drivers['dateOfBirth'] = pd.to_datetime(drivers['dateOfBirth'],format="%Y-%m-%d")
print(drivers)
time.sleep(1)
# %%
print("transforming the driverId column >>>")
time.sleep(1)
drivers.drop(columns=['driverId'], inplace=True)
drivers.rename(columns={'permanentNumber':'driverId'},inplace=True)
time.sleep(1)
#%%
print("fetching data from big query to check with the current data >>>")
time.sleep(1)
sql = """
SELECT * FROM `formulaonedw.f1Entities.driverDetails` 
"""
existingDrivers = pandas_gbq.read_gbq(
    sql,
    project_id="formulaonedw",
    credentials=credentials
    
    # Set the dialect to "legacy" to use legacy SQL syntax. As of
    # pandas-gbq version 0.10.0, the default dialect is "standard".
)
print(existingDrivers)
time.sleep(1)
#%%
print("merging both current and already existing data >>>>")
time.sleep(1)
mergedDrivers = drivers.merge(existingDrivers, on = "driverId",how = "left",indicator=True)
mergedDrivers.drop(columns=['code_y','givenName_y','familyName_y','dateOfBirth_y','nationality_y'],inplace=True)
mergedDrivers.rename(columns={'code_x':'code','givenName_x':'givenName','familyName_x':'familyName','dateOfBirth_x':'dateOfBirth','nationality_x':'nationality'},inplace=True)
print(mergedDrivers)
time.sleep(1)
# %%
print("checking for data that is not present before >>>>")
appender = mergedDrivers[mergedDrivers['_merge'] == "left_only"]
# %%
if appender.empty:
    print("no data to append. script ran successfully ....")
else :
    appender.drop(columns=['_merge'],inplace=True)
    print("data found for appending >>>")
    print(appender)
    print("appending data to relevant table >>>")
    pandas_gbq.to_gbq(appender, "f1Entities.driverDetails", project_id="formulaonedw",if_exists='append',credentials=credentials)
    time.sleep(1)
    print("finished appending, script ran successfully")
