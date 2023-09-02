
#%%
import requests
import pandas as pd
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime
import time
#%%

#%%
"""
key_path = '/Users/vamseekrishna/Desktop/Portfolio/DE projects/F1 project/formula1_key.json'
project_id = 'formulaonedw'



pandas_gbq.to_gbq(df, table_id, project_id=project_id,if_exists='append')
"""
#%%
#requesting data from the ergast api
url = "http://ergast.com/api/f1/2014/drivers.json"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = json.loads(response.text)
#%%

#accessing the driver elements and creating the driver details df, dropping url column
print(data)
driver_details = data['MRData']['DriverTable']['Drivers']
print(driver_details)
driver_details_df = pd.DataFrame(driver_details)
driver_details_df = driver_details_df.drop('url',axis= 1)
print(driver_details_df)
#%%
#dateString = "1981-07-29"

#parsedString = datetime.strptime(dateString,format)
#print(parsedString)
#print(type(parsedString))
#converting date of birth to datetime *
format = "%Y-%m-%d"
driver_details_df['dateOfBirth'] = pd.to_datetime(driver_details_df['dateOfBirth'],format="%Y-%m-%d")

# %%
#just checking type of date of birth after converting.
print(driver_details_df)
print(type(driver_details_df['dateOfBirth'][0]))

# %%
#droppiing the driverId column and replacing it with permanent number *
driver_details_df = driver_details_df.drop('driverId',axis= 1)
driver_details_df = driver_details_df.rename(columns= {'permanentNumber':"driverId"})
print(driver_details_df)
# %%
#appending the values to the driver details table
project_id = 'formulaonedw'

table_id = 'f1Entities.driverdDetails'
pandas_gbq.to_gbq(driver_details_df, table_id, project_id=project_id,if_exists='append')

# %%

project_id = 'formulaonedw'

table_id = 'f1Entities.driverDetails'
#%%
final_drivers_df = pd.DataFrame()
#%%
for i in range(2014,2023):
    y = str(i)
    url = "http://ergast.com/api/f1/{}/drivers.json".format(y)
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.text)
    print(data)
    print("accessing driver data elements:")
    driver_details = data['MRData']['DriverTable']['Drivers']
    print(driver_details)
    print("converting driver details into a data frame")
    driver_details_df = pd.DataFrame(driver_details)
    print('dropping url column')
    driver_details_df = driver_details_df.drop('url',axis= 1)
    print(driver_details_df)
    print('converting date of birth to datetime format')
    format = "%Y-%m-%d"
    driver_details_df['dateOfBirth'] = pd.to_datetime(driver_details_df['dateOfBirth'],format="%Y-%m-%d")
    print("dropping driverId and replacing it with permanenet number")
    driver_details_df = driver_details_df.drop('driverId',axis= 1)
    driver_details_df = driver_details_df.rename(columns= {'permanentNumber':"driverId"})
    print('adding data into driver details table in f1entities dataset')
    #pandas_gbq.to_gbq(driver_details_df, table_id, project_id=project_id,if_exists='append')
    final_drivers_df = pd.concat([final_drivers_df,driver_details_df],ignore_index=True)
    time.sleep(3)

#%%
#print(final_drivers_df)
#print(type(final_drivers_df['dateOfBirth'][0]))
final_drivers_df = final_drivers_df.drop_duplicates()
print(final_drivers_df)

#%%
final_drivers_df = final_drivers_df.reset_index(drop=True)
# %%
print(final_drivers_df)

# %%
pandas_gbq.to_gbq(final_drivers_df, table_id, project_id=project_id,if_exists='append')
# %%

""""
sql = """  
#SELECT * FROM `f1Entities.driverdDetails` LIMIT 1000
"""
df = pandas_gbq.read_gbq(
    sql,
    project_id="formulaonedw"
    
    # Set the dialect to "legacy" to use legacy SQL syntax. As of
    # pandas-gbq version 0.10.0, the default dialect is "standard".
    
)
print(df)
"""
#%%

