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
url = "http://ergast.com/api/f1/{}.json".format(current_year)
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
#%%
print("extracting relevant data from the response text >>>")
time.sleep(1)
racesJson = data['MRData']['RaceTable']['Races']
print(racesJson)
time.sleep(1)
# %%
print("converting json data into data frame >>>")
time.sleep(1)
races = pd.DataFrame(racesJson)
print(races)
time.sleep(1)
# %%
print("dropping url column >>>")
time.sleep(1)
races.drop(columns=['url'],inplace=True)
time.sleep(1)
# %%
#these functions will derive date time from the dictionary and just the date time string respectively
def deriveDateTime(dict):
    try:
        time_str = dict['date'] + " " + dict['time']
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%SZ")
    except:
        return np.nan
    
def deriveDateTime2(time_str):
    try:
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%SZ")
    except:
        return np.nan
#%%
print("extracting the date time from dict type columns >>>")
time.sleep(1)
columns = ['FirstPractice','SecondPractice','ThirdPractice','Qualifying','Sprint']
for column in columns:
    races[column] = races[column].apply(deriveDateTime)
print(races)
time.sleep(1)
# %%
print("creating date time column using race date and time >>>")
time.sleep(1)
races['date'] = races['date'] + " " + races['time']
races.drop(columns=['time'],inplace=True)
#%%
races['date'] = races['date'].apply(deriveDateTime2)
print(races)
time.sleep(1)
# %%
print("deriving the circuit ID from the Circuit column >>>")
time.sleep(1)
def deriveCircuit(dict):
    try :
        return dict['circuitId']
    except :
        return np.nan
races['Circuit'] = races['Circuit'].apply(deriveCircuit)
races.rename(columns={'Circuit':'circuitId'},inplace=True)
print(races)
time.sleep(1)
# %%
print("dropping the raceName column >>>")
time.sleep(1)
races.drop(columns=['raceName'],inplace=True)
print(races)
time.sleep(1)
#%%
print("renamiing the season column to year >>>")
time.sleep(1)
races.rename(columns= {'season':'year'},inplace=True)
time.sleep(1)
# %%
races['year'] = races['year'].astype(int)
races['round'] = races['round'].astype(int)
#%%
print('getting data about existing race schedules >>>')
time.sleep(1)
sql = """
SELECT * FROM `formulaonedw.f1Entities.raceDetails` 
"""
existingRaces = pandas_gbq.read_gbq(sql, "formulaonedw",credentials=credentials)
print(existingRaces)
time.sleep(1)
# %%
print("getting data that is not present >>>")
time.sleep(1)
mergedRaces = races.merge(existingRaces,on = ['year','round'], how = "left",indicator=True)
mergedRaces.rename(columns = {'circuitId_x':'circuitId','date_x':'date','FirstPractice_x':'FirstPractice','SecondPractice_x':'SecondPractice','ThirdPractice_x':'ThirdPractice','Qualifying_x':'Qualifying','Sprint_x':'Sprint'},inplace=True)
mergedRaces.drop(columns=['circuitId_y','date_y','FirstPractice_y','SecondPractice_y','ThirdPractice_y','Qualifying_y','Sprint_y'],inplace=True)
# %%
appender = mergedRaces[mergedRaces['_merge'] == "left_only"]
print(appender)
time.sleep(1)
# %%
if appender.empty:
    print("script executed successsfully, no data to append")
else :
    print("data to append found. Adding data to big query >>>")
    time.sleep(1)
    appender.drop(columns=['_merge'],inplace=True)
    pandas_gbq.to_gbq(appender, "f1Entities.raceDetails", project_id="formulaonedw",if_exists='append',credentials=credentials)
    print("appended data to big query >>>")
    time.sleep(1)