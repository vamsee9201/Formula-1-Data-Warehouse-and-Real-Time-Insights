#%%
import requests
import pytz
import pandas as pd
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime,timezone
import time
import numpy as np
from google.oauth2 import service_account
#%%
#getting lap times data
credentials = service_account.Credentials.from_service_account_file(
    'formula1_key.json',
)
sql = """
SELECT * FROM `formulaonedw.f1Results.lapTimes` 
"""
existingLapTimes = pandas_gbq.read_gbq(sql, 'formulaonedw',credentials=credentials)
#%%
print(existingLapTimes)
# %%
existingLapTimes = existingLapTimes[['year','round']]
print(existingLapTimes)
# %%
existingSR = existingLapTimes.copy()
existingSR.drop_duplicates(inplace = True)
existingSR.reset_index(drop=True,inplace=True)
print(existingSR)
# %%

#getting race schedule data
sql = """
SELECT * FROM `formulaonedw.f1Entities.raceDetails` 
"""
raceSchedule = pandas_gbq.read_gbq(sql, "formulaonedw",credentials=credentials)
print(raceSchedule)
# %%
raceSchedule = raceSchedule[['year','round','date']]
print(raceSchedule)
# %%
existingSR['year'] = existingSR['year'].astype(str)
existingSR['round'] = existingSR['round'].astype(str)
#%%
merged = raceSchedule.merge(existingSR,on = ['year','round'],how = "left",indicator=True)
print(merged)
# %%
merged = merged[merged['_merge'] == "left_only"]
print(merged)
#%%
merged.reset_index(drop=True,inplace=True)
# %%
cdt_timezone = pytz.timezone('America/Chicago')
utc_timezone = pytz.utc
# Convert CDT time to UTC
current_time_utc = cdt_timezone.localize(datetime.now()).astimezone(utc_timezone)
current_time_utc = pd.Timestamp(current_time_utc)
#%%
merged = merged[merged['date'] < current_time_utc]
merged.reset_index(drop = True, inplace=True)
print(merged)
# %%
#need to get the number of laps for each of these races
def getTotalLaps(year,round):
    time.sleep(2)
    url = "http://ergast.com/api/f1/{}/{}/results.json".format(year, round)
    payload={}
    headers = {}
    print("adding no of laps >>>")
    response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.text)
    totalLapsJson = data['MRData']['RaceTable']['Races'][0]['Results']
    totalLaps = pd.DataFrame(totalLapsJson)
    totalLaps = totalLaps[['laps','status']]
    totalLaps = totalLaps[totalLaps['status'] == "Finished"]['laps'][0]
    print(totalLaps)
    return totalLaps
#%%
merged['laps'] = merged.apply(lambda row: getTotalLaps(row['year'], row['round']), axis=1)
print(merged)
# %%
