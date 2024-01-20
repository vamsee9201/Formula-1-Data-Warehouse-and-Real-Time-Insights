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
#%%
sql = """
SELECT * FROM `formulaonedw.f1Entities.raceDetails` 
"""
raceDetails = pandas_gbq.read_gbq(sql, "formulaonedw",credentials=credentials)
print(raceDetails)
# %%
print(type(raceDetails['year'][0]))
print(type(raceDetails['round'][0]))
# %%
raceDetails['year'] = raceDetails['year'].astype(int)
raceDetails['round'] = raceDetails['round'].astype(int)
# %%
print(raceDetails)
# %%
print(type(raceDetails['year'][0]))
print(type(raceDetails['round'][0]))
# %%
raceDetails = pandas_gbq.to_gbq(raceDetails,"f1Entities.raceDetails","formulaonedw",credentials=credentials)

# %%
sql = """
SELECT * FROM `formulaonedw.f1Results.pitStops` 
"""
pitStops = pandas_gbq.read_gbq(sql, "formulaonedw",credentials=credentials)
#%%
print(pitStops)
# %%
pitStops['duration'] = pitStops['duration'].astype(float)
print(pitStops)
# %%
sql = """
SELECT * FROM `formulaonedw.f1Results.raceResults` 
"""
# %%
raceResults = pandas_gbq.read_gbq(sql, "formulaonedw",credentials=credentials)
# %%
print(raceResults)

# %%
raceResults['statusId'] = raceResults['statusId'].astype(str)
#%%
raceResults['fastestLapSpeed'] = raceResults['fastestLapSpeed'].astype(float)
#%%
print(raceResults)

# %%
def convertToInteger(str):
    try :
        return int(str)
    except :
        return None

# %%
raceResults['position'] = raceResults['position'].apply(convertToInteger)


# %%
print(raceResults)
# %%
raceResults.rename(columns={'seconds':'duration'},inplace=True)
print(raceResults)
# %%
pandas_gbq.to_gbq(raceResults,"f1Results.raceResults","formulaonedw",credentials=credentials)
#%%

