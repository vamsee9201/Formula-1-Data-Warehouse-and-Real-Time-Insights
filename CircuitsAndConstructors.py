#%%
import requests
import pandas as pd
import json
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime
import time
import numpy as np
#%%

url = "http://ergast.com/api/f1/2023/circuits.json"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = json.loads(response.text)
print(data)
# %%
print(type(data))
# %%
circuitJson = data['MRData']['CircuitTable']['Circuits']
print(circuitJson)
# %%
circuits = pd.DataFrame(circuitJson)
# %%
print(circuits)

# %%
circuits = pd.DataFrame()

for year in range(2014,2023):
    url = "http://ergast.com/api/f1/{}/circuits.json".format(year)
    payload={}
    headers = {}
    #response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.text)
    circuitJson = data['MRData']['CircuitTable']['Circuits']
    circuitsYear = pd.DataFrame(circuitJson)
    circuits = pd.concat([circuits,circuitsYear],axis = 0,ignore_index=True)
    time.sleep(3)
    print("year added {}".format(year))
    print(circuits)


    
# %%
print(circuits)
# %%
def extract_lat(dict):
    return dict['lat']

def extract_lng(dict):
    return dict['long']

def extract_locality(dict):
    return dict['locality']

def extract_country(dict):
    return dict['country']

circuits['lat'] = circuits['Location'].apply(extract_lat)
circuits['lng'] = circuits['Location'].apply(extract_lng)
circuits['location'] = circuits['Location'].apply(extract_locality)
circuits['country'] = circuits['Location'].apply(extract_country)

print(circuits)
# %%

circuits.drop(columns= ['url','Location'],inplace=True)
print(circuits)

# %%
circuitsDump = circuits.drop_duplicates()
print(circuitsDump)

# %%
circuitsDump.reset_index(inplace=True)
print(circuitsDump)
#%%
circuitsDump['lat'] = circuitsDump['lat'].astype(float)
circuitsDump['lng'] = circuitsDump['lng'].astype(float)
print(type(circuitsDump['lat'][0]))
#%%
circuitsDump.drop(columns=['index'],inplace=True)
print(circuitsDump)

# %%
#pandas_gbq.to_gbq(circuitsDump, "f1Entities.circuitDetails", project_id="formulaonedw",if_exists='append')

# %%
#constructors

url = "http://ergast.com/api/f1/2023/constructors.json"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = json.loads(response.text)
print(data)
# %%
constructors = data['MRData']['ConstructorTable']['Constructors']
constructors = pd.DataFrame(constructors)
print(constructors)
#%%

constructors = pd.DataFrame()
for year in range(2014,2023):
    url = "http://ergast.com/api/f1/{}/constructors.json".format(year)
    payload={}
    headers = {}
    #response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.text)
    constructorsJson = data['MRData']['ConstructorTable']['Constructors']
    constructorsYear = pd.DataFrame(constructorsJson)
    constructors = pd.concat([constructors,constructorsYear],axis=0,ignore_index=True)
    time.sleep(3)

print(constructors)
# %%
constructors.drop(columns=['url'],inplace=True)
constructors.drop_duplicates(inplace=True)
constructors.reset_index(drop=True)
print(constructors)

# %%
constructors.reset_index(drop=True,inplace=True)
print(constructors)


# %%
pandas_gbq.to_gbq(constructors, "f1Entities.constructorDetails", project_id="formulaonedw",if_exists='append')


# %%
