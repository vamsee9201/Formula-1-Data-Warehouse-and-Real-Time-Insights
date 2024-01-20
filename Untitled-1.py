
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

url = "http://ergast.com/api/f1/2014.json"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = json.loads(response.text)
#%%

print(data)

# %%
yearData = data['MRData']['RaceTable']['Races']
print(yearData)
# %%
df = pd.DataFrame(yearData)
# %%
print(df)
"""
def deriveCircuit(dict):
    return dict['circuitId']
df['Circuit'] = df['Circuit'].apply(deriveCircuit)
print(df)
"""
# %%


raceDump = pd.DataFrame()
for i in range(2014,2023):
    y = str(i)
    url = "http://ergast.com/api/f1/{}.json".format(y)
    payload={}
    headers = {}
    #response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.text)
    yearData = data['MRData']['RaceTable']['Races']
    yearDf = pd.DataFrame(yearData)
    raceDump = pd.concat([raceDump,yearDf],ignore_index=True)
    time.sleep(3)

#%%
print(raceDump)
#%%
#x = raceDump['SecondPractice'][177]
#print(x)
#print(type(x))
def deriveDateTime(dict):
    try:
        time_str = dict['date'] + " " + dict['time']
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%SZ")
    except:
        return np.nan
    
#%%
temp = raceDump['FirstPractice'].apply(deriveDateTime)
print(temp)
#%%
print(temp)
for element in raceDump['FirstPractice'].to_list():
    if isinstance(element,float):
        print(element)
#%%
print(raceDump.columns)
uList = ['FirstPractice','SecondPractice','ThirdPractice','Qualifying','Sprint']

raceDumpU = raceDump.copy()
#%%
print(raceDumpU)
#%%
for column in uList:
    raceDumpU[column] = raceDumpU[column].apply(deriveDateTime)
print(raceDumpU)
#%%

#raceDumpU['date'] = raceDumpU['date'] + ' ' + raceDumpU['time']
print(raceDumpU)

def deriveDateTime2(str):
    try:
        
        return datetime.strptime(str, "%Y-%m-%d %H:%M:%SZ")
    except:
        return np.nan
    
#%%
#raceDumpU['date'] = raceDumpU['date'].apply(deriveDateTime2)
print(raceDumpU)
def extractCircuit(dict):
    return dict['circuitId']

raceDumpU['Circuit'] = raceDumpU['Circuit'].apply(extractCircuit)
#%%
raceDumpU.drop(columns=['url','raceName','time'],inplace=True)
#%%
raceDumpU.rename(columns={'season':'year','Circuit':'circuitId'},inplace=True)
print(raceDumpU)
# %%
#pandas_gbq.to_gbq(raceDumpU, "f1Entities.raceDetails", project_id="formulaonedw",if_exists='append')



# %%

#face table data
constructors = pd.read_csv("f1db_csv/constructors.csv")
print(constructors)
drivers = pd.read_csv("f1db_csv/drivers.csv")
print(drivers)
races = pd.read_csv("f1db_csv/races.csv")
print(races)
raceResults = pd.read_csv("f1db_csv/results.csv")
print(raceResults)
# %%
print(raceResults)
# %%

results_races_merged = pd.merge(raceResults,races, on = "raceId",how = "inner")
print(results_races_merged)
#%%
results_races_merged = results_races_merged[['resultId', 'driverId', 'constructorId', 'number', 'grid',
       'position', 'positionText', 'positionOrder', 'points', 'laps', 'time_x',
       'milliseconds', 'fastestLap', 'rank', 'fastestLapTime',
       'fastestLapSpeed', 'statusId', 'year', 'round',]]
#%%
print(results_races_merged)


# %%
constructor_results_races_merged = pd.merge(results_races_merged,constructors,on = "constructorId",how = "inner")
print(constructor_results_races_merged)
# %%
print(constructor_results_races_merged.columns)

# %%
constructor_results_races_merged = constructor_results_races_merged[['resultId', 'driverId', 'number', 'grid', 'position',
       'positionText', 'positionOrder', 'points', 'laps', 'time_x',
       'milliseconds', 'fastestLap', 'rank', 'fastestLapTime',
       'fastestLapSpeed', 'statusId', 'year', 'round', 'constructorRef',
       ]]
print(constructor_results_races_merged)
# %%
drivers_constructors_results_races_merged = pd.merge(constructor_results_races_merged,drivers,on = "driverId",how = "inner")
print(drivers_constructors_results_races_merged)
# %%
#print(drivers_constructors_results_races_merged.columns)
drivers_constructors_results_races_merged = drivers_constructors_results_races_merged[['resultId', 'driverId', 'number_x', 'grid', 'position', 'positionText',
       'positionOrder', 'points', 'laps', 'time_x', 'milliseconds',
       'fastestLap', 'rank', 'fastestLapTime', 'fastestLapSpeed', 'statusId',
       'year', 'round', 'constructorRef', 'driverRef','number_y']]
print(drivers_constructors_results_races_merged)
# %%
all_merged = drivers_constructors_results_races_merged.copy()
# %%
all_merged = all_merged[['year','round','number_y','constructorRef','grid','position','positionText','points','laps','statusId','milliseconds','fastestLap','fastestLapTime','fastestLapSpeed']]
print(all_merged)
# %%
all_merged.rename(columns={'number_y':'driverId','constructorRef':'constructorId'},inplace=True)
print(all_merged)
# %%
results_2014To2022 = all_merged[ (all_merged['year'] >= 2014) & (all_merged['year'] < 2023)]

# %%
print(results_2014To2022)
# %%
results_2014To2022.reset_index(drop=True,inplace=True)
print(results_2014To2022)
# %%
results_2014To2022.replace(r"\N",np.nan,inplace=True)
print(results_2014To2022)
# %%
columns = results_2014To2022.columns
for column in columns:
    print(column)
    print(type(results_2014To2022[column][0]))
   
# %%

#milliseconds : need to convert this to seconds and change the name of the column
#fasttestLapTime : str -> seconds

resultsDump = results_2014To2022.copy()
print(resultsDump)

# %%
x = resultsDump['fastestLapTime']
# %%
def deriveSeconds(str):
    try :
        str,seconds_str = str.split(':')
        seconds = int(str)*60 + float(seconds_str)
        return seconds
    except :
        return np.nan
print(deriveSeconds("1:43.066"))
# %%
resultsDump['fastestLapTime'] = resultsDump['fastestLapTime'].apply(deriveSeconds)
print(resultsDump)
# %%
resultsDump['milliseconds'] = resultsDump['milliseconds'].astype(float)
print(resultsDump)
# %%
resultsDump['milliseconds'] = resultsDump['milliseconds']/1000
print(resultsDump)
# %%
resultsDump.rename(columns= {'milliseconds':'seconds'},inplace=True)
print(resultsDump)

# %%
#pandas_gbq.to_gbq(resultsDump, "f1Results.raceResults", project_id="formulaonedw",if_exists='append')

# %%
print("hello")
# %%
#Qualifying data cleaning

qualiResults = pd.read_csv("/Users/vamseekrishna/Desktop/Portfolio/DE projects/F1 project/f1db_csv/qualifying.csv")
print(qualiResults)
# %%
results_quali_merged = pd.merge(qualiResults,races, on = "raceId",how = "inner")
print(results_quali_merged)

# %%
print(results_quali_merged.columns)
# %%
results_quali_merged = results_quali_merged[['qualifyId', 'raceId', 'driverId', 'constructorId', 'number',
       'position', 'q1', 'q2', 'q3', 'year', 'round']]
print(results_quali_merged)
# %%
drivers_results_quali_merged = pd.merge(results_quali_merged,drivers,on = "driverId",how = "inner")
print(drivers_results_quali_merged)
# %%
drivers_results_quali_merged = drivers_results_quali_merged[['qualifyId', 'constructorId', 
       'position', 'q1', 'q2', 'q3', 'year', 'round','number_y']]
print(drivers_results_quali_merged)
# %%
constructors_drivers_results_quali_merged = pd.merge(drivers_results_quali_merged,constructors,on = "constructorId",how = "inner")
print(constructors_drivers_results_quali_merged)
# %%
print(constructors_drivers_results_quali_merged.columns)
# %%
constructors_drivers_results_quali_merged = constructors_drivers_results_quali_merged[['year',
       'round', 'number_y',  'constructorRef','position', 'q1', 'q2', 'q3']]
print(constructors_drivers_results_quali_merged)
# %%
time_columns = ['q1','q2','q3']
for column in time_columns:
    constructors_drivers_results_quali_merged[column] = constructors_drivers_results_quali_merged[column].apply(deriveSeconds)
print(constructors_drivers_results_quali_merged)
# %%
qualiDump = constructors_drivers_results_quali_merged.copy()
qualiDump = qualiDump[(qualiDump['year'] >= 2014) & (qualiDump['year'] < 2023)]
print(qualiDump)
# %%
qualiDump.reset_index(drop=True,inplace=True)
print(qualiDump)
 #%%
columns = qualiDump.columns
for column in columns:
    print(column)
    print(type(qualiDump[column][150]))

# %%
qualiDump.rename(columns={'number_y':'driverId','constructorRef':'constructorId'},inplace=True)
print(qualiDump)
# %%
#pandas_gbq.to_gbq(qualiDump, "f1Results.qualiResults", project_id="formulaonedw",if_exists='append')
# %%
#Lap times

lap_times = pd.read_csv("/Users/vamseekrishna/Desktop/Portfolio/DE projects/F1 project/f1db_csv/lap_times.csv")

print(lap_times)
# %%
races_lap_times_merged = pd.merge(lap_times,races,on = "raceId",how = "inner")
print(races_lap_times_merged)
# %%
print(races_lap_times_merged.columns)
# %%
races_lap_times_merged = races_lap_times_merged[['raceId', 'driverId', 'lap', 'position', 'time_x', 'milliseconds',
       'year', 'round']]
print(races_lap_times_merged)
# %%
drivers_races_lap_times_merged = pd.merge(races_lap_times_merged,drivers,on = "driverId",how = "inner")
print(drivers_races_lap_times_merged)
# %%
print(drivers_races_lap_times_merged.columns)
# %%
drivers_races_lap_times_merged = drivers_races_lap_times_merged[['year', 'round',  'number','lap', 'position', 'milliseconds',]]
print(drivers_races_lap_times_merged)
# %%
lapTimesDump = drivers_races_lap_times_merged.copy()
# %%
lapTimesDump = lapTimesDump[(lapTimesDump['year'] >=2014) & (lapTimesDump['year'] < 2023)]
print(lapTimesDump)
# %%
lapTimesDump.reset_index(drop=True,inplace=True)
lapTimesDump['milliseconds'] = lapTimesDump['milliseconds']/1000

print(lapTimesDump)
# %%
lapTimesDump.rename(columns= {'number':'driverId','milliseconds':'seconds'},inplace=True)

# %%
print(lapTimesDump)
# %%
#pandas_gbq.to_gbq(lapTimesDump, "f1Results.lapTimes", project_id="formulaonedw",if_exists='append')


# %%
#pit stops
pitStops = pd.read_csv("f1db_csv/pit_stops.csv")
print(pitStops)
# %%
races_pitStops_merged = pd.merge(pitStops,races,on = "raceId",how = "inner")
print(races_pitStops_merged)
# %%
print(races_pitStops_merged.columns)
# %%
races_pitStops_merged = races_pitStops_merged[[ 'driverId', 'stop', 'lap','milliseconds',
       'year', 'round']]
print(races_pitStops_merged)
# %%
drivers_races_pitStops_merged = pd.merge(races_pitStops_merged,drivers,on = "driverId",how = "inner")
print(drivers_races_pitStops_merged.columns)
# %%
drivers_races_pitStops_merged = drivers_races_pitStops_merged[['year', 'round','number','stop', 'lap', 'milliseconds', 
       ]]
print(drivers_races_pitStops_merged)
# %%
pitStopsDump = drivers_races_pitStops_merged.copy()
print(pitStopsDump)
# %%
pitStopsDump = pitStopsDump[(pitStopsDump['year'] >= 2014) & (pitStopsDump['year'] < 2023)]
print(pitStopsDump)
# %%
pitStopsDump.reset_index(drop=True,inplace=True)
# %%
pitStopsDump['milliseconds'] = pitStopsDump['milliseconds']/1000
print(pitStopsDump)
#%%
pitStopsDump.rename(columns={'seconds':'duration'},inplace=True)
#pandas_gbq.to_gbq(pitStopsDump, "f1Results.pitStops", project_id="formulaonedw",if_exists='append')

# %%
print(raceDumpU)

# %%
