#%%
import json
import pandas
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
#%%
def getDriversData(year):
    drivers_url = "https://www.formula1.com/en/results.html/{}/drivers.html".format(year)
    drivers_url
    response = requests.get(drivers_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('tbody')
    first_names = [element.text for element in table.find_all(class_ = "hide-for-tablet")]
    last_names = [element.text for element in table.find_all(class_ = "hide-for-mobile")]
    driver_codes = [element.text for element in table.find_all(class_ = "uppercase hide-for-desktop")]
    driver_nationality = [element.text for element in table.find_all(class_ = "dark semi-bold uppercase")]
    driver_teams = [element.text for element in table.find_all(class_ = "grey semi-bold uppercase ArchiveLink")]
    drivers_data = {
    "firstName":first_names,
    "lastName":last_names,
    "driverId":driver_codes,
    "nationality":driver_nationality,
    "team":driver_teams,
    "year":year
    }
    drivers_df = pd.DataFrame(drivers_data)
    return drivers_df
#%%
drivers2018 = getDriversData(2023)
#%%

#getting a dataframe to backfill driver's data
driversBackFill = pd.DataFrame()
for year in range(2014,2024):
    driversForYear = getDriversData(year)
    print(driversForYear)
    driversBackFill = pd.concat([driversBackFill,driversForYear],ignore_index=True)
    time.sleep(3)
driversBackFill
    

#%%
#remove duplicates.
allDrivers = driversBackFill[['firstName','lastName','driverId','nationality']].drop_duplicates().reset_index(drop=True)
# %%
#get team details

allteams = driversBackFill[['year','team','driverId']]

# %%
