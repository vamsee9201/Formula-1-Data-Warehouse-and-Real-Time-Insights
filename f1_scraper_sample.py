#%%
import json
import pandas
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
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
#trying to get schedules
year = 2023
schedule_url = "https://www.formula1.com/en/results.html/{}/races.html".format(year)
response = requests.get(schedule_url)
soup = BeautifulSoup(response.text, 'html.parser')
grandPrixs = [element.text for element in soup.find('select',{"class":"resultsarchive-filter-form-select","name":"meetingKey"}).find('option').find_all_next('option')]
#%%
grandPrixs = [string.lower().replace(" ","-") for string in grandPrixs]

# %%
links = [element.get("data-value") for element in soup.find_all('a',{"class" : "resultsarchive-filter-item-link FilterTrigger","data-name":"meetingKey"})]
grandPrixs = [element.find('span').text.lower().replace(" ","-") for element in soup.find_all('a',{"class" : "resultsarchive-filter-item-link FilterTrigger","data-name":"meetingKey"})]
#soup.find_all('a',{"class" : "resultsarchive-filter-item-link FilterTrigger","data-name":"meetingKey"})
# %%
links = ["https://www.formula1.com/en/results.html/{}/races/{}/race-result.html".format(year,link) for link in links]
links
#%%
scheduleData = {
    "grandPrix":grandPrixs,
    "link":links
}
schedule = pd.DataFrame(scheduleData)
# %%
print(schedule['link'][0])

# %%
linkList = schedule['link'].to_list()
#%%
scheduleResponse = requests.get(linkList[0])
scheduleSoup = BeautifulSoup(scheduleResponse.text, 'html.parser')
circuitInfo = scheduleSoup.find('span',{"class":"circuit-info"}).text
startDate = scheduleSoup.find('span',{"class":"start-date"}).text
endDate = scheduleSoup.find('span',{"class":"full-date"}).text
# %%
date_format = "%d %b %Y"
endDateTime = datetime.strptime(endDate,date_format)
monthYear = endDateTime.strftime("%b %Y")
startDate = startDate + " {}".format(monthYear)
startDateTime = datetime.strptime(startDate,date_format)
# %%
