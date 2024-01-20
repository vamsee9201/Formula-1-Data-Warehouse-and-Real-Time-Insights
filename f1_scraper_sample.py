#%%
import json
import pandas
import requests
from bs4 import BeautifulSoup
import pandas as pd
#%%
year = 2014
url = "https://www.formula1.com/en/results.html/{}/races.html".format(year)
print(url)
# %%
response = requests.get(url)
# %%
drivers_url = "https://www.formula1.com/en/results.html/{}/drivers.html".format(year)
drivers_url
# %%
response = requests.get(drivers_url)
soup = BeautifulSoup(response.text, 'html.parser')

# %%
table = soup.find('tbody')
first_names = [element.text for element in table.find_all(class_ = "hide-for-tablet")]
last_names = [element.text for element in table.find_all(class_ = "hide-for-mobile")]
driver_codes = [element.text for element in table.find_all(class_ = "uppercase hide-for-desktop")]
driver_nationality = [element.text for element in table.find_all(class_ = "dark semi-bold uppercase")]
driver_teams = [element.text for element in table.find_all(class_ = "grey semi-bold uppercase ArchiveLink")]
# %%
drivers_data = {
"first_name":first_names,
"last_name":last_names,
"code":driver_codes,
"nationality":driver_nationality,
"team":driver_teams
}