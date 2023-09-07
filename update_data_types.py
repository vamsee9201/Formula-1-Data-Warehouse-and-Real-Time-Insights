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
