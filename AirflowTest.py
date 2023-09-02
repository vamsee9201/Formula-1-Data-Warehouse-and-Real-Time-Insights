
#%%
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
#%%

credentials = service_account.Credentials.from_service_account_file(
    '/Users/vamseekrishna/Desktop/Portfolio/DE projects/F1 project/formula1_key.json',
)
credentials = credentials.with_scopes(
    [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/cloud-platform',
    ],
)
#%%
#df = pd.DataFrame()
sql = """  
select nationality,COUNT(driverId) as total_count from `f1Entities.driverDetails`
GROUP BY nationality ORDER BY COUNT(driverId) DESC
"""
df = pandas_gbq.read_gbq(sql, project_id="formulaonedw", credentials=credentials)
#%%
print(df)
# %%
