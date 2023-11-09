
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
credentials = service_account.Credentials.from_service_account_info(
   {
  "type": "service_account",
  "project_id": "formulaonedw",
  "private_key_id": "f751593329fc697e2c275282dc65a7046f6b30bf",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDXLGKhrUGgviyl\nWm4tm7V3GPOWfIqWozZVUyPFwx3ssJl2G6DzLfvOmCP/1YSw9DMNaFrrxf/gq6nk\nvnFqxBYeix6g1XBQGlEuyC0QbnSwNTbv4sQhDVQ/ayGHZZ1gkVtMBmtTYcd8hYqX\ns4reYAcjG+79yyRv3ONLDzUAd0IhtigLaGn+bCar4SeUJBZXe4ScvGq4a9fBGOrn\nzHrtZ9fXzvNzLeJ7Pc+m1Daktpu4shSUDHQ0ULhtCl/aQ68K3fTqS8oDU3LVtb4A\n+38efcJNFeMr0Ls8gAMsqByrox7OsjYuHsa6I9eLb2o0D8pFPj6Z2foRkzxU4dx2\nJu4ZY/T3AgMBAAECggEARzd2kaJhdU1cQ4Ixs0C1B+0nOiiQShlM82KoFV1l+x/c\nOv4cAEC49gDe8V7/Iv5AI3DlnuGStg67FW1+5mJuG7/Z7EuYd7quIfi5CZY1YtjK\nDQ2V8oaR2sefekvEkeCqQfhh72AOgYVFgL44+S75TcKCEFf7AFcwl3g6B6RxK+yd\njhWefYegUxq2gJBLmw6t59OL5uaqFU3rGeeER7QS8XZwEir7jIkqpTmO73WMsHS2\n4IVAfAU50boOUWQHWuEfranE+JWJMbKOCGntozt2UtBNOXMUKfaCca0q4c4GzB02\nSgo0Zg77aRpVypJVrDzs3yqktJr+vv/S/br0+I5c2QKBgQD7MGZbcaf+AWH4jSuE\nkEv/iuunO/KmrSEjczWVvA2YZIGp39gs4xLIG+63G7Ar2x/QRqnXYrPs49fNfH/O\nxJLRv4dIfWVWpqN+hBV8hInMUGKzYf4PN7lgZqAUy7S9O1IkUk+5KpswNLiZ1XW+\n7q7dCmhZyMto94I5rvGN/vcL2wKBgQDbS2XNOSueN+jURwMAIrg2ZwWCsvUIGfOY\nrIMu/SjKD2FtZk8dhJFvNTXqf2yeOWYifIV525CqE9OrmA1AvwwYehKX2T9GpSqW\nUQfJiUN6Id6JEw00CadwIOB/s7u4JISLnkREPKGTAK35s6J00Z0kHgr8B/eAiZbk\nvtprf360FQKBgDe/cSggXGlaQzUXl63vHH7VhSFzg5IMYItumVjnCJlmzQQ3otGr\nf9KbqGpJIdtJ4ZCm7jDYPhh4JL/9PpxUMkWM3WhwNBp8F1MP23jsLW0D9jPbrrgP\n5PuJK0QSRcWtsbhP8FOnKhQTz1iM8Hn0nSh/k9NBZiugqu1Eb2XVTVz7AoGAHdE3\nq1rdqmqxtsNdDhSqdYM2hZntpUdaDuVOBQKZHDhRJ/3kPgA7giJ7DUJ8M1tcns3N\nnE8VcXq2qjtHeglADMf0ZT890k485ylnZKjMSvv19S9o+S1i3eUqgVKf8J7Oguln\nINwZoBJUlYIuUbH6quGG3tP6fDi5Eqs8qacxRC0CgYAXeI0rpLX94PfBATroniR9\nXYDItjw4Ts2ZGLSWlbxlBGSnGrQc5fZ7JfZItz2f3AwbYxUqH1CKjvGWuWLZQebb\nfP13D46CeznnteqfKfhi9PeUYd54VrcKtqRLJ464SxKoukTXJvTfFml9HLGsx2ML\nv1XHKw7eXAIsFmUwyrEpwg==\n-----END PRIVATE KEY-----\n",
  "client_email": "vamsee-formula1@formulaonedw.iam.gserviceaccount.com",
  "client_id": "107337333355905217002",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/vamsee-formula1%40formulaonedw.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
},
)
#%%
url = "http://ergast.com/api/f1/2023/circuits.json"
payload={}
headers = {}
print("getting data from the api ....")
response = requests.request("GET", url, headers=headers, data=payload)
time.sleep(2)
data = json.loads(response.text)
print(data)
# %%
print("extracting relevant data >>> ")
time.sleep(2)
circuitJson = data['MRData']['CircuitTable']['Circuits']
print(circuitJson)
time.sleep(2)
# %%
print("converting data into data frame >>>")
time.sleep(2)
circuits = pd.DataFrame(circuitJson)
print(circuits)
time.sleep(2)
#%%
def extract_lat(dict):
    return dict['lat']
def extract_lng(dict):
    return dict['long']
def extract_locality(dict):
    return dict['locality']
def extract_country(dict):
    return dict['country']
print("extracting relevant data from location column >>>")
time.sleep(2)
circuits['lat'] = circuits['Location'].apply(extract_lat)
circuits['lng'] = circuits['Location'].apply(extract_lng)
circuits['location'] = circuits['Location'].apply(extract_locality)
circuits['country'] = circuits['Location'].apply(extract_country)
print(circuits)
time.sleep(2)
# %%
print("dropping url and location columns")
time.sleep(1)
circuits.drop(columns= ['url','Location'],inplace=True)
print(circuits)
time.sleep(1)
# %%
print("getting data from warehouse to check >>>")
time.sleep(1)
sql = """
SELECT * FROM `formulaonedw.f1Entities.circuitDetails`
"""
existingCircuits = pandas_gbq.read_gbq(
    sql,
    project_id="formulaonedw",
    credentials=credentials
    
    # Set the dialect to "legacy" to use legacy SQL syntax. As of
    # pandas-gbq version 0.10.0, the default dialect is "standard".
    
)
print(existingCircuits)
time.sleep(1)
# %%
print("adjusting order of the columns >>>")
time.sleep(1)
circuits = circuits[['circuitId','circuitName','location','country','lat','lng']]
print(circuits)
time.sleep(1)
# %%
print("checking which columns are not present >>>")
mergedCircuits = circuits.merge(existingCircuits, on = "circuitId",how = "left",indicator=True)
# %%
appender = mergedCircuits[mergedCircuits['_merge'] == "left_only"]
appender.drop(columns=['circuitName_y','lat_y','lng_y','location_y','country_y','_merge'],inplace=True)
appender.rename(columns={'circuitName_x':'circuitName','location_x':'location','country_x':'country','lat_x':'lat','lng_x':'lng'},inplace=True)
print("found the entry >>>")
time.sleep(1)
print(appender)
# %%
print("adjusting the order of the appender >>>")
time.sleep(1)
appender = appender[['circuitId','circuitName','lat','lng','location','country']]
print(appender)
time.sleep(1)
# %%
print("converting lat and lng data types to float >>>")
time.sleep(1)
appender['lat'] = appender['lat'].astype(float)
appender['lng'] = appender['lng'].astype(float)
time.sleep(1)
#%%
print("adding the data to big query >>>")
time.sleep(1)
pandas_gbq.to_gbq(appender, "f1Entities.circuitDetails", project_id="formulaonedw",if_exists='append',credentials=credentials)
print("successfully executed ")