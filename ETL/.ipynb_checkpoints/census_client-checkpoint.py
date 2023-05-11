import requests
import pandas as pd
import io

with open('census_api_key.txt') as key:
    api_key=key.read().strip()

year='2021'
dsource='acs'
dname='acs1'
cols='NAME,B19013_001E,B25105_001E'
state='24'
puma='01101'
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=public%20use%20microdata%20area:{puma}&in=state:{state}&key={api_key}'

puma_response = requests.get(data_url)
puma_df = pd.read_csv(io.StringIO(puma_response.content.decode('utf-8')))
puma_df.columns = "index", "name", "median_household_income", "median_monthly_rent", "state_id", "puma"
print(puma_df)

year='2021'
dsource='acs'
dname='acs1'
cols='NAME,B19013_001E,B25105_001E'
msa='47900'
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{msa}&key={api_key}'

msa_response = requests.get(data_url)
msa_df = pd.read_csv(io.StringIO(msa_response.content.decode('utf-8')))
msa_df.columns = "index", "name", "median_household_income", "median_monthly_rent", "msa"
print(msa_df)