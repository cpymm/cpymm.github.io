import requests
import pandas as pd

with open('census_api_key.txt') as key:
    api_key=key.read().strip()

year='2018'
dsource='pep'
dname='components'
cols='GEONAME,NATURALINC,DOMESTICMIG,INTERNATIONALMIG'
tract='170'
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=tract:{tract}&key={api_key}'

response = requests.get(data_url)
print(response.text)