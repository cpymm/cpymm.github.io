import requests
import pandas as pd
import io
from pathlib import Path 

with open('census_api_key.txt') as key:
    api_key = key.read().strip()
    
school_df = pd.read_csv('generated_data/ScorecardData.csv')
zip_cbsa_tract_df = pd.read_csv('generated_data/ZipToCBSATract.csv')

tract_puma_df = pd.read_table("source_data/2020_Census_Tract_to_2020_PUMA.txt", sep=",").astype('string')
tract_puma_df.sort_values('TRACTCE')
tract_puma_df.columns = ['state', 'county', 'tract', 'puma']

school_df['school.zip'] = school_df['school.zip'].str.slice(0,5).astype('int64')
school_df.sort_values('school.zip')

recent_sample = school_df[school_df["year"] == 2020].copy()
desired_counties = [
    "001", "081", "309", "007", "017", "025", "017", "049", "044", "037",
    "003", "035", "027", "077", "059", "061", "109", "009", "063", "001", 
    "121", "073", "005", "001", "001", "121", "063", "017", "001", "105", 
    "510", "077", "037", "079", "017", "037", "013", "061", "183", 
    "025", "031", "055", "037", "021", "157", "083", "201", "023", 
    "085", "113", "085", "017", "103", "067", "041", "439", "453", 
    "017", "071", "029", "001", "113", "059", "037", "047", "065", "073", 
    "083", "087", "031", "013", "013", "003", "001", "059", "031", "019", "103",
    "033", "015", "086", "161", "053", "063", "141",
    "101", "003", "055", "073", "057", "037", "540",
    "033", "025", "037", "045", "121", "067", "510",
    "830", "047", "009", "061"]
unis = recent_sample["school.name"].to_list()
uni_county_pairs = list(zip(unis, desired_counties))
uni_county_mapping = {uni: county for (uni, county) in uni_county_pairs}
school_df["county_fips"] = school_df.apply(lambda row: 
            uni_county_mapping[row["school.name"]],
        axis=1
)

zip_cbsa_tract_df = pd.read_csv('generated_data/ZipToCBSATract.csv')
school_df['school.zip'] = school_df['school.zip'].str.slice(0,5).astype('int64')
school_df.sort_values('school.zip')

matching_zips_cbsas_df = school_df.merge(zip_cbsa_tract_df[['zip', 'tract', 'cbsa']], left_on='school.zip', right_on='zip')[['school.name', 'zip', 'tract', 'cbsa', 'year', 'school.state_fips', 'county_fips']]
matching_zips_cbsas_df.columns = ['name', 'zip', 'tract', 'cbsa', 'year', 'desired_state', 'desired_county']
matching_zips_cbsas_df["desired_state"] = matching_zips_cbsas_df['desired_state'].astype(str).str.zfill(2)
matching_zips_cbsas_df["desired_county"] = matching_zips_cbsas_df['desired_county'].astype(str).str.zfill(3)


matching_zips_cbsas_df['tract'] = matching_zips_cbsas_df['tract'].astype(str).str.zfill(11)
matching_zips_cbsas_df['state'] = matching_zips_cbsas_df['tract'].astype('string').str.slice(0,2).str.zfill(2)
matching_zips_cbsas_df['county'] = matching_zips_cbsas_df['tract'].astype('string').str.slice(2,5).str.zfill(3)
# print(matching_zips_cbsas_df[matching_zips_cbsas_df['name'] == "Auburn University"])

matching_zips_cbsas_df['tract'] = matching_zips_cbsas_df['tract'].astype('string').str.slice(5,11)
# print(matching_zips_cbsas_df[matching_zips_cbsas_df['name'] == "Auburn University"])

match_df = matching_zips_cbsas_df[(matching_zips_cbsas_df["desired_state"] == matching_zips_cbsas_df["state"]) & (matching_zips_cbsas_df["desired_county"] == matching_zips_cbsas_df["county"])].drop_duplicates().copy()
match_df['state'] = match_df['state'].astype(int)
match_df['county'] = match_df['county'].astype(int)
match_df['tract'] = match_df['tract'].astype(int)


tract_puma_df['state'] = tract_puma_df['state'].astype(int)
tract_puma_df['county'] = tract_puma_df['county'].astype(int)
tract_puma_df['tract'] = tract_puma_df['tract'].astype(int)

final_df = pd.merge(match_df, tract_puma_df, how="inner", on=["state","county","tract"])
final_df = final_df.drop(columns=['desired_state','desired_county', 'year'])
final_df['state'] = final_df['state'].astype(str).str.zfill(2)
final_df['county'] = final_df['county'].astype(str).str.zfill(3)
final_df['puma'] = final_df['puma'].astype(str)
final_df = final_df.drop(columns=['STATEFP', 'COUNTYFP', 'TRACTCE', 'PUMA5CE'])
final_df = final_df[(final_df['state'].astype('int64') == final_df['desired_state'].astype('int64')) & (final_df['county'].astype('int64') == final_df['desired_county'].astype('int64'))].reset_index(drop=True)
final_df = final_df.drop(columns=['desired_state','desired_county'])

# Get data I need from my final dataframe
years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
states = ','.join(pd.unique(final_df['state'].astype('string')).tolist())
pumas = final_df['puma'].astype('string')
msas = ','.join(pd.unique(final_df['cbsa'].astype('string')).tolist())

for year in years:
    # Create PUMA dataframe
    dsource='acs'
    dname='acs1'
    cols='B19013_001E,B25105_001E'
    state=states
    puma=pumas
    base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
    data_url = f'{base_url}?get={cols}&for=public%20use%20microdata%20area:*&in=state:{state}&key={api_key}'

    puma_response = requests.get(data_url)
    puma_df = pd.read_csv(io.StringIO(puma_response.content.decode('utf-8')), on_bad_lines='skip')
    puma_df = puma_df.drop(puma_df.columns[4], axis=1)
    puma_df.columns = "median_household_income", "median_monthly_rent", "state_id", "puma"
    puma_df["median_household_income"] = puma_df["median_household_income"].map(lambda i: ''.join([x for x in  i if x.isdigit()]))
    puma_df["puma"] = puma_df["puma"].map(lambda i: ''.join([x for x in  i if x.isdigit()]))
    puma_df = puma_df[puma_df['puma'].astype('int64').isin(pumas.astype('int64'))]
    puma_df.to_csv(Path(f'generated_data/PUMA{year}.csv'), index=False)

    # Create MSA dataframe
    cols='NAME,B19013_001E,B25105_001E'
    msa=msas
    base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
    data_url = f'{base_url}?get={cols}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{msa}&key={api_key}'

    msa_response = requests.get(data_url)
    msa_df = pd.read_csv(io.StringIO(msa_response.content.decode('utf-8')))
    msa_df = msa_df.drop(msa_df.columns[4], axis=1)
    msa_df.columns = "name", "median_household_income", "median_monthly_rent", "msa"

    msa_df.to_csv(Path(f'generated_data/MSA{year}.csv'), index=False)

# Create PUMA dataframe
year='2021'
dsource='acs'
dname='acs1'
cols='B19013_001E,B25105_001E'
state=states
puma=pumas
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=public%20use%20microdata%20area:*&in=state:{state}&key={api_key}'

puma_response = requests.get(data_url)
puma_df = pd.read_csv(io.StringIO(puma_response.content.decode('utf-8')), on_bad_lines='skip')
puma_df = puma_df.drop(puma_df.columns[4], axis=1)
puma_df.columns = "median_household_income", "median_monthly_rent", "state_id", "puma"
puma_df["median_household_income"] = puma_df["median_household_income"].map(lambda i: ''.join([x for x in  i if x.isdigit()]))
puma_df["puma"] = puma_df["puma"].map(lambda i: ''.join([x for x in  i if x.isdigit()]))
puma_df = puma_df[puma_df['puma'].astype('int64').isin(pumas.astype('int64'))]
puma_df.to_csv(Path('generated_data/PUMA2021.csv'), index=False)

# Create MSA dataframe
cols='NAME,B19013_001E,B25105_001E'
msa=msas
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{msa}&key={api_key}'

msa_response = requests.get(data_url)
msa_df = pd.read_csv(io.StringIO(msa_response.content.decode('utf-8')))
msa_df = msa_df.drop(msa_df.columns[4], axis=1)
msa_df.columns = "name", "median_household_income", "median_monthly_rent", "msa"

msa_df.to_csv(Path('generated_data/MSA2021.csv'), index=False)
