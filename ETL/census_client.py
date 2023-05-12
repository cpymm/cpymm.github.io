import requests
import pandas as pd
import io

with open('census_api_key.txt') as key:
    api_key = key.read().strip()
     
with open('zip_to_tract_api_key.txt') as key:
    zip_api_key = key.read().strip() 

# Go from school zips to tracts and MSAs/CBSAs
school_df = pd.read_csv('generated_data/ScorecardData.csv')
recent_sample = school_df[school_df["year"] == 2019].copy()
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
matching_zips_cbsas_df = school_df.merge(zip_cbsa_tract_df[['zip', 'tract', 'cbsa']], left_on='school.zip', right_on='zip')[['zip', 'tract', 'cbsa', 'year']]
matching_zips_cbsas_df.columns = ['zip', 'tract', 'cbsa', 'year']
matching_zips_cbsas_df['state'] = matching_zips_cbsas_df['tract'].astype('string').str.slice(0,2)
matching_zips_cbsas_df['county'] = matching_zips_cbsas_df['tract'].astype('string').str.slice(2,5)
matching_zips_cbsas_df['tract'] = matching_zips_cbsas_df['tract'].astype('string').str.slice(5,11)
matching_zips_cbsas_df.sort_values('tract')

# Get state and PUMA from tracts
tract_puma_df = pd.read_table("source_data/2020_Census_Tract_to_2020_PUMA.txt", sep=",").astype('string')
tract_puma_df.sort_values('TRACTCE')
final_df = matching_zips_cbsas_df.merge(tract_puma_df, left_on='tract', right_on='TRACTCE')
final_df['state'] = final_df['STATEFP'].str.zfill(2)
final_df['county'] = final_df['COUNTYFP'].str.zfill(3)
final_df['puma'] = final_df['PUMA5CE']
final_df = final_df.drop(columns=['STATEFP', 'COUNTYFP', 'TRACTCE', 'PUMA5CE'])

final_df = final_df.drop_duplicates(subset=['zip', 'tract', 'year'], keep='first').reset_index(drop=True)

correct_states = pd.Series()
correct_counties = pd.Series()

# Create PUMA dataframe
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

# Get MSA/CBSA from zips


# Create MSA dataframe
cols='NAME,B19013_001E,B25105_001E'
msa='47900'
base_url = f'https://api.census.gov/data/{year}/{dsource}/{dname}'
data_url = f'{base_url}?get={cols}&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{msa}&key={api_key}'

msa_response = requests.get(data_url)
msa_df = pd.read_csv(io.StringIO(msa_response.content.decode('utf-8')))
msa_df.columns = "index", "name", "median_household_income", "median_monthly_rent", "msa"
msas = msa_df['msa']

print(msa_df)