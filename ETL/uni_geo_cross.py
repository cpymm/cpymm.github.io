import pandas as pd
from pathlib import Path  

school_df = pd.read_csv('generated_data/ScorecardData.csv')
zip_cbsa_tract_df = pd.read_csv('generated_data/ZipToCBSATract.csv')

tract_puma_df = pd.read_table("source_data/2020_Census_Tract_to_2020_PUMA.txt", sep=",").astype('string')
tract_puma_df.sort_values('TRACTCE')
tract_puma_df.columns = ['state', 'county', 'tract', 'puma']

school_df['school.zip'] = school_df['school.zip'].str.slice(0,5).astype('int64')
school_df.sort_values('school.zip')

recent_sample = school_df[school_df["year"] == 2020].copy()
desired_counties = [
    "1", "81", "309", "7", "17", "25", "17", "49", "44", "37",
    "3", "35", "027", "077", "059", "061", "109", "009", "063", "001", 
    "121", "073", "005", "001", "001", "121", "063", "017", "001", "105", 
    "510", "077", "037", "079", "017", "037", "013", "061", "183", 
    "025", "031", "055", "037", "021", "157", "083", "201", "023", 
    "085", "113", "085", "017", "103", "067", "041", "439", "453", 
    "017", "071", "029", "001", "113", "059", "037", "047", "065", "073", 
    "083", "087", "031", "013", "013", "003", "001", "059", "031", "019", "103",
    "033", "015", "086", "161", "053", "063", "141",
    "101", "003", "055", "073", "057", "037", "540",
    "033", "025", "037", "045", "121", "067", "510",
    "830", "027", "009", "061"]
unis = recent_sample["school.name"].to_list()
uni_county_pairs = list(zip(unis, desired_counties))
uni_county_mapping = {uni: county for (uni, county) in uni_county_pairs}
recent_sample["county_fips"] = recent_sample.apply(lambda row: 
            uni_county_mapping[row["school.name"]],
        axis=1
)
school_df = recent_sample

matching_zips_cbsas_df = school_df.merge(zip_cbsa_tract_df[['zip', 'tract', 'cbsa']], left_on='school.zip', right_on='zip')[['school.name', "id",'zip', 'tract', 'cbsa', 'year', 'school.state_fips', 'county_fips']]
matching_zips_cbsas_df.columns = ['name', 'id','zip', 'tract', 'cbsa', 'year', 'desired_state', 'desired_county']
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

out_df = final_df[["name","id","state","county", "cbsa","puma"]].copy().drop_duplicates()
filepath = Path("generated_data/college_FIPs.csv")
out_df.to_csv(filepath, index=False)