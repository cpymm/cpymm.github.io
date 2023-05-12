import pandas as pd
from pathlib import Path  

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
recent_sample["county_fips"] = recent_sample.apply(lambda row: 
            uni_county_mapping[row["school.name"]],
        axis=1
)

out_df = recent_sample[["school.name","school.state_fips","county_fips"]].copy()
filepath = Path("generated_data/college_FIPs.csv")
out_df.to_csv(filepath, index=False)