import pandas as pd
from pathlib import Path 

# Join zip_cbsa and zip_tract
filepath = Path("generated_data/ZipToCBSATract.csv")

zip_cbsa = pd.read_excel('source_data/zip_cbsa.xlsx', usecols='A, B')
zip_tract = pd.read_excel('source_data/zip_tract.xlsx', usecols='A:D')
zip_cbsa_tract = pd.merge(zip_cbsa, zip_tract, on='zip')
zip_cbsa_tract.columns = "zip", "cbsa", "tract", "city", "state"
zip_cbsa_tract.to_csv(filepath, index=False)
print("Done!")