import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

CollegeScorecard_df_raw = pd.read_csv("generated_data/ScorecardData.csv")
scard_df = CollegeScorecard_df_raw.copy()
scard_df = scard_df.rename(
    columns={
        "student.size": "size",
        "cost.tuition.in_state": "in_state_tuition",
        "cost.tuition.out_of_state": "out_state_tuition",
        "cost.avg_net_price.public": "public_net_price",
        "cost.avg_net_price.private": "private_net_price",
        "id": "ipeds_id",
        "school.name": "name",
        "school.carnegie_size_setting": "size_setting",
        "school.zip": "zip",
        "school.region_id": "region_id",
        "school.locale": "locale",
        "school.ownership": "ownership"
    }
)


scard_df["net_cost"] = scard_df.apply(lambda row: 
            row["public_net_price"] if (row["ownership"] == 1) else row["private_net_price"],
        axis=1
)


usn_college_data = pd.read_csv("source_data/usnews_stripped.csv")
usn_top_100 = (usn_college_data[usn_college_data["2023"] < 100])[["University Name","IPEDS ID", "2023"]]

cpi_df = pd.read_csv("source_data/cpi_all.csv").groupby("Year")["Value"].mean()

print(cpi_df)
scard_df["net_cost_adjusted"] = scard_df.apply(lambda row: 
            (row["net_cost"]/cpi_df.at[row["year"]]) * 100,
        axis=1
)
print(scard_df)