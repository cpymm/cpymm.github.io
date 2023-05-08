import pandas as pd
import requests

import json
from bs4 import BeautifulSoup

BATCH_SIZE = 10
BASE_QUERY = "https://api.data.gov/ed/collegescorecard/v1/schools.json?{params}&{fields}&api_key={key}"

class ErrorEvent:
    def __init__(self, code, ids):
        self.code = code
        self.id_list = ids
class DataResult:
    def __init__(self, df, reqs, errors, m_rows):
        self.df = df
        self.reqs = reqs
        self.err = errors
        self.missing_rows = m_rows

class CollegeScorecardClient:
    def __init__(self, api_key, b_size=10) -> None:
        self.batch_size = b_size
        self.key = api_key
        self.errors = []
        self.reqs = 0

    def __get_year_data(self, year, dated_fields, static_fields, ipeds_id_list):
        fields = [(str(year) + "." + field) for field in dated_fields]
        fields.extend(static_fields)
        
        ipeds_param = "id=" + ",".join(ipeds_id_list)
        fields_param = "fields=" + ",".join(fields)

        req = BASE_QUERY.format(params=ipeds_param, fields=fields_param, key=self.key)
        res = requests.get(req)
        self.reqs += 1

        if res.status_code != 200:
            self.errors.append(ErrorEvent(res.status_code, ipeds_id_list))
            print(res.status_code)
            return None
        return res

    def __res_to_df(self, res, year, year_fields, static_fields):
        if not (res is None):
            soup = BeautifulSoup(res.content, features="html.parser")
            data_json = json.loads(soup.text)

            items = []
            for row in data_json["results"]:
                item = {field: row[str(year) + "." + field] for field in year_fields}
                for s_field in static_fields:
                    item[s_field] = row[s_field]
                item["year"] = year
                items.append(item)
            df = pd.DataFrame(items)
            return df
        return None

    def generate_df(self, ipeds_list, years, static_fields, dated_fields):
        responses_full = []
        expected_ids = len(ipeds_list)
        year_count = len(years)
        expected_rows = expected_ids * year_count
        self.reqs = 0
        while ipeds_list:
            batch_ids = []
            if len(ipeds_list) >= self.batch_size:
                for _ in range(self.batch_size):
                    batch_ids.append(ipeds_list.pop())
            else:
                batch_ids.extend(ipeds_list)
                ipeds_list.clear()
            batch_responses = [(self.__get_year_data(year, dated_fields, static_fields, batch_ids), year) for year in years]
            responses_full.extend(batch_responses)
        frames =  [self.__res_to_df(response, year, dated_fields, static_fields) for response, year in responses_full]
        combined_df = pd.DataFrame()

        if frames:
            combined_df = pd.concat(frames).sort_values(by=["year", "school.name"]).reset_index(drop=True)
            combined_df.to_csv("YourCollegeData.csv", index=False)

        report = DataResult(combined_df, self.reqs, self.errors, expected_rows - combined_df.shape[0])
        self.reqs = 0
        self.errors = []
        return report

