from crawler import Crawler
from CONFIG import FOLDER_DATA
import os
import pandas as pd


def crawl_df_industry_href():
    crl = Crawler()
    df = crl.get_df_industry_href()
    df.to_csv(f"{FOLDER_DATA}/df_industry_href.csv", index=False)

# crawl_df_industry_href()


def crawl_all_df_city_href():
    for state in ["florida", "texas"]:
        os.makedirs(f"{FOLDER_DATA}/{state}", exist_ok=True)
        crl = Crawler()
        T_ = crl.multithread_get_all_df_city_href(state, num_thread=1)
        for df in T_.list_df:
            try: data = pd.concat([data, df], ignore_index=True)
            except: data = df.copy()

        try: old_data = pd.read_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv")
        except: old_data = None

        if old_data is not None:
            data = pd.concat([old_data, data], ignore_index=True)

        data.to_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv", index=False)
        T_.df_check.to_csv(f"{FOLDER_DATA}/{state}/df_check.csv")

crawl_all_df_city_href()