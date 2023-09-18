from crawler import Crawler
from CONFIG import FOLDER_DATA
import os
import pandas as pd
from linh_tinh import zip_proxy_extensions
import re

# zip_proxy_extensions()

def crawl_df_industry_href(number_proxy):
    os.makedirs(FOLDER_DATA, exist_ok=True)
    crl = Crawler()
    df = crl.get_df_industry_href(number_proxy)
    df.to_csv(f"{FOLDER_DATA}/df_industry_href.csv", index=False)


def crawl_all_df_city_href(list_state, number_proxy, num_thread, max_trial):
    for state in list_state:
        os.makedirs(f"{FOLDER_DATA}/{state}", exist_ok=True)
        print(f"{FOLDER_DATA}/{state}", flush=True)
        crl = Crawler()
        T_ = crl.multithread_get_all_df_city_href(state, num_thread, number_proxy, max_trial)
        data = None
        for df in T_.list_df:
            try: data = pd.concat([data, df], ignore_index=True)
            except: data = df.copy()

        try:
            old_data = pd.read_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv")
            print("A")
        except:
            old_data = None
            print("B")

        if old_data is not None:
            data = pd.concat([old_data, data], ignore_index=True)

        def foo(x):
            try: y = re.search(r"\(\d+\,\d+\)", x).group().replace("(", "").replace(")", "").replace(",", "")
            except: y = re.search(r"\(\d+\)", x).group().replace("(", "").replace(")", "")
            y = int(y)
            if y <= 1000: return y
            return 1000

        data["count"] = data["city"].apply(foo)
        data.to_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv", index=False)
        T_.df_check.to_csv(f"{FOLDER_DATA}/{state}/df_check.csv", index=False)


crawl_df_industry_href(0)
crawl_all_df_city_href(["florida", "texas"], 0, 8, 1)
