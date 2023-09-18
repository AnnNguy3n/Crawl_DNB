from crawler import Crawler
from CONFIG import FOLDER_DATA
import os
import pandas as pd
from linh_tinh import zip_proxy_extensions

def crawl_df_industry_href(number_proxy):
    os.makedirs(FOLDER_DATA, exist_ok=True)
    crl = Crawler()
    df = crl.get_df_industry_href(number_proxy)
    df.to_csv(f"{FOLDER_DATA}/df_industry_href.csv", index=False)

def crawl_all_df_city_href(list_state, num_proxy, num_thread, max_trial):
    crl = Crawler()
    for state in list_state:
        os.makedirs(f"{FOLDER_DATA}/{state}", exist_ok=True)
        print(f"{FOLDER_DATA}/{state}", flush=True)
        crl.multithread_get_all_df_city_href(state, num_proxy, num_thread, max_trial)

def crawl_all_df_company_href(list_state, num_thread, num_proxy, max_trial, start_index, last_index):
    for state in list_state:
        os.makedirs(f"{FOLDER_DATA}/{state}/URLs", exist_ok=True)
        print(f"{FOLDER_DATA}/{state}/URLs")
        crl = Crawler()
        crl.multithread_get_all_df_company_href(state, num_thread, num_proxy, max_trial, start_index, last_index)

# zip_proxy_extensions()
# crawl_df_industry_href(10)
# crawl_all_df_city_href(["florida", "texas"], 0, 8, 1)
crawl_all_df_company_href(["florida"], 8, 0, 1, 0, 16)