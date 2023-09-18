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

zip_proxy_extensions()
crawl_df_industry_href(10)