from crawler import Crawler
from CONFIG import FOLDER_DATA
import os
import pandas as pd
import math
import re
from linh_tinh import zip_proxy_extensions


def crawl_df_industry_href(number_proxy):
    os.makedirs(FOLDER_DATA, exist_ok=True)
    crl = Crawler()
    df = crl.get_df_industry_href(number_proxy)
    df.to_csv(f"{FOLDER_DATA}/df_industry_href.csv", index=False)
    return

def crawl_all_df_city_href(list_state, num_proxy, num_thread, max_trial):
    crl = Crawler()
    for state in list_state:
        os.makedirs(f"{FOLDER_DATA}/{state}", exist_ok=True)
        print(f"{FOLDER_DATA}/{state}", flush=True)
        crl.multithread_get_all_df_city_href(state, num_proxy, num_thread, max_trial)

def synthesize_df_city_href(list_state, chunk_size=10000):
    list_df = []
    for state in list_state:
        df = pd.read_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv")
        df["state"] = state
        list_df.append(df)

    data = pd.concat(list_df, ignore_index=True)
    data = data.sample(frac=1, ignore_index=True)
    num_file = math.ceil(float(len(data))/chunk_size)
    os.makedirs(f"{FOLDER_DATA}/City_hrefs", exist_ok=True)
    for i in range(num_file):
        data.loc[chunk_size*i:chunk_size*(i+1)-1].to_csv(f"{FOLDER_DATA}/City_hrefs/{i}.csv", index=False)

def crawl_all_df_company_href(name, num_proxy, num_thread, max_trial):
    os.makedirs(f"{FOLDER_DATA}/Company_hrefs", exist_ok=True)
    crl = Crawler()
    crl.multithread_get_all_df_company_href(name, num_proxy, num_thread, max_trial)

def synthesize_df_company_href(chunk_size=10000):
    folder = f"{FOLDER_DATA}/Synthesized_company_hrefs"
    os.makedirs(folder, exist_ok=True)
    folder_com = f"{FOLDER_DATA}/Company_hrefs"

    try: k = pd.read_csv(f"{folder}/count.csv").loc[0, "count"] + 1
    except: k = 0

    def foo(p):
        try:
            re.search(r"\d+\_\d+\.\d+.csv", p).group()
            return True
        except:
            return False

    list_path = [folder_com + "\\" + p for p in os.listdir(folder_com) if foo(p)]
    data = pd.concat([pd.read_csv(p) for p in list_path]).drop_duplicates(["href"], ignore_index=True)
    num_file = math.ceil(float(len(data))/chunk_size)
    for i in range(num_file):
        data.loc[chunk_size*i:chunk_size*(i+1)-1, ["href"]].to_csv(f"{folder}\\{k+i}.csv", index=False)

    pd.DataFrame({"count": [k+i]}).to_csv(f"{folder}/count.csv", index=False)
    for path in list_path:
        os.remove(path)

def crawl_all_company_infor(name, num_proxy, num_thread, max_trial):
    os.makedirs(f"{FOLDER_DATA}/Raw_Data", exist_ok=True)
    crl = Crawler()
    crl.multithread_get_all_company_infor(name, num_proxy, num_thread, max_trial)

zip_proxy_extensions()
# crawl_df_industry_href(0)
# crawl_all_df_city_href(["florida", "texas"], 0, 8, 1)
# synthesize_df_city_href(["florida", "texas"])
# crawl_all_df_company_href("0", 0, 8, 3)
# crawl_all_df_company_href("1", 0, 8, 3)
# synthesize_df_company_href()
for i in range(266, 270):
    print("+++++", i, "+++++")
    crawl_all_company_infor(str(i), 1, 8, 3)