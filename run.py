from crawler import Crawler
from CONFIG import FOLDER_DATA


def crawl_df_industry_href():
    crl = Crawler()
    df = crl.get_df_industry_href()
    df.to_csv(f"{FOLDER_DATA}/df_industry_href.csv", index=False)

# crawl_df_industry_href()