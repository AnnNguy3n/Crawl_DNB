from bs4 import BeautifulSoup
from browser import EdgeBrowser
from CONFIG import MAIN_PAGE, FOLDER_DATA
import time
import pandas as pd
import threading


class TempClass:
    def __init__(self) -> None: pass

    def __temp__0__(self):
        self.state = ""
        self.df_check = pd.DataFrame({})
        self.lock = threading.Lock()
        self.last_index = 0
        self.len_ = 0
        self.list_df = []
        self.number_proxy = 0


class Crawler:
    def __init__(self) -> None: pass

    def terminate_browser(self, br: EdgeBrowser):
        try:
            br.driver.quit()
            del br
        except: pass

    def reset_browser(self, br: EdgeBrowser):
        number_proxy = br.number_proxy
        self.terminate_browser(br)
        return self.get_browser(number_proxy)

    def get_browser(self, number_proxy):
        return EdgeBrowser(number_proxy=number_proxy)

    def wait_for_access(self, br: EdgeBrowser, timeout=60):
        t_ = time.time()
        while "Challenge Validation" in br.driver.title:
            time.sleep(0.1)
            if time.time() - t_ >= timeout:
                raise Exception("Timed out")

        time.sleep(1)

    def get_df_industry_href(self, number_proxy):
        br = self.get_browser(number_proxy)
        count_access_denied = 0
        count_error = 0
        while True:
            try:
                br.driver.get(MAIN_PAGE)
                if "Challenge Validation" in br.driver.title:
                    self.wait_for_access(br)

                title = br.driver.title
                if "Business Directory" in title:
                    status = "Done"
                elif "Access Denied" in title:
                    status = "Denied"
                else:
                    status = "Error"
            except:
                status = "Broken"

            if status == "Denied":
                count_access_denied += 1
            elif status == "Error":
                count_error += 1
            elif status == "Broken":
                count_access_denied = 10
            else:
                try:
                    soup = BeautifulSoup(br.driver.page_source, "html.parser")
                    tables = soup.find_all(name="div",
                                           attrs={"class": "z_eec717462e0cbef1_accordionItems SIFqKvSkDlwoWWHNiK58"})
                    for table in tables:
                        field_name = table.find(name="div",
                                                attrs={"class": "default-ic-card"}).text
                        list_a_tag = table.find_all("a")
                        df = pd.DataFrame({"a_tag": list_a_tag})
                        df["industry"] = df["a_tag"].apply(lambda x: x.text)
                        df["href"] = df["a_tag"].apply(lambda x: x["href"].replace("/business-directory/industry-analysis.", "").replace(".html", ""))
                        df["field_name"] = field_name
                        try: data = pd.concat([data, df], ignore_index=True)
                        except: data = df.copy()

                    data.pop("a_tag")
                    self.terminate_browser(br)
                    return data
                except:
                    count_error += 1

            if count_access_denied == 10 or (br.number_proxy!=0 and count_error==br.number_proxy):
                br = self.reset_browser(br)
                count_access_denied = 0
                count_error = 0
            else:
                br.change_proxy()
