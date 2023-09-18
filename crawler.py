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

    def reset_browser(self, br: EdgeBrowser, lock=None):
        '''
        :type lock: threading.Lock
        '''
        number_proxy = br.number_proxy
        self.terminate_browser(br)
        if lock is None:
            return self.get_browser(number_proxy)

        lock.acquire()
        try: br = self.get_browser(number_proxy)
        finally: lock.release()
        return br

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
                    status = 1
                elif "Access Denied" in title:
                    status = 2
                else:
                    status = 3
            except:
                status = 4

            if status == 2:
                count_access_denied += 1
            elif status == 3:
                count_error += 1
            elif status == 4:
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

    def visit_webpage_of_state(self, br: EdgeBrowser, industry_href, state):
        url = f"https://www.dnb.com/business-directory/company-information.{industry_href}.us.{state}.html"
        try:
            br.driver.get(url)
            if "Challenge Validation" in br.driver.title:
                self.wait_for_access(br)

            title = br.driver.title
            if "Discover" in title and "Dun & Bradstreet" in title:
                status = 1
            elif "Access Denied" in title:
                status = 2
            else:
                status = 3
        except:
            status = 4

        return status

    def get_df_city_href(self, br: EdgeBrowser, industry_href, state):
        count_access_denied = 0
        count_error = 0
        while True:
            status = self.visit_webpage_of_state(br, industry_href, state)
            if status == 2:
                count_access_denied += 1
            elif status == 3:
                count_error += 1
            elif status == 4:
                count_access_denied = 10
            else:
                try:
                    soup = BeautifulSoup(br.driver.page_source, "html.parser")
                    table = soup.find(name="div",
                                    attrs={"class": "locationResults"})
                    list_a_tag = table.find_all("a")
                    df = pd.DataFrame({"a_tag": list_a_tag})
                    df["city"] = df["a_tag"].apply(lambda x: x.text)
                    df["href"] = df["a_tag"].apply(lambda x: x["href"])
                    df["industry_href"] = industry_href
                    df.pop("a_tag")
                    return status, df
                except:
                    count_error += 1

            if count_access_denied == 10:
                return 2, None
            if count_error == 10:
                return 3, None

            br.change_proxy()

    def _get_all_df_city_href_thread(self, T_: TempClass, thread_id):
        is_br_on = False
        while True:
            T_.lock.acquire()
            try:
                index = T_.last_index
                T_.last_index += 1
            finally: T_.lock.release()

            if index >= T_.len_:
                break

            is_done = T_.df_check.loc[index, "status"]
            if is_done == "Done": continue
            href = T_.df_check.loc[index, "href"]

            if not is_br_on:
                T_.lock.acquire()
                try: br = self.get_browser(T_.number_proxy)
                finally: T_.lock.release()
                is_br_on = True

            status, df = self.get_df_city_href(br, href, T_.state)
            while status == 2:
                br = self.reset_browser(br, T_.lock)
                status, df = self.get_df_city_href(br, href, T_.state)

            if status == 1:
                value = "Done"
                getattr(T_, f"list_df_{thread_id}").append(df)
            else: value = "Error"
            T_.df_check.loc[index, "status"] = value

            print(index, href, status, flush=True)

        if is_br_on:
            self.terminate_browser(br)

    def multithread_get_all_df_city_href(self, state, num_proxy, num_thread, max_trial):
        try:
            df_check = pd.read_csv(f"{FOLDER_DATA}/{state}/df_check.csv")
        except:
            df_industry_href = pd.read_csv(f"{FOLDER_DATA}/df_industry_href.csv")
            df_check = df_industry_href[["href"]].copy()
            df_check["status"] = "NotDone"

        T_ = TempClass()
        T_.df_check = df_check
        T_.lock = threading.Lock()
        T_.last_index = 0
        T_.len_ = len(df_check)
        T_.state = state
        T_.number_proxy = num_proxy
        for thread_id in range(num_thread):
            setattr(T_, f"list_df_{thread_id}", [])

        for trial in range(max_trial):
            print("Lần", trial, flush=True)
            T_.last_index = 0
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_df_city_href_thread,
                                          args=(T_, i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        data = None
        for thread_id in range(num_thread):
            for df in getattr(T_, f"list_df_{thread_id}"):
                try: data = pd.concat([data, df], ignore_index=True)
                except: data = df.copy()

        try: old_data = pd.read_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv")
        except: old_data = None

        if old_data is not None:
            try: data = pd.concat([old_data, data], ignore_index=True)
            except: data = old_data.copy()

        def foo(x):
            x = x.replace("\n", "").strip()
            temp = x.split(" ")
            while True:
                try: temp.remove("")
                except: break

            return " ".join(temp)

        if data is not None:
            data["city"] = data["city"].apply(foo)
            data["href"] = data["href"].combine(data["industry_href"], lambda x, y: x.replace(f"/business-directory/company-information.{y}.us.{state}.", "").replace(".html", ""))
            data.to_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv", index=False)
            T_.df_check.to_csv(f"{FOLDER_DATA}/{state}/df_check.csv", index=False)
