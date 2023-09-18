from selenium.webdriver.common.by import By
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

    def get_browser(self, number_proxy=-1):
        return EdgeBrowser(number_proxy=number_proxy)

    def wait_for_access(self, br: EdgeBrowser, timeout=60):
        t_ = time.time()
        while "Challenge Validation" in br.driver.title:
            time.sleep(0.1)
            if time.time() - t_ >= timeout:
                raise Exception("Timed out")

        time.sleep(0.5)

    def get_df_industry_href(self, number_proxy):
        br = self.get_browser(number_proxy)
        count_access_denied = 0
        count_error = 0
        while True:
            br.change_proxy()
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
                if count_access_denied == 10:
                    count_access_denied = 0
                    self.terminate_browser(br)
                    br = self.get_browser(number_proxy)
                    count_error = 0
            elif status == "Error":
                count_error += 1
                if count_error >= br.number_proxy:
                    raise Exception("Đã thử qua tất cả proxy nhưng không kéo được")
            elif status == "Broken":
                count_access_denied = 0
                self.terminate_browser(br)
                br = self.get_browser(number_proxy)
                count_error = 0
            else:
                try:
                    soup = BeautifulSoup(br.driver.page_source, "html.parser")
                    tables = soup.find_all("div",
                                           {"class": "z_eec717462e0cbef1_accordionItems SIFqKvSkDlwoWWHNiK58"})
                    for table in tables:
                        field_name = table.find("div",
                                                {"class": "default-ic-card"}).text
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
                    if count_error >= br.number_proxy:
                        raise Exception("Đã thử qua tất cả proxy nhưng không kéo được")

    def visit_webpage_of_state(self, br: EdgeBrowser, industry_href, state, country="us"):
        url = f"https://www.dnb.com/business-directory/company-information.{industry_href}.{country}.{state}.html"
        try:
            br.driver.get(url)
            if "Challenge Validation" in br.driver.title:
                self.wait_for_access(br)

            title = br.driver.title
            if "Discover" in title and "Dun & Bradstreet" in title:
                status = "Done"
            elif "Access Denied" in title:
                status = "Denied"
            else:
                status = "Error"
        except:
            status = "Broken"

        return status

    @staticmethod
    def handle_string(x):
        x = x.text
        x = x.replace("\n", "").strip()
        temp = x.split(" ")
        while True:
            try: temp.remove("")
            except: break

        return " ".join(temp)

    def get_df_city_href(self, br: EdgeBrowser, industry_href, state):
        count_access_denied = 0
        count_error = 0
        while True:
            br.change_proxy()
            status = self.visit_webpage_of_state(br, industry_href, state)
            if status == "Denied":
                count_access_denied += 1
                if count_access_denied == 10:
                    return status, None
            elif status == "Error":
                count_error += 1
                if count_error >= 10:
                    return status, None
            elif status == "Broken":
                return status, None
            else:
                try:
                    soup = BeautifulSoup(br.driver.page_source, "html.parser")
                    table = soup.find("div", {"class": "locationResults"})
                    list_a_tag = table.find_all("a")
                    df = pd.DataFrame({"a_tag": list_a_tag})
                    df["city"] = df["a_tag"].apply(Crawler.handle_string)
                    df["href"] = df["a_tag"].apply(lambda x: x["href"].replace(f"/business-directory/company-information.{industry_href}.us.{state}.", "").replace(".html", ""))
                    df["industry_href"] = industry_href
                    df.pop("a_tag")
                    return status, df
                except:
                    count_error += 1
                    if count_error >= 10:
                        return "Error", None

    def _get_all_df_city_href_thread(self, T_: TempClass):
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
            if is_done == "Done":
                continue

            href = T_.df_check.loc[index, "href"]

            if not is_br_on:
                T_.lock.acquire()
                try: br = self.get_browser(T_.number_proxy)
                finally: T_.lock.release()
                is_br_on = True

            status, df = self.get_df_city_href(br, href, T_.state)
            while status in ["Denied", "Broken"]:
                self.terminate_browser(br)
                T_.lock.acquire()
                try: br = self.get_browser(T_.number_proxy)
                finally: T_.lock.release()
                status, df = self.get_df_city_href(br, href, T_.state)

            T_.lock.acquire()
            try:
                T_.df_check.loc[index, "status"] = status
                T_.df_check.to_csv(f"{FOLDER_DATA}/{T_.state}/df_check.csv")
                if status == "Done":
                    T_.list_df.append(df)
            finally: T_.lock.release()

            print(index, href, status, flush=True)

        if is_br_on:
            self.terminate_browser(br)

    def multithread_get_all_df_city_href(self, state, num_thread=2, number_proxy=-1, max_trial=5):
        try:
            df_check = pd.read_csv(f"{FOLDER_DATA}/{state}/df_check.csv")
        except:
            df_industry_href = pd.read_csv(f"{FOLDER_DATA}/df_industry_href.csv")
            df_check = df_industry_href[["href"]].copy()
            df_check["status"] = "NotDone"

        T_ = TempClass()
        T_.state = state
        T_.df_check = df_check
        T_.lock = threading.Lock()
        T_.last_index = 0
        T_.len_ = len(df_check)
        T_.list_df = []
        T_.number_proxy = number_proxy

        for trial in range(max_trial):
            print("Lần", trial, flush=True)
            T_.last_index = 0
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_df_city_href_thread,
                                        args=(T_,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        return T_

    @staticmethod
    def get_url_of_city(industry_href, country, state, city, page):
        return f"https://www.dnb.com/business-directory/company-information.{industry_href}.{country}.{state}.{city}.html?page={page}"

    def visit_webpage_of_city(self, br: EdgeBrowser, industry_href, state, city, country="us"):
        page = 1
        url = Crawler.get_url_of_city(industry_href, country, state, city, page)
        list_soup = []
        check_continue = True
        while check_continue:
            try:
                br.driver.get(url)
                if "Challenge Validation" in br.driver.title:
                    self.wait_for_access(br)

                title = br.driver.title
                if "Find" in title and "Dun & Bradstreet" in title:
                    soup = BeautifulSoup(br.driver.page_source, "html.parser")
                    list_soup.append(soup)
                    ul = soup.find("ul", {"class": "integratedSearchPaginationPagination"})
                    if ul.find_all("li", attrs={"class": "next"}).__len__() == 0 or page==20:
                        check_continue = False
                elif "Access Denied" in title:
                    return "Denied", None
                else:
                    return "Error", None
            except:
                return "Broken", None

            page += 1
            url = Crawler.get_url_of_city(industry_href, country, state, city, page)

        return "Done", list_soup

    @staticmethod
    def convert_list_soup_to_df_company_href(list_soup: list):
        list_names = []
        list_hrefs = []
        list_sales = []
        for soup in list_soup:
            coms = soup.find("div", {"id": "companyResults"})
            list_a_tag = coms.find_all("a")
            list_names += [_.text.replace("\n", "").strip() for _ in list_a_tag]
            list_hrefs += [_["href"].replace("/business-directory/company-profiles.", "").replace(".html", "") for _ in list_a_tag]
            list_sales += [_.text.replace("Sales Revenue ($M):", "").replace("\n", "").strip() for _ in coms.find_all("div", {"class": "col-md-2 last"}) if "Sales Revenue ($M):" in _.text]

        return pd.DataFrame({"name": list_names, "href": list_hrefs, "sales revenue": list_sales})

    def get_df_company_href(self, br: EdgeBrowser, industry_href, state, city):
        count_access_denied = 0
        while True:
            br.change_proxy()
            status, list_soup = self.visit_webpage_of_city(br, industry_href, state, city)
            if status == "Denied":
                count_access_denied += 1
                if count_access_denied == 10:
                    return status, None
            elif status == "Error":
                return status, None
            elif status == "Broken":
                return status, None
            else:
                try:
                    df = Crawler.convert_list_soup_to_df_company_href(list_soup)
                    return status, df
                except:
                    return "Error", None

    def _get_all_df_company_href_thread(self, T_: TempClass, thread_id):
        is_br_on = False
        count = 0
        while True:
            T_.lock.acquire()
            try:
                index = T_.last_index
                T_.last_index += 1
            finally: T_.lock.release()

            if index >= T_.len_:
                break

            is_done = T_.df_check.loc[index, "status"]
            if is_done == "Done":
                continue

            industry_href = T_.df_check.loc[index, "industry_href"]
            city = T_.df_check.loc[index, "href"]

            if not is_br_on:
                T_.lock.acquire()
                try: br = self.get_browser(T_.number_proxy)
                finally: T_.lock.release()
                is_br_on = True

            status, df = self.get_df_company_href(br, industry_href, T_.state, city)
            while status in ["Denied", "Broken"]:
                self.terminate_browser(br)
                T_.lock.acquire()
                try: br = self.get_browser(T_.number_proxy)
                finally: T_.lock.release()
                status, df = self.get_df_company_href(br, industry_href, T_.state, city)

            T_.df_check.loc[index, "status"] = status
            if status == "Done":
                getattr(T_, f"list_df_{thread_id}").append(df)
                count += 1
                T_.lock.acquire()
                try:
                    if T_.last_index_done < index:
                        T_.last_index_done = index
                finally: T_.lock.release()

                if count == 100:
                    data = None
                    for df in getattr(T_, f"list_df_{thread_id}"):
                        try: data = pd.concat([data, df], ignore_index=True)
                        except: data = df.copy()

                    count = 0
                    setattr(T_, f"list_df_{thread_id}", [])
                    file_path = f"{FOLDER_DATA}/{T_.state}/URLs/{index}.csv"
                    data.to_csv(file_path, index=False)
                    T_.lock.acquire()
                    try: T_.df_check.to_csv(f"{FOLDER_DATA}/{T_.state}/URLs/df_check.csv", index=False)
                    finally: T_.lock.release()

            print(index, industry_href, city, status, flush=True)

        if is_br_on:
            self.terminate_browser(br)

    def multithread_get_all_df_company_href(self,
                                            state,
                                            num_thread=2,
                                            number_proxy=-1,
                                            max_trial=5,
                                            start_index=0,
                                            last_index=-1,
                                            ):
        try:
            df_check = pd.read_csv(f"{FOLDER_DATA}/{state}/URLs/df_check.csv")
        except:
            df_city_href = pd.read_csv(f"{FOLDER_DATA}/{state}/df_city_href.csv")
            df_check = df_city_href[["href", "industry_href"]].copy()
            df_check["status"] = "NotDone"

        T_ = TempClass()
        T_.state = state
        T_.df_check = df_check
        T_.lock = threading.Lock()
        T_.last_index = start_index
        if last_index == -1 or last_index >= len(df_check):
            T_.len_ = len(df_check)
        else:
            T_.len_ = last_index

        for i in range(num_thread):
            setattr(T_, f"list_df_{i}", [])

        T_.number_proxy = number_proxy
        T_.last_index_done = -1

        for trial in range(max_trial):
            print("Lần", trial, flush=True)
            T_.last_index = start_index
            T_.last_index_done = -1
            print(T_.last_index, T_.len_, flush=True)
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_df_company_href_thread,
                                          args=(T_, i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            if T_.last_index_done != -1:
                data = None
                for thread_id in range(num_thread):
                    for df in getattr(T_, f"list_df_{thread_id}"):
                        try: data = pd.concat([data, df], ignore_index=True)
                        except: data = df.copy()

                    setattr(T_, f"list_df_{thread_id}", [])

                file_path = f"{FOLDER_DATA}/{T_.state}/URLs/{T_.last_index_done}.csv"
                data.to_csv(file_path, index=False)

            T_.df_check.to_csv(f"{FOLDER_DATA}/{state}/URLs/df_check.csv", index=False)

    def visit_webpage_of_company(self, br, href):
        url = f"https://www.dnb.com/business-directory/company-profiles.{href}.html"
        try:
            br.driver.get(url)
            if "Challenge Validation" in br.driver.title:
                self.wait_for_access(br)
            
            title = br.driver.title
            if "Company Profile" in title and "Dun & Bradstreet" in title:
                status = "Done"
            elif "Access Denied" in title:
                status = "Denied"
            else:
                status = "Error"
        except:
            status = "Broken"

        return status
    
    def get_infor_company(self, br, href):
        count_access_denied = 0
        while True:
            br.change_proxy()
            status = self.visit_webpage_of_company(br, href)
            if status == "Denied":
                count_access_denied += 1
                if count_access_denied == 10:
                    return status, None
            elif status == "Error":
                return status, None
            elif status == "Broken":
                return status, None
            else:
                try:
                    soup = BeautifulSoup(br.driver.page_source, "html.parser")
                    return soup
                except:
                    return "Error", None