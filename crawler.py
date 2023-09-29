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

    def get_browser(self, number_proxy):
        return EdgeBrowser(number_proxy=number_proxy)

    def terminate_browser(self, br: EdgeBrowser):
        try:
            br.driver.quit()
            del br
        except: pass

    def reset_browser(self, br: EdgeBrowser, lock:threading.Lock=None):
        number_proxy = br.number_proxy
        self.terminate_browser(br)
        if lock is None:
            return self.get_browser(number_proxy)

        lock.acquire()
        try: br = self.get_browser(number_proxy)
        finally: lock.release()
        return br

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
                    tables = soup.find_all("div", attrs={"class": "z_eec717462e0cbef1_accordionItems SIFqKvSkDlwoWWHNiK58"})
                    list_df = []
                    for table in tables:
                        field_name = table.find("div", attrs={"class": "default-ic-card"}).text
                        list_a_tag = table.find_all("a")
                        df = pd.DataFrame({"a_tag": list_a_tag})
                        df["industry"] = df["a_tag"].apply(lambda x: x.text)
                        df["href"] = df["a_tag"].apply(lambda x: x["href"])
                        df["field_name"] = field_name
                        list_df.append(df)

                    data = pd.concat(list_df, ignore_index=True)
                    data.pop("a_tag")
                    self.terminate_browser(br)
                    data["href"] = data["href"].apply(lambda x: x.replace("/business-directory/industry-analysis.", "").replace(".html", ""))
                    return data
                except:
                    count_error += 1

            if count_access_denied == 10 or (br.number_proxy != 0 and count_error == br.number_proxy):
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
                    table = soup.find("div", attrs={"class": "locationResults"})
                    list_a_tag = table.find_all("a")
                    df = pd.DataFrame({"a_tag": list_a_tag})
                    df["city"] = df["a_tag"].apply(lambda x: x.text)
                    df["href"] = df["a_tag"].apply(lambda x: x["href"])
                    df["industry_href"] = industry_href
                    df.pop("a_tag")
                    return 1, df
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

        list_df = []
        for thread_id in range(num_thread):
            list_df += getattr(T_, f"list_df_{thread_id}")

        try: data = pd.concat(list_df, ignore_index=True)
        except: data = None
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

    @staticmethod
    def get_url_of_city(industry_href, state, city, page):
        return f"https://www.dnb.com/business-directory/company-information.{industry_href}.us.{state}.{city}.html?page={page}"

    def visit_webpage_of_city(self, br: EdgeBrowser, industry_href, state, city):
        page = 1
        url = Crawler.get_url_of_city(industry_href, state, city, page)
        list_soup = []
        check_continue = True
        while check_continue:
            try:
                br.driver.get(url)
                if "Challenge Validation" in br.driver.title:
                    self.wait_for_access(br)

                title = br.driver.title
                if "Find" in title and "Dun & Bradstreet" in title:
                    try:
                        soup = BeautifulSoup(br.driver.page_source, "html.parser")
                        list_soup.append(soup)
                        ul = soup.find("ul", attrs={"class": "integratedSearchPaginationPagination"})
                        if ul.find_all("li", attrs={"class": "next"}).__len__() == 0 or page==20:
                            check_continue = False
                    except:
                        return 3, None
                elif "Access Denied" in title:
                    return 2, None
                else:
                    return 3, None
            except:
                return 4, None

            page += 1
            url = Crawler.get_url_of_city(industry_href, state, city, page)

        return 1, list_soup

    @staticmethod
    def convert_list_soup_to_df_company_href(list_soup: list):
        list_names = []
        list_hrefs = []
        list_sales = []
        for soup in list_soup:
            coms = soup.find("div", attrs={"id": "companyResults"})
            list_a_tag = coms.find_all("a")
            list_names += [_.text for _ in list_a_tag]
            list_hrefs += [_["href"] for _ in list_a_tag]
            list_sales += [_.text for _ in coms.find_all("div", attrs={"class": "col-md-2 last"}) if "Sales Revenue ($M):" in _.text]

        return pd.DataFrame({"name": list_names, "href": list_hrefs, "sales revenue": list_sales})

    def get_df_company_href(self, br: EdgeBrowser, industry_href, state, city):
        count_access_denied = 0
        while True:
            status, list_soup = self.visit_webpage_of_city(br, industry_href, state, city)
            if status == 2:
                count_access_denied += 1
                if count_access_denied == 10: return 2, None
            elif status != 1:
                return status, None
            else:
                try:
                    df = Crawler.convert_list_soup_to_df_company_href(list_soup)
                    return 1, df
                except:
                    return 3, None

            br.change_proxy()

    def _get_all_df_company_href_thread(self, T_: TempClass, thread_id):
        is_br_on = False
        count = 0

        def foo(data):
            data["name"] = data["name"].apply(lambda x: x.replace("\n", "").strip())
            data["href"] = data["href"].apply(lambda x: x.replace("/business-directory/company-profiles.", "").replace(".html", ""))
            data["sales revenue"] = data["sales revenue"].apply(lambda x: x.replace("Sales Revenue ($M):", "").replace("\n", "").strip())

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
            industry_href = T_.df_check.loc[index, "industry_href"]
            state = T_.df_check.loc[index, "state"]
            city = T_.df_check.loc[index, "href"]

            if not is_br_on:
                T_.lock.acquire()
                try: br = self.get_browser(T_.number_proxy)
                finally: T_.lock.release()
                is_br_on = True

            br.change_proxy()
            status, df = self.get_df_company_href(br, industry_href, state, city)
            while status % 2 == 0:
                br = self.reset_browser(br, T_.lock)
                status, df = self.get_df_company_href(br, industry_href, state, city)

            if status == 1:
                df["industry_href"] = industry_href
                df["state"] = state
                df["city"] = city
                T_.df_check.loc[index, "status"] = "Done"
                getattr(T_, f"list_df_{thread_id}").append(df)
                count += 1
                if count == 100:
                    data = pd.concat(getattr(T_, f"list_df_{thread_id}"), ignore_index=True)
                    foo(data)
                    count = 0
                    setattr(T_, f"list_df_{thread_id}", [])
                    T_.lock.acquire()
                    try:
                        file_path = f"{FOLDER_DATA}/Company_hrefs/{T_.name}_{str(float(time.time()))}.csv"
                        data.to_csv(file_path, index=False)
                    finally: T_.lock.release()
            else:
                T_.df_check.loc[index, "status"] = "Error"

            print(index, industry_href, state, city, status, flush=True)

        try: data = pd.concat(getattr(T_, f"list_df_{thread_id}"), ignore_index=True)
        except: data = None
        if data is not None:
            foo(data)
            T_.lock.acquire()
            try:
                file_path = f"{FOLDER_DATA}/Company_hrefs/{T_.name}_{str(float(time.time()))}.csv"
                data.to_csv(file_path, index=False)
            finally: T_.lock.release()

        if is_br_on: self.terminate_browser(br)

    def multithread_get_all_df_company_href(self, name, num_proxy, num_thread, max_trial):
        T_ = TempClass()
        T_.name = name
        try:
            df_check = pd.read_csv(f"{FOLDER_DATA}/Company_hrefs/df_check_{T_.name}.csv")
        except:
            df_check = pd.read_csv(f"{FOLDER_DATA}/City_hrefs/{T_.name}.csv")
            df_check["status"] = "NotDone"

        T_.df_check = df_check
        T_.lock = threading.Lock()
        T_.last_index = 0
        T_.len_ = len(df_check)
        for thread_id in range(num_thread):
            setattr(T_, f"list_df_{thread_id}", [])

        T_.number_proxy = num_proxy

        for trial in range(max_trial):
            print("Lần", trial, flush=True)
            T_.last_index = 0
            for thread_id in range(num_thread):
                setattr(T_, f"list_df_{thread_id}", [])

            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_df_company_href_thread,
                                          args=(T_, i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        T_.df_check.to_csv(f"{FOLDER_DATA}/Company_hrefs/df_check_{T_.name}.csv", index=False)

    def visit_webpage_of_company(self, br, href):
        url = f"https://www.dnb.com/business-directory/company-profiles.{href}.html"
        try:
            br.driver.get(url)
            if "Challenge Validation" in br.driver.title:
                self.wait_for_access(br)

            title = br.driver.title
            if "Company Profile" in title and "Dun & Bradstreet" in title:
                status = 1
            elif "Access Denied" in title or "Oh no! 500 Error" in title:
                status = 2
            else:
                status = 3
        except:
            status = 4

        return status

    @staticmethod
    def convert_soup_to_infor(soup):
        infor = {}
        try: infor["name_company"] = soup.find("div", attrs={"class": "company-profile-header-title"}).text
        except: pass
        try: infor["key_principal"] = soup.find("span", attrs={"name": "key_principal"}).text
        except: pass
        try: infor["address"] = soup.find("span", attrs={"name": "company_address"}).text
        except: pass
        try: infor["website"] = soup.find("span", attrs={"name": "company_website"}).text
        except: pass
        try: infor["industry"] = soup.find("span", attrs={"name": "industry_links"}).text
        except: pass
        try:
            contact_div = soup.find("div", attrs={"class": "contacts-body"})
            contacts = contact_div.find_all("li", attrs={"class": "employee"})
            for i in range(len(contacts)):
                infor[f"contact_{i+1}"] = contacts[i].text
        except: pass
        try: infor["revenue"] = soup.find("span", attrs={"name": "revenue_in_us_dollar"}).text
        except: pass
        try: infor["contacts"] = soup.find("div", {"class": "contacts"}).text
        except: pass
        return infor

    def get_company_infor(self, br: EdgeBrowser, href):
        count_access_denied = 0
        while True:
            status = self.visit_webpage_of_company(br, href)
            if status == 2:
                count_access_denied += 1
                if count_access_denied == 10: return 2, None
            elif status != 1:
                return status, None
            else:
                try:
                    infor = Crawler.convert_soup_to_infor(
                        BeautifulSoup(br.driver.page_source, "html.parser")
                    )
                    return 1, infor
                except: return 3, None

            br.change_proxy()

    def _get_all_company_infor_thread(self, T_: TempClass, thread_id):
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

            br.change_proxy()
            status, infor = self.get_company_infor(br, href)
            while status % 2 == 0:
                br = self.reset_browser(br, T_.lock)
                status, infor = self.get_company_infor(br, href)

            if infor is not None:
                infor["href"] = href

            if status == 1:
                T_.df_check.loc[index, "status"] = "Done"
                getattr(T_, f"list_infor_{thread_id}").append(infor)
            else:
                T_.df_check.loc[index, "status"] = "Error"

            print(index, href, status, flush=True)

        if is_br_on: self.terminate_browser(br)

    def multithread_get_all_company_infor(self, name, num_proxy, num_thread, max_trial):
        T_ = TempClass()
        T_.name = name
        try:
            df_check = pd.read_csv(f"{FOLDER_DATA}/Raw_Data/df_check_{T_.name}.csv")
        except:
            df_check = pd.read_csv(f"{FOLDER_DATA}/Synthesized_company_hrefs/{T_.name}.csv")
            df_check["status"] = "NotDone"

        T_.df_check = df_check
        T_.lock = threading.Lock()
        T_.last_index = 0
        T_.len_ = len(df_check)
        for thread_id in range(num_thread):
            setattr(T_, f"list_infor_{thread_id}", [])

        T_.number_proxy = num_proxy

        for trial in range(max_trial):
            print("Lần", trial, flush=True)
            T_.last_index = 0
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_company_infor_thread,
                                          args=(T_, i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        list_infor = []
        for thread_id in range(num_thread):
            list_infor += getattr(T_, f"list_infor_{thread_id}")

        list_columns = ["name_company",
                        "key_principal",
                        "address",
                        "website",
                        "industry",
                        "contact_1",
                        "contact_2",
                        "contact_3",
                        "contact_4",
                        "revenue",
                        "contacts",
                        "href"
                        ]
        data = pd.DataFrame(list_infor, columns=list_columns)
        if len(data) > 0:
            file_path = f"{FOLDER_DATA}/Raw_Data/{T_.name}_{str(float(time.time()))}.csv"
            data.to_csv(file_path, index=False)

        T_.df_check.to_csv(f"{FOLDER_DATA}/Raw_Data/df_check_{T_.name}.csv", index=False)

