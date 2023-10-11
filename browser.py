from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import os
import numpy as np
import re
from linh_tinh import get_df_proxy_by_text_file
from CONFIG import FOLDER_EXTENSION


class EdgeBrowser:
    def __init__(self, number_proxy=-1, random_proxies=True, list_proxy_id=[]) -> None:
        self.list_proxy_infor = []
        list_all_proxy = os.listdir(FOLDER_EXTENSION)
        list_all_proxy.sort()
        if number_proxy == -1 or number_proxy >= len(list_all_proxy):
            number_proxy = len(list_all_proxy)
            list_proxy = list_all_proxy.copy()
        else:
            if random_proxies:
                list_proxy_id = np.random.choice(np.arange(len(list_all_proxy)), number_proxy, replace=False)
            else:
                number_proxy = len(list_proxy_id)
            list_proxy = [list_all_proxy[_] for _ in list_proxy_id]

        self.number_proxy = number_proxy
        list_proxy.sort()
        list_proxy = [f"{FOLDER_EXTENSION}/{_}" for _ in list_proxy]
        df_proxy = get_df_proxy_by_text_file()
        for proxy in list_proxy:
            proxy_name = re.search(r"proxy_\d+", proxy).group()
            proxy_id = int(proxy_name.split("proxy_")[1])
            host, port, username, password = df_proxy.loc[proxy_id]
            address = f"{username}:{password}@{host}:{port}"
            self.list_proxy_infor.append({
                "name": proxy_name,
                "address": address
            })

        options = webdriver.EdgeOptions()
        options.binary_location = "/usr/bin/microsoft-edge"
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument("--disable-blink-features=AutomationControlled")
        [options.add_extension(_) for _ in list_proxy]
        self.driver = webdriver.Edge(options, Service(EdgeChromiumDriverManager().install()))
        self.active_proxy_id = -1
        if number_proxy != 0:
            [button.click() for button in self.get_list_proxy_button()]

        self.change_proxy(random_proxy=True)
        self.driver.get("https://httpbin.org/ip")

    def get_list_proxy_button(self):
        self.driver.get("edge://extensions/")
        extension_list = self.driver.find_element(By.ID, "extensions-list")
        list_proxy_button = extension_list.find_elements(By.TAG_NAME, "input")
        return list_proxy_button

    def change_proxy(self, random_proxy=False):
        if self.number_proxy == 0:
            return

        if self.number_proxy == 1 and self.active_proxy_id != -1:
            return

        list_proxy_button = self.get_list_proxy_button()
        old_proxy_id = self.active_proxy_id
        if random_proxy:
            self.active_proxy_id = np.random.choice([_ for _ in range(self.number_proxy) if _ != self.active_proxy_id])
        else:
            self.active_proxy_id = (self.active_proxy_id + 1) % self.number_proxy

        list_proxy_button[self.active_proxy_id].click()
        if old_proxy_id != -1:
            list_proxy_button[old_proxy_id].click()

    def clear_proxy(self):
        if self.active_proxy_id != -1:
            list_proxy_button = self.get_list_proxy_button()
            list_proxy_button[self.active_proxy_id].click()
            self.active_proxy_id = -1
