from typing import Union, List
import re

re_spaces = re.compile(r'\s*')


def wikify(text: str):
    text = text.replace('&#1122;', 'Ѣ').replace('&#1123;', 'ѣ').replace('&amp;#1122;', 'Ѣ').replace('&amp;#1123;', 'ѣ')

    text = re.sub(r'^(--|-)\s*(?!\w)', '— ', text)
    return text


# !/usr/bin/env python3
from selenium import webdriver
import requestium
from sqlalchemy_utils.functions import database_exists, create_database
import dataset
from dataset.types import Types as T
import threading
import numpy as np
import json
from datetime import datetime, timedelta, date
import time
from urllib.parse import quote_plus


class SelenuimSession:
    """ If you get the error:
    selenium ProtocolError: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))
    check that versions of your `chromedriver` and Chrome are same, to update: https://chromedriver.chromium.org/downloads

    """
    # headless = False
    headless = True

    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--start-maximized')
        options.add_argument('--window-size=1200,1000')

        if self.headless:
            options.add_argument('--headless')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')

        # Disable the signature that mark Selenium as a bot
        options.add_argument("--disable-blink-features")
        options.add_argument("--disable-blink-features=AutomationControlled")

        # Disable logging undo Windows
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        self.s = requestium.Session(
            # cfg.chromedriver_path,
            'chromedriver',
            browser='chrome', default_timeout=20,
            # webdriver_options='options'
        )

        self.s._driver = requestium.requestium.RequestiumChrome(self.s.webdriver_path,
                                                                options=options,
                                                                default_timeout=self.s.default_timeout)

    def e_click(self, e):
        self.s.driver.execute_script("arguments[0].click();", e)


class Scraper(SelenuimSession):
    def __init__(self):
        super().__init__()
        self.open_browser()

    def open_browser(self):
        driver = self.s.driver
        driver.get('https://ru.wikisource.org/w/index.php?title=Sandbox&action=edit')
        driver.ensure_element_by_id('wpTextbox1')

    def wikify(self, text):
        text_new = self.s.driver.execute_script("return window.Wikify(arguments[0]);", text)
        return text_new


if __name__ == '__main__':

    scraper = Scraper()
    try:
        wikitext = scraper.wikify(text='-- dd  f')
    except Exception as e:
        print(e)
    finally:
        scraper.s.driver.quit()

    # print(soup)

    # wc = mwp.parse(r.text)
    # tags = wc.filter_tags()
    # j = tags[68].contents.nodes.get()
    # t = []
    # for n in tags[68].contents.nodes:
    #     # print(n)
    #     if isinstance(n, mwp.nodes.Tag):
    #         for n1 in n:
    #             print(n1)
    #             t.append(n1.value)
    #         continue
    #     t.append(n.value)
