# from typing import Union, List
# import re
#
# re_spaces = re.compile(r'\s*')
#
#
# def wikify(text: str):
#     text = text.replace('&#1122;', 'Ѣ').replace('&#1123;', 'ѣ').replace('&amp;#1122;', 'Ѣ').replace('&amp;#1123;', 'ѣ')
#
#     text = re.sub(r'^(--|-)\s*(?!\w)', '— ', text)
#     return text


# !/usr/bin/env python3
from selenium import webdriver
import requestium
import chromedriver_binary  # Adds chromedriver binary to path
from sqlalchemy_utils.functions import database_exists, create_database
import dataset
from dataset.types import Types as T
import threading, queue
import json
from datetime import datetime, timedelta, date
import time
from typing import Optional, Union, Tuple, List, NamedTuple
from urllib.parse import quote_plus
import pypandoc
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra, dataclasses

import db_schema as db


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


class D(NamedTuple):
    tid: int
    text: Optional[str]
    desc: Optional[str]


def wikify(r, scraper):
    text = scraper.wikify(r['wiki'])
    desc = scraper.wikify(r['text_desc']) if r['text_desc'] else ''
    d = D(tid=r['tid'], text=text, desc=desc)
    return d


def wikify_all_into_db():
    # from wikificator import Scraper
    scraper = Scraper()
    ta = db.all_tables
    for r in ta.find(ta.table.c.wiki.isnot(None), do_upload=True):
        print(tid, end=' ')
        d = wikify(r)
        # db.htmls.upsert({'tid': tid, 'wikified': text})
        db.wiki.update({'tid': d.tid, 'wikified': d.text, 'desc': d.desc}, ['tid'])
        print('updated')


def test():
    scraper = Scraper()
    try:
        wikitext = scraper.wikify(text='-- dd  f')
    except Exception as e:
        print(e)
    finally:
        scraper.s.driver.quit()


def main():
    lock = threading.RLock()
    q = queue.Queue(maxsize=20)  # fifo_queue
    db_q = queue.Queue(maxsize=5)

    def db_fill_pool():
        # ta = db.all_tables
        tt = db.titles.table
        th = db.htmls.table
        tw = db.wiki.table
        td = db.desc.table

        chunk = 1000  # rows
        offset = 0
        while True:
            stm = f"SELECT {tt.name}.id as tid,wiki2 as wiki,{td.name}.desc as text_desc " \
                  f'FROM {tt.name} ' \
                  f'LEFT JOIN {th.name} ON {tt.c.id}={th.c.tid} ' \
                  f'LEFT JOIN {tw.name} ON {tt.c.id}={tw.c.tid} ' \
                  f'LEFT JOIN {td.name} ON {tt.c.id}={td.c.tid} ' \
                  f'WHERE {tt.c.do_upload} IS TRUE ' \
                  f'AND {th.c.wiki2} IS NOT NULL ' \
                  f'LIMIT {chunk} OFFSET {offset};'  #  f'AND {tw.c.text} IS NULL ' \
            # f'LIMIT {q.maxsize} OFFSET {offset};'
            # f'LIMIT 1000;'
            resultsproxy = db.db.query(stm)
            # resultsproxy = t.find(ta.table.c.wiki.is_not(None), ta.table.c.wikified.is_(None), do_upload=1, _limit=q.maxsize, _offset=offset)
            if resultsproxy.result_proxy.rowcount == 0:
                break
            for r in resultsproxy:
                q.put(r)
            offset += chunk  # q.maxsize

    def worker():
        try:
            scraper = Scraper()
            while True:
                # print(f'{q.unfinished_tasks=}')
                while q.empty():
                    # print(f'q.empty sleep')
                    time.sleep(0.2)
                r = q.get()
                print(r['tid'])
                d = wikify(r, scraper)
                if d.text:
                    while db_q.full():
                        time.sleep(0.2)
                    db_q.put(d)

                q.task_done()

        except Exception as e:
            raise
        finally:
            scraper.s.driver.quit()

    def db_save_pool():
        while True:
            while db_q.empty():
                # print(f'db_q.empty sleep')
                time.sleep(0.2)
            # print(f'{db_q.unfinished_tasks=}')
            d = db_q.get()
            print('updated', d.tid)

            # with lock:
            db.db.begin()
            try:
                db.wiki.upsert({'tid': d.tid, 'text': d.text, 'desc': d.desc}, ['tid'])
                db.db.commit()
            except:
                print(d.tid, 'rollback')
                db.db.rollback()

            db_q.task_done()

    print('find db for rows to work, initial threads')
    threading.Thread(target=db_save_pool, name='db_save', daemon=True).start()
    for r in range(q.maxsize):
        threading.Thread(target=worker, daemon=True).start()
    # threading.Thread(target=db_fill_pool, name='db_fill_pool', daemon=True).start()

    # t = db.all_tables
    # cols = t.table.c

    db_fill_pool()

    # block until all tasks are done
    q.join()
    db_q.join()
    print('All work completed')


if __name__ == '__main__':
    main()
    # wikify_all_into_db()
