# !/usr/bin/env pypy
from selenium import webdriver
import requestium
import chromedriver_binary  # Adds chromedriver binary to path
import sqlalchemy as sa
from sqlalchemy_utils.functions import database_exists, create_database
from sqlalchemy.orm import sessionmaker, relationship, scoped_session, Query
import dataset
from dataset.types import Types as T
import threading, queue
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime, timedelta, date
import time
from typing import Optional, Union, Tuple, List, NamedTuple
from dataclasses import dataclass
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

        try:
            self.s._driver = requestium.requestium.RequestiumChrome(self.s.webdriver_path,
                                                                    options=options,
                                                                    default_timeout=self.s.default_timeout)
        except Exception as e:
            print(e)

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


@dataclass
class D:
    tid: int
    # wiki_raw: Optional[str]
    text: Optional[str] = None
    desc: Optional[str] = None


def main():
    # lock = threading.RLock()
    q = queue.Queue(maxsize=50)  # fifo_queue
    db_q = queue.Queue(maxsize=5)

    # deprecated
    def feeder_statement_listpages_uploaded(chunk, offset):
        stmt = db.db_.s.query(
            db.Titles.id.label('tid'),
            db.Htmls.wiki,
            db.Desc.desc.label('text_desc')
        ).select_from(db.Titles).join(db.Htmls).join(db.Wiki).outerjoin(db.Desc) \
            .outerjoin(db.WSlistpages_uploaded, db.Titles.title_ws_as_uploaded_2 == db.WSlistpages_uploaded.pagename)
        stmt = stmt.filter(
            db.Htmls.wiki2_converted == 1,
            db.Htmls.is_wikified == 0,
        )
        stmt = stmt.filter(db.WSlistpages_uploaded.pagename.isnot(None))
        stmt = stmt.limit(chunk).offset(offset)
        return stmt

    def feeder_statement(chunk, offset):
        # stmt = db.db_.s.query(
        #     db.Titles.id.label('tid'),
        #     db.Htmls.wiki,
        #     db.Desc.desc.label('text_desc')
        # ).join(db.Htmls).outerjoin(db.Desc).filter(  # .join(db.Wiki)
        stmt = sa.select(
            db.Titles.id.label('tid'),
            db.Htmls.wiki,
            db.Desc.desc.label('text_desc')
        ).join(db.Htmls).outerjoin(db.Desc).where(  # .join(db.Wiki)
            # db.Wiki.text.is_(None),
            # db.Htmls.tid == 101686,
            # db.Titles.do_upload == True,
            # db.Htmls.wiki_differ_wiki2 == 1,
            db.Htmls.wiki2_converted == 1,
            # db.Htmls.is_wikified != 1,
            db.Htmls.is_wikified.is_(False),
            # db.Htmls.tid > 147000,
            db.Htmls.wiki.is_not(None)
        ).limit(chunk).offset(offset)
        return stmt

    def feeder():
        chunk_size = 100  # q.maxsize
        offset = 0
        while True:
            stmt = feeder_statement(chunk_size, offset)
            # res = stmt.all()
            res = db.db_.conn.execute(stmt).fetchall()
            for r in res:
                q.put(dict(r))
            if len(res) < chunk_size:
                break
            offset += chunk_size

    def worker():
        scraper = Scraper()
        try:
            while True:
                r = q.get()
                print(r['tid'])
                d = D(tid=r['tid'],
                      text=scraper.wikify(r['wiki']),
                      desc=scraper.wikify(r['text_desc']) if r['text_desc'] else '')
                db_q.put(d)

                q.task_done()

        except Exception as e:
            raise
        finally:
            scraper.s.driver.quit()

    def db_save():
        while True:
            d = db_q.get()
            print('updated', d.tid)

            db.db.begin()
            try:
                x = {'tid': d.tid, 'text': d.text, 'desc': d.desc, 'is_new_text_differed': 0}
                if m := db.wiki.find_one(tid=d.tid):
                    if m['text'] != d.text:
                        x['is_new_text_differed'] = 1
                        db.wiki.update(x, ['tid'])
                    else:
                        db.wiki.update(x, ['tid'])
                else:
                    db.wiki.insert(x, ['tid'])

                db.htmls.update({'tid': d.tid, 'is_wikified': 1}, ['tid'])
                db.db.commit()
            except Exception as e:
                print(d.tid, 'rollback', e)
                db.db.rollback()
            db_q.task_done()

        db_q.join()

    print('find db for rows to work, initial threads')
    # threading.Thread(target=db_save, name='db_save', daemon=True).start()
    # for r in range(q.maxsize):
    #     threading.Thread(target=worker, daemon=True).start()
    # # threading.Thread(target=db_fill_pool, name='db_fill_pool', daemon=True).start()
    #
    # # t = db.all_tables
    # # cols = t.table.c

    # feeder()

    # # block until all tasks are done
    # q.join()
    # db_q.join()
    # print('All work completed')

    with ThreadPoolExecutor(q.maxsize) as exec, \
            ThreadPoolExecutor(thread_name_prefix='db_save') as exec_db_save, \
            ThreadPoolExecutor(thread_name_prefix='feeder') as exec_feeder:
        futures = [exec.submit(worker) for i in range(q.maxsize)]
        futures += [
            exec_feeder.submit(feeder),
            exec_db_save.submit(db_save)
        ]
        # results_db_save = exec_feeder.submit(feeder())
        # results_db_save = exec_db_save.submit(db_save)
        # results_workers = exec.submit(worker)
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

    # q.join()
    # db_q.join()


if __name__ == '__main__':
    main()
    # wikify_all_into_db()
