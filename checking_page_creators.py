#!/usr/bin/env python3
import re
import time
import dateutil.parser
from pathlib import Path
from typing import Optional, Union, Sequence, List, Tuple
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import pywikibot as pwb

import db_schema as db


def get_page_data(page) -> dict:
    # is_libru_in_summary_created = 'lib.ru' in page.oldest_revision['comment'].lower()
    created_before_0326 = bool(page.oldest_revision['timestamp'] < date_of_start_bot_uploading)
    mybot_creater = bool(page.oldest_revision['user'] == 'TextworkerBot')
    d = dict(
        pid_ws=page.pageid,
        created_before_0326=created_before_0326,
        mybot_creater=mybot_creater,
    )
    if not created_before_0326 and mybot_creater:
        d['title_ws_as_uploaded'] = title
    return d


def process_page_by_main_list(r):
    title = r.title_ws_proposed
    tid = r.cid
    print(title)
    page = pwb.Page(SITE, title)
    if page.exists():
        d = d.update(get_page_data(page))
        db.titles.update(d, ['id'])
    else:
        print('not exists:', title)
        db.titles.update({'id': tid, 'uploaded': False}, ['id'])


def by_main_list():
    offset = 0
    limit = 100
    while True:
        stmt = db.db_.s.query(db.Titles).filter(
            db.Titles.uploaded == 1,
            db.Titles.title_ws_proposed.isnot(None),
            db.Titles.title_ws_as_uploaded.is_(None),
            # db.Titles.title_ws_as_uploaded.isnot(None),
            # db.Titles.created_before_0326.is_(None),
        ).offset(offset).limit(limit)
        res = stmt.all()
        for r in res:
            title = r.title_ws_proposed
            process_page(r)
        if len(res) < limit:
            break
        offset += limit


def process_page_by_uploaded_list(r):
    title = r.title_ws_proposed
    tid = r.cid
    print(title)
    page = pwb.Page(SITE, title)
    if page.exists():
        d = d.update(get_page_data(page))
        db.titles.update(d, ['text_url'])
    else:
        print('not exists:', title)
        db.titles.update({'text_url': tid, 'uploaded': False}, ['text_url'])


def by_uploaded_list():
    offset = 0
    limit = 100
    while True:
        stmt = db.db_.s.query(db.Titles) \
            .outerjoin(db.WSpages_w_img_err, db.WSpages_w_img_err.pagename == db.Titles.title_ws_as_uploaded_2)
        stmt = stmt.filter(
            db.Titles.title_ws_as_uploaded_2.is_(None),
        )
        stmt = stmt.offset(offset).limit(limit)
        res = stmt.all()
        for r in res:
            title = r.title_ws_proposed
            process_page(title, r.cid)
        if len(res) < limit:
            break
        offset += limit


if __name__ == '__main__':
    SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')
    date_of_start_bot_uploading = dateutil.parser.parse('2022-03-26')

    ta = db.all_tables
    cola = ta.table.c
