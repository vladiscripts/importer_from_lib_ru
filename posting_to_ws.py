#!/usr/bin/env python3
import re
import time
from pathlib import Path
import uuid
from typing import Optional, Union, Sequence, List, Tuple
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import pywikibot as pwb

import db_schema as db
import make_work_wikipages


class D(BaseModel):
    class Config:
        # validate_assignment = True
        extra = Extra.allow
        # arbitrary_types_allowed = True


SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')
summary = '[[Викитека:Проект:Импорт текстов/Lib.ru]]'
year_limited = 2022 - 70 - 1


def posting_page(d: D):
    def wiki_posting_page(page: D, text_new: str, summary: str):
        if page.text != text_new:
            page.text = text_new
            page.save(summary=summary)

    tid = d.tid
    title = d.title_ws

    if len(title.encode()) >= 255:
        print(f'length > 255 bytes, {title=}')
        return
    elif re.search(r'[\[\]]', title):
        print(f'illegal char(s) in {title=}')
        return
    page = pwb.Page(SITE, title)
    if not page.exists():  # todo: to reposting
        return
        print(f'page exists {tid=}, {title=}')
        db.titles.update({'id': tid, 'is_same_title_in_ws_already': True}, ['id'])
        # page.title += '/Дубль'
        # page.text = r['wiki_page'] + '\n[[Категория:Импорт/lib.ru/Дубли имён существующих страниц]]'
        # if page.isRedirectPage():
        #     pass
    else:
        try:
            page.text = d.wikipage_text
            page.save(summary=summary)

        except pwb.exceptions.OtherPageSaveError as e:
            print(e)
            Path(f'error_wikipages_texts', uuid.uuid4().hex + '.wiki').write_text(f'{title}\n{d.wikipage_text}')

        except Exception as e:
            print(e)
            Path(f'error_wikipages_texts', uuid.uuid4().hex + '.wiki').write_text(f'{title}\n{d.wikipage_text}')

        else:
            db.titles.update({'id': tid,
                              'uploaded': True, 'updated_as_named_guess':  True,
                              'title_ws_as_uploaded':title}, ['id'])
            print(f'{tid=}, {d.year_dead=}')
            return True


def make_wikipages_to_db():
    ta = db.all_tables
    cola = ta.table.c

    offset = 0
    limit = 100
    while True:
        res = ta.find(
            cola.wikified.isnot(None),
            # cola.title_ws.isnot(None),
            cola.title_ws_guess.isnot(None),
#            cola.text_len < 2048,
            cola.year_dead <= year_limited,
            # cola.wikified.not_like('%feb-web.ru%'),
            # col.lang.isnot(None),
            # uploaded_text=False, do_upload=True,
            do_update_as_named_guess=True, updated_as_named_guess=False,
            # is_same_title_in_ws_already=False,
            _offset=offset, _limit=limit)
        if res.result_proxy.rowcount == 0:
            print(f'did not found rows in DB')
            break
        for r in res:
            d = make_work_wikipages.X.parse_obj(r)
            d.make_wikipage()
            posting_page(d)
        offset += limit


wiki_text = make_wikipages_to_db()
# db.titles.update({'id': 91593, 'uploaded': True}, ['id'])
