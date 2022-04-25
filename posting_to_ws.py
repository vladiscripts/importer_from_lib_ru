#!/usr/bin/env python3
import re
import time
import dateutil.parser
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

date_of_start_bot_uploading = dateutil.parser.parse('2022-03-26')

def posting_page(d: D, do_update_only=False):
    def __posting_page(page: pwb.Page, text_new: str, summary: str):
        try:
            if page.text != d.wikipage_text:
                page.text = d.wikipage_text
                page.save(summary=summary)
        except pwb.exceptions.OtherPageSaveError as e:
            print(e)
            Path(f'error_wikipages_texts', uuid.uuid4().hex + '.wiki').write_text(f'{title}\n{d.wikipage_text}')
        except Exception as e:
            print(e)
            Path(f'error_wikipages_texts', uuid.uuid4().hex + '.wiki').write_text(f'{title}\n{d.wikipage_text}')
        else:
            return True
        print()

    tid = d.tid
    title = d.title_ws_proposed

    if len(title.encode()) >= 255:
        print(f'title length > 255 bytes, {title=}')
        return
    elif re.search(r'[\[\]]', title):
        print(f'illegal char(s) in {title=}')
        return
    page = pwb.Page(SITE, title)

    # moved_target = page.moved_target()  # переименованная страница без редиректа
    # page.has_deleted_revisions() # определение удалённых страниц, или только удалённых ревизий?
    # contributors = page.contributors()

    # page.oldest_revision['timestamp'] < date_of_start_bot_uploading
    # page.oldest_revision['user']

    if do_update_only:
        if page.exists():  # todo: to reposting

            created_before_0326 = page.oldest_revision['timestamp'] < date_of_start_bot_uploading
            mybot_creater = page.oldest_revision['user'] == 'TextworkerBot'
            db.titles.update({
                'id': tid,
                'created_before_0326': created_before_0326,
                'mybot_creater': mybot_creater
            }, ['id'])
            if created_before_0326 and not mybot_creater:
                print('страница создана не ботом:', title)
                return

            if ok := __posting_page(page, d.wikipage_text, summary):
                db.titles.update({'id': tid,
                                  'uploaded': True,
                                  'updated_as_named_proposed': True,
                                  'title_ws_as_uploaded': title}, ['id'])
                print(f'{tid=}, {d.year_dead=}')
                return True
        else:
            print('страница не сущ.:', title)

    else:
        if page.exists():  # todo: to reposting
            print(f'page exists {tid=}, {title=}')
            # if page.isRedirectPage():
            #     pass
            # else:
            #     pass
        else:
            if ok := __posting_page(page, d.wikipage_text, summary):
                db.titles.update({'id': tid, 'uploaded': True, 'title_ws_as_uploaded': title}, ['id'])
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
            cola.title_ws_proposed.isnot(None),
            #            cola.text_len < 2048,
            # cola.year_dead <= year_limited,
            # cola.year <= year_limited,
            # cola.wiki.like('%.da.ru%'),
            # cola.wikified.not_like('%feb-web.ru%'),
            # col.lang.isnot(None),
            # do_upload=True,
            # do_update_as_named_proposed=True,
            uploaded_text=True,
            # wiki_differ_wiki2=1,
            # tid={'>':150000},
            # tid=87481,
            # updated_as_named_proposed=False,
            # is_same_title_in_ws_already=False,
            _offset=offset, _limit=limit)
        for r in res:
            d = make_work_wikipages.X.parse_obj(r)
            d.make_wikipage()
            posting_page(d, do_update_only=False)
            # posting_page(d, do_update_only=True)
        if res.result_proxy.rowcount < limit:
            break
        offset += limit


if __name__ == '__main__':
    wiki_text = make_wikipages_to_db()
    # db.titles.update({'id': 91593, 'uploaded': True}, ['id'])
