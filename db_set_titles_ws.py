#!/usr/bin/env python3
import re
import time
import dateutil.parser
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.sql import or_
from dataclasses import dataclass, InitVar, asdict
from typing import Optional, Union, Tuple, List, NamedTuple
import pywikibot as pwb
# from pydantic_sqlalchemy import sqlalchemy_to_pydantic
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import concurrent.futures
from multiprocessing import cpu_count, Queue  # Вернет количество ядер процессора
import multiprocessing
import threading, queue

import db_schema as db
import make_work_wikipages

s = db.db_.s
tt = db.Titles
# tt = db.Titles
tw = db.Wiki
tl = db.WikisourceListpages

date_of_start_bot_uploading = dateutil.parser.parse('2022-03-26')
SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')


@dataclass
class TitleRow:
    id: int
    oo: bool
    title: InitVar[str] = None
    family_parsed: InitVar[str] = None
    title_ws_proposed: str = None
    is_already_this_title_in_ws: bool = False


class Level:
    # TextIdenticalLevel
    identical = 0
    begin_identical_5000 = 1
    begin_identical_2000 = 2
    not_identical = 10
    redirect = 15
    not_exists = -1
    pagename_error = -2


def make_title_oo(title: str, oo: bool):
    return f'{title}/ДО' if oo else title


def make_title_proposed(d):
    title = d.title if len(d.title.encode()) < 100 else f'{d.title[:100]}…'
    title = re.sub(r'\s{2,}', ' ', title)

    family_part = f' ({d.family_parsed})' if d.is_author else ''

    title_base = re.sub(r'\s{2,}', ' ', f'{title}{family_part}').replace('[', '(').replace(']', ')')
    title_proposed = make_title_oo(title_base, d.oo)
    i = 1
    while True:
        is_already_this_title_in_ws = s.query(tl) \
            .filter(or_(tl.pagename == title_proposed, tl.pagename == title_proposed.replace(' ', '_'))).first()
        is_already_this_title_in_proposed = s.query(tt).filter(
            or_(tt.title_ws_as_uploaded_2 == title_proposed,
                tt.title_ws_proposed == title_proposed,
                tt.title_ws_as_uploaded == title_proposed,
                ), tt.id != d.tid).first()

        if is_already_this_title_in_ws or is_already_this_title_in_proposed:
            i += 1
            title_ = f"{title_base}/Версия {i}"
            title_proposed = make_title_oo(title_, d.oo)
        else:
            break
    d.is_already_this_title_in_ws = is_already_this_title_in_ws
    d.title_ws_proposed = title_proposed
    return d


def set_title_ws(r):
    # todo некоторые страницы переименованы вручную, потом исправить по select(
    #  title_ws_as_uploaded is not null,
    #  title_ws_proposed_identical_level = -1

    try:
        dct = {c.name: getattr(r, c.name) for c in r.__table__.columns}
        d = make_work_wikipages.X.parse_obj(dct)
        d.make_wikipage()
    except Exception as e:
        print(e)

    if d.renamed_manually:
        d.title_ws_proposed = d.title_ws_as_uploaded
    else:
        d = make_title_proposed(d)

    pagename = d.title_ws_proposed

    k = {tt.title_ws_proposed: pagename,
         tt.created_before_0326: None,
         tt.mybot_creater: None}

    page = pwb.Page(SITE, pagename)
    try:
        is_page_exists = page.exists()
    except pwb.exceptions.InvalidTitleError as e:
        print(e)
        k.update({tt.title_ws_proposed_identical_level: Level.pagename_error})
    except Exception as e:
        print(e)
        k.update({tt.title_ws_proposed_identical_level: None})
    else:

        if is_page_exists:
            print(f"page exists {d.tid=} {d.title=} {pagename=}")
            if page.isRedirectPage():
                k.update({tt.title_ws_proposed_identical_level: Level.redirect})
            else:
                if m := re.search(r'ИСТОЧНИК *?= [^\n|]*?\[%s az.lib.ru\]' % d.text_url, page.text):
                    k.update({
                        tt.pid_ws: page.pageid,
                        tt.title_ws_as_uploaded_2: pagename})

                if page.text == d.wikipage_text:
                    k.update({tt.title_ws_proposed_identical_level: Level.identical,
                              tt.title_ws_as_uploaded: pagename, tt.title_ws_as_uploaded_2: pagename})
                elif page.text[:5000] == d.wikipage_text[:5000]:
                    k.update({tt.title_ws_proposed_identical_level: Level.begin_identical_5000})
                elif page.text[:2000] == d.wikipage_text[:2000]:
                    k.update({tt.title_ws_proposed_identical_level: Level.begin_identical_2000})
                else:
                    k.update({tt.title_ws_proposed_identical_level: Level.not_identical})

                k.update({
                    tt.created_before_0326: bool(page.oldest_revision['timestamp'] < date_of_start_bot_uploading),
                    tt.mybot_creater: bool(page.oldest_revision['user'] == 'TextworkerBot')
                })

        else:
            k.update({tt.title_ws_proposed_identical_level: Level.not_exists})
            print(f'page not exists {d.tid=}, {pagename=}')

    s.query(tt).filter_by(id=d.tid).update(k)
    s.commit()


def set_title_ws_by_uploaded_list(pagename):
    k = {
        # tt.title_ws_proposed: pagename,
        tt.created_before_0326: None,
        tt.mybot_creater: None}

    page = pwb.Page(SITE, pagename)
    try:
        is_page_exists = page.exists()
    except pwb.exceptions.InvalidTitleError as e:
        print(e)
        k.update({tt.title_ws_proposed_identical_level: Level.pagename_error})
    except Exception as e:
        print(e)
        k.update({tt.title_ws_proposed_identical_level: None})
    else:

        if is_page_exists:
            print(f"page exists {pagename=}")
            if page.isRedirectPage():
                k.update({tt.title_ws_proposed_identical_level: Level.redirect})
            else:
                if m := re.search(r'ИСТОЧНИК *?= [^\n|]*?\[([^ ]+) az.lib.ru\]', page.text):
                    if db_row := db.all_tables.find_one(text_url=m.group(1)):
                        d = make_work_wikipages.X.parse_obj(db_row)
                        d.make_wikipage()
                        k.update({
                            tt.pid_ws: page.pageid,
                            tt.title_ws_as_uploaded: pagename,
                            tt.title_ws_as_uploaded_2: pagename
                        })
                        print(f"page exists {d.tid=} {pagename=}")

                        if page.text == d.wikipage_text:
                            k.update({tt.title_ws_proposed_identical_level: Level.identical,
                                      tt.title_ws_as_uploaded: pagename, tt.title_ws_as_uploaded_2: pagename})
                        elif page.text[:5000] == d.wikipage_text[:5000]:
                            k.update({tt.title_ws_proposed_identical_level: Level.begin_identical_5000})
                        elif page.text[:2000] == d.wikipage_text[:2000]:
                            k.update({tt.title_ws_proposed_identical_level: Level.begin_identical_2000})
                        else:
                            k.update({tt.title_ws_proposed_identical_level: Level.not_identical})

                        k.update({
                            tt.created_before_0326: bool(
                                page.oldest_revision['timestamp'] < date_of_start_bot_uploading),
                            tt.mybot_creater: bool(page.oldest_revision['user'] == 'TextworkerBot')
                        })

                        s.query(tt).filter_by(id=d.tid).update(k)
                        s.commit()
                else:
                    print('url not found')


def main():
    ta = db.AllTables
    chunk_size = 500
    offset = 0
    while True:
        # stmt = sa.select(db._titles).select_from(sqlalchemy.join(db._titles, db._wiki, db._titles.c.id==db._wiki.c.tid)).limit(1)
        # stmt = s.query(*[c for t in [tt,tw] for c in t.__table__.c if c.name != 'id']).join(tw).filter(
        stmt = s.query(ta).filter(
            ta.title.is_not(None),
            ta.wikified.is_not(None),
            # ta.title_ws_as_uploaded.is_(None),
            # ta.title_ws_proposed.is_not(None),
            # ta.title_ws_as_uploaded.is_not(None),

            # ta.title_ws_proposed.is_(None),

            # ta.title_ws_as_uploaded_2.is_(None),
            # ta.title_ws_as_uploaded.isnot(None),

            # ta.title_ws_proposed_identical_level.is_(None),
            # ta.do_update_2 == 1,
            # cola.title_ws.isnot(None),
            # ta.title_ws_as_uploaded == 'Млечный путь (Авсеенко)/3/ДО',
            # ta.title == 'Алиса в стране чудес',
            # ta.title_ws_proposed_identical_level.is_(None),
            # cola.title_ws_proposed.isnot(None),
            #            cola.text_len < 2048,
            # cola.year_dead <= year_limited,
            # cola.year <= year_limited,
            # cola.wiki.like('%.da.ru%'),
            # cola.wikified.not_like('%feb-web.ru%'),
            # col.lang.isnot(None),
            ta.do_upload == 1,
            # do_update_as_named_proposed=True,
            ta.uploaded_text == 1,
            ta.title != ta.title_old,
            # wiki_differ_wiki2=1,
            # tid={'>':150000},
            # ta.tid == 142825,
            # updated_as_named_proposed=False,
            # is_same_title_in_ws_already=False,

        )
        # stmt = stmt.order_by(sa.desc(ta.title_ws_proposed))
        stmt = stmt.offset(offset).limit(chunk_size)
        res = stmt.all()  # .limit(2)
        for r in res:
            # db_row = TitleRow(id=r.id, title=r.title, family_parsed=r.family_parsed, oo=r.oo)
            set_title_ws(r)

        if len(res) < chunk_size:
            break
        offset += chunk_size


def main_by_uploaded_list():
    ta = db.AllTables
    chunk_size = 500
    offset = 0
    while True:
        stmt = db.db_.s.query(db.WSlistpages_uploaded.pagename, db.Titles).outerjoin(
            db.Titles,
            # db.Titles.title_ws_as_uploaded_2 == db.WSlistpages_uploaded.pagename)
            db.Titles.title_ws_as_uploaded_2 == db.WSlistpages_uploaded.pagename)
        # db.Titles.title_ws_proposed == db.WSlistpages_uploaded.pagename)
        stmt = stmt.filter(
            db.Titles.title_ws_as_uploaded_2.is_(None),
            # db.Titles.uploaded == 1,
            # db.Titles.title_ws_as_uploaded == 'Стихотворения (Радищев)/Версия 2',
        )

        # stmt = db.db_.s.query(db.Titles)
        # stmt = stmt.filter(
        #     db.Titles.title_ws_as_uploaded_2.is_(None),
        #     db.Titles.title_ws_as_uploaded.isnot(None),
        #     # db.Titles.title_ws_proposed.isnot(None),
        #     db.Titles.uploaded == 1)

        stmt = stmt.offset(offset).limit(chunk_size)  # .limit(2)
        res = stmt.all()
        for r in res:
            # title_ws_as_uploaded = r.Titles.title_ws_as_uploaded
            # title_ws_as_uploaded_2 = r.Titles.title_ws_as_uploaded_2
            set_title_ws_by_uploaded_list(
                r.text_pagename
                # r.title_ws_as_uploaded
                # r.title_ws_proposed
            )

        if len(res) < chunk_size:
            break
        offset += chunk_size


if __name__ == '__main__':
    main()
    # main_by_uploaded_list()

"""
d = make_work_wikipages.X.parse_obj({**dict(res[0].Titles))
make_work_wikipages.X.parse_obj(dict(stmt))
"""
