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
from pydantic_sqlalchemy import sqlalchemy_to_pydantic
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
    title, family_parsed = d.title, d.family_parsed

    title_base = f'{title} ({family_parsed})'
    title_proposed = make_title_oo(title_base, d.oo)
    i = 1
    while True:
        is_already_this_title_in_ws = s.query(tl) \
            .filter(or_(tl.pagename == title_proposed, tl.pagename == title_proposed.replace(' ', '_'))).scalar()
        is_already_this_title_in_proposed = s.query(tt).filter(
            or_(tt.title_ws_proposed == title_proposed,
                tt.title_ws_as_uploaded == title_proposed,
                tt.title_ws_as_uploaded_2 == title_proposed), tt.id != d.tid).scalar()

        if is_already_this_title_in_ws or is_already_this_title_in_proposed:
            i += 1
            title_base = f"{title} ({family_parsed})/Версия {i}"
            title_proposed = make_title_oo(title_base, d.oo)
        else:
            break
    d.is_already_this_title_in_ws = is_already_this_title_in_ws
    d.title_ws_proposed = title_proposed
    return d


def set_title_ws(d):
    # todo некоторые страницы переименованы вручную, потом исправить по select(
    #  title_ws_as_uploaded is not null,
    #  title_ws_proposed_identical_level = -1
    if d.renamed_manually:
        d.title_ws_proposed = d.title_ws_as_uploaded
    else:
        d = make_title_proposed(d)

    title_ws = d.title_ws_proposed

    r = {tt.title_ws_proposed: title_ws,
         tt.created_before_0326: None,
         tt.mybot_creater: None}

    page = pwb.Page(SITE, title_ws)
    try:
        is_page_exists = page.exists()
    except pwb.exceptions.InvalidTitleError as e:
        print(e)
        r.update({tt.title_ws_proposed_identical_level: Level.pagename_error})
    except Exception as e:
        print(e)
        r.update({tt.title_ws_proposed_identical_level: None})
    else:

        if is_page_exists:
            print(f"page exists {d.tid=} {d.title=} {title_ws=}")
            if page.isRedirectPage():
                r.update({tt.title_ws_proposed_identical_level: Level.redirect})
            else:
                if f'= [{d.text_url} az.lib.ru]' in page.text:
                    r.update({
                        tt.pid_ws: page.pageid,
                        tt.title_ws_as_uploaded_2: title_ws
                    })

                if page.text == d.wikipage_text:
                    r.update({tt.title_ws_proposed_identical_level: Level.identical,
                              tt.title_ws_as_uploaded: title_ws, tt.title_ws_as_uploaded_2: title_ws})
                elif page.text[:5000] == d.wikipage_text[:5000]:
                    r.update({tt.title_ws_proposed_identical_level: Level.begin_identical_5000})
                elif page.text[:2000] == d.wikipage_text[:2000]:
                    r.update({tt.title_ws_proposed_identical_level: Level.begin_identical_2000})
                else:
                    r.update({tt.title_ws_proposed_identical_level: Level.not_identical})

                r.update({
                    tt.created_before_0326: bool(page.oldest_revision['timestamp'] < date_of_start_bot_uploading),
                    tt.mybot_creater: bool(page.oldest_revision['user'] == 'TextworkerBot')
                })

        else:
            r = {tt.title_ws_proposed_identical_level: Level.not_exists}
            print(f'page not exists {d.tid=}, {title_ws=}')

    s.query(tt).filter_by(id=d.tid).update(r)
    s.commit()


def set_title_ws_by_uploaded_list(pagename):
    r = {
        # tt.title_ws_proposed: pagename,
        tt.created_before_0326: None,
        tt.mybot_creater: None}

    page = pwb.Page(SITE, pagename)
    try:
        is_page_exists = page.exists()
    except pwb.exceptions.InvalidTitleError as e:
        print(e)
        r.update({tt.title_ws_proposed_identical_level: Level.pagename_error})
    except Exception as e:
        print(e)
        r.update({tt.title_ws_proposed_identical_level: None})
    else:

        if is_page_exists:
            print(f"page exists {pagename=}")
            if page.isRedirectPage():
                r.update({tt.title_ws_proposed_identical_level: Level.redirect})
            else:
                if m := re.search(r'ИСТОЧНИК *?= \[([^ ]+) az.lib.ru\]', page.text):
                    db_row = db.all_tables.find_one(text_url=m.group(1))
                    if db_row:
                        d = make_work_wikipages.X.parse_obj(db_row)
                        d.make_wikipage()
                        print(f"page exists {d.tid=} {pagename=}")

                        r.update({
                            tt.pid_ws: page.pageid,
                            tt.title_ws_as_uploaded_2: pagename
                        })

                        if page.text == d.wikipage_text:
                            r.update({tt.title_ws_proposed_identical_level: Level.identical,
                                      tt.title_ws_as_uploaded: pagename, tt.title_ws_as_uploaded_2: pagename})
                        elif page.text[:5000] == d.wikipage_text[:5000]:
                            r.update({tt.title_ws_proposed_identical_level: Level.begin_identical_5000})
                        elif page.text[:2000] == d.wikipage_text[:2000]:
                            r.update({tt.title_ws_proposed_identical_level: Level.begin_identical_2000})
                        else:
                            r.update({tt.title_ws_proposed_identical_level: Level.not_identical})

                        r.update({
                            tt.created_before_0326: bool(
                                page.oldest_revision['timestamp'] < date_of_start_bot_uploading),
                            tt.mybot_creater: bool(page.oldest_revision['user'] == 'TextworkerBot')
                        })

                        s.query(tt).filter_by(id=d.tid).update(r)
                        s.commit()


def _main():
    q = queue.Queue(maxsize=500)

    def worker():
        while True:
            r = q.get()
            try:
                dct = {c.name: getattr(r, c.name) for c in r.__table__.columns}
                d = make_work_wikipages.X.parse_obj(dct)
                d.make_wikipage()
            except Exception as e:
                print(e)
            # db_row = TitleRow(id=r.id, title=r.title, family_parsed=r.family_parsed, oo=r.oo)
            set_title_ws(d)
            # t.update({
            #     'id': db_row.id,
            #     'is_already_this_title_in_ws': db_row.is_already_this_title_in_ws,
            #     'title_ws_proposed': db_row.title_ws_proposed}, ['id'])
            q.task_done()

    def feeder() -> Optional[List[dict]]:
        ta = db.AllTables
        chunk_size = q.maxsize
        offset = 0
        while True:
            # stmt = sa.select(db._titles).select_from(sqlalchemy.join(db._titles, db._wiki, db._titles.c.id==db._wiki.c.tid)).limit(1)
            # stmt = s.query(*[c for t in [tt,tw] for c in t.__table__.c if c.name != 'id']).join(tw).filter(
            stmt = s.query(ta).filter(
                ta.title.is_not(None),
                ta.wikified.is_not(None),
                # col.title_ws_as_uploaded.is_(None),
                ta.title_ws_as_uploaded.is_not(None),
                ta.title_ws_proposed_identical_level.is_(None),
                # cola.title_ws.isnot(None),
                # ta.title_ws_as_uploaded == 'Млечный путь (Авсеенко)/3/ДО',
                # ta.title == '"Окраины" - о том чего домогается Остзейское рыцарство в России',
                # ta.title_ws_proposed_identical_level.is_(None),
                # cola.title_ws_proposed.isnot(None),
                #            cola.text_len < 2048,
                # cola.year_dead <= year_limited,
                # cola.year <= year_limited,
                # cola.wiki.like('%.da.ru%'),
                # cola.wikified.not_like('%feb-web.ru%'),
                # col.lang.isnot(None),
                # do_upload=True,
                # do_update_as_named_proposed=True,
                ta.do_upload == 1,
                # wiki_differ_wiki2=1,
                # tid={'>':150000},
                # ta.tid == 87504,
                # updated_as_named_proposed=False,
                # is_same_title_in_ws_already=False,

            ).offset(offset).limit(chunk_size)
            res = stmt.all()  # .limit(2)
            for r in res:
                q.put(r)
            if len(res) < chunk_size:
                break
            offset += chunk_size
            while not q.empty():
                time.sleep(1)

    db.titles.update({'title_ws_proposed': None}, [])  # сбросить все перед запуском
    with ThreadPoolExecutor() as exec, ThreadPoolExecutor(thread_name_prefix='feeder') as exec_feeder:
        futures = [
            exec_feeder.submit(feeder),
            exec.submit(worker)
        ]
        # results_db_save = exec_feeder.submit(feeder())
        # results_db_save = exec_db_save.submit(db_save)
        # results_workers = exec.submit(worker)
        for future in concurrent.futures.as_completed(futures):
            # url = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (future, exc))
                # print('%r generated an exception: %s' % (url, exc))
            else:
                print(future.result())
                pass


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
            ta.title_ws_proposed.is_(None),
            # ta.title_ws_proposed_identical_level.is_(None),
            ta.do_update_2 == 1,
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
            # do_upload=True,
            # do_update_as_named_proposed=True,
            ta.uploaded_text == 1,
            # wiki_differ_wiki2=1,
            # tid={'>':150000},
            # ta.tid == 142825,
            # updated_as_named_proposed=False,
            # is_same_title_in_ws_already=False,

        ).offset(offset).limit(chunk_size)
        res = stmt.all()  # .limit(2)
        for r in res:

            try:
                dct = {c.name: getattr(r, c.name) for c in r.__table__.columns}
                d = make_work_wikipages.X.parse_obj(dct)
                d.make_wikipage()
            except Exception as e:
                print(e)
            # db_row = TitleRow(id=r.id, title=r.title, family_parsed=r.family_parsed, oo=r.oo)
            set_title_ws(d)

        if len(res) < chunk_size:
            break
        offset += chunk_size


def main_by_uploaded_list():
    ta = db.AllTables
    chunk_size = 500
    offset = 0
    while True:
        stmt = db.db_.s.query(db.WSlistpages_uploaded).outerjoin(db.Titles,
                                                                 # db.Titles.title_ws_as_uploaded_2 == db.WSlistpages_uploaded.pagename)
                                                                 db.Titles.title_ws_as_uploaded == db.WSlistpages_uploaded.pagename)
        stmt = stmt.filter(db.Titles.title_ws_as_uploaded_2.is_(None), db.Titles.uploaded == 1)
        stmt = stmt.offset(offset).limit(chunk_size)  # .limit(2)
        res = stmt.all()
        for r in res:
            set_title_ws_by_uploaded_list(r.pagename)

        if len(res) < chunk_size:
            break
        offset += chunk_size


if __name__ == '__main__':
    # main()
    main_by_uploaded_list()

"""
d = make_work_wikipages.X.parse_obj({**dict(res[0].Titles))
make_work_wikipages.X.parse_obj(dict(stmt))
"""
