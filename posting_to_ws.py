#!/usr/bin/env python3
import re
import time
import dateutil.parser
import datetime
from pathlib import Path
import uuid
from typing import Optional, Union, Sequence, List, Tuple
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import pywikibot as pwb
import sqlalchemy as sa
from sqlalchemy.sql import or_

import db_schema as db
import make_work_wikipages
from db_set_titles_ws import Level

SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')
summary = '[[Викитека:Проект:Импорт текстов/Lib.ru]]'
year_limited = 2022 - 70 - 1

date_of_start_bot_uploading = dateutil.parser.parse('2022-03-26')
datetime_now = datetime.datetime.now()

ta = db.AllTables
tt = db.Titles


class D(BaseModel):
    class Config:
        # validate_assignment = True
        extra = Extra.allow
        # arbitrary_types_allowed = True


def posting_page(page: pwb.Page, text_new: str, summary: str):
    # if page.text != text_new:
    if page.text != text_new:  # or len(page.text) - len(text_new):
        # len1, len2 = len(page.text), len(text_new) # or len(page.text) - len(text_new):
        # if len1 != len2:
        page.text = text_new
        try:
            page.save(summary=summary)
        except pwb.exceptions.OtherPageSaveError as e:
            print(e)
            Path(f'error_wikipages_texts', uuid.uuid4().hex + '.wiki').write_text(f'{page.title()}\n{text_new}')
        except Exception as e:
            print(e)
            Path(f'error_wikipages_texts', uuid.uuid4().hex + '.wiki').write_text(f'{page.title()}\n{text_new}')
        else:
            return True
    else:
        return True


def process_page(d: make_work_wikipages.X, update_only=False, update_text_content_only=False):
    tid = d.tid
    if update_only:
        pagename = d.title_ws_as_uploaded_2
    else:
        pagename = d.title_ws_as_uploaded or d.title_ws_as_uploaded_2 or d.title_ws_proposed

    if pagename is None:
        print('pagename is None')
        return

    if len(pagename.encode()) >= 255:
        print(f'title length > 255 bytes, {pagename=}')
        return
    elif re.search(r'[\[\]]', pagename):
        print(f'illegal char(s) in {pagename=}')
        return
    page = pwb.Page(SITE, pagename)

    # moved_target = page.moved_target()  # переименованная страница без редиректа
    # page.has_deleted_revisions() # определение удалённых страниц, или только удалённых ревизий?
    # contributors = page.contributors()

    # page.oldest_revision['timestamp'] < date_of_start_bot_uploading
    # page.oldest_revision['user']

    if update_only:
        if page.exists():  # todo: to reposting

            created_before_0326 = page.oldest_revision['timestamp'] < date_of_start_bot_uploading
            mybot_creater = page.oldest_revision['user'] == 'TextworkerBot'
            db.titles.update({
                'id': tid,
                'created_before_0326': created_before_0326,
                'mybot_creater': mybot_creater
            }, ['id'])
            if created_before_0326 and not mybot_creater:
                print('страница создана не ботом:', pagename)
                return

            if page.latest_revision['user'] == 'TextworkerBot' and not d.time_update:
                update_text_content_only = False
            if page.latest_revision['user'] in ['ButkoBot', 'Lozman'] and not d.time_update:
                update_text_content_only = True
            # if page.latest_revision['user'] == ['TextworkerBot', 'ButkoBot'] and not d.time_update:
            #     update_text_content_only = True

            text_new = re.sub(r'(<div class="text">).*?(</div>\s*(?:{{PD.*?}})?\s*\[\[Категория)',
                              r'\1\n' + d.wikified.replace('\u005c', r'\\') + r'\n\2', page.text, flags=re.DOTALL) \
                if update_text_content_only else d.wikipage_text
            # if page.text != text_new:
            #     print()

            if page.latest_revision['user'] not in ['TextworkerBot', 'ButkoBot', 'Lozman']:
                db.titles.update({'id': tid,
                                  'uploaded': True,
                                  # 'updated_as_named_proposed': True,
                                  'title_ws_as_uploaded': pagename,
                                  # 'time_update': datetime_now,
                                  'is_lastedit_by_user': True,
                                  }, ['id'])
                print("page.latest_revision['user'] not in ['TextworkerBot', 'ButkoBot']")


            else:
                if ok := posting_page(page, text_new, summary):
                    db.titles.update({'id': tid,
                                      'uploaded': True,
                                      'updated_as_named_proposed': True,
                                      'title_ws_as_uploaded': pagename,
                                      'time_update': datetime_now,
                                      }, ['id'])
                    print(f'{tid=}, {d.year_dead=}')
                    return True
        else:
            print('страница не сущ.:', pagename)

    else:
        if page.exists():  # todo: to reposting
            print(f'page exists {tid=}, {pagename=}')
            # if page.isRedirectPage():
            #     pass
            # else:
            #     pass
        else:
            if ok := posting_page(page, d.wikipage_text, summary):
                db.titles.update({'id': tid,
                                  'uploaded': True,
                                  'title_ws_as_uploaded': pagename,
                                  'title_ws_as_uploaded_2': pagename,
                                  'time_update': datetime_now,
                                  'title_ws_proposed_identical_level': Level.identical,
                                  'created_before_0326': False,
                                  'mybot_creater': True},
                                 ['id'])
                print(f'{tid=}, {d.year_dead=}')
                return True


def make_wikipages_to_db():
    offset = 0
    limit = 100
    while True:
        # res = db.db_.connect.query(f'''
        #         SELECT * FROM all_tables a
        #             left join ws_pages_w_img_err u on u.pagename = a.title_ws_as_uploaded_2
        #         where u.pagename is not null and a.time_update is null
        #         limit {limit} offset {offset};''')

        stmt = sa.select(db.AllTables).where(
            ta.do_upload == 1,
            # do_update_as_named_proposed=True,
            ta.uploaded_text == 0,
            # do_update_2=True,
            # is_wikified=True,
            ta.is_wikified == 1,
            # ta.is_new_text_differed == 1,
            # wiki_differ_wiki2=1,
            # tid={'>':150000},
            # tid=94652,
            # updated_as_named_proposed=False,
            # is_same_title_in_ws_already=False,
            # ta.wikified.isnot(None),
            # cola.title_ws_proposed.isnot(None),
            # ta.title_ws_as_uploaded_2.isnot(None),
            # cola.title_ws_as_uploaded.isnot(None),
            # ta.time_update.is_(None),
            # or_(ta.time_update.is_(None), ta.time_update < dateutil.parser.parse("2022-05-01 11:30")),
            # ta.is_lastedit_by_user == 1,
            ta.text_len < 2048,
            # or_(ta.year_dead <= year_limited, ta.year <= 1917),
            ta.oo == 1,
            # cola.wiki.like('%.da.ru%'),
            # cola.wikified.not_like('%feb-web.ru%'),
            # col.lang.isnot(None),
        ).limit(limit).offset(offset)
        res = db.db_.conn.execute(stmt).fetchall()

        for r in res:
            d = make_work_wikipages.X.parse_obj(r)
            d.make_wikipage()
            process_page(d, update_only=False)
            # process_page(d, update_only=True, update_text_content_only=True)
        if len(res) < limit:
            break
        offset += limit


if __name__ == '__main__':
    wiki_text = make_wikipages_to_db()
    # db.titles.update({'id': 91593, 'uploaded': True}, ['id'])
