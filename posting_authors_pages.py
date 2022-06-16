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
import make_author_wikipages
from db_set_titles_ws import Level

SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')
summary = '[[Викитека:Проект:Импорт текстов/Lib.ru]]'
year_limited = 2022 - 70 - 1

date_of_start_bot_uploading = dateutil.parser.parse('2022-06-03')
datetime_now = datetime.datetime.now()

tat = db.AllTables
tt = db.Titles
tac = db.authors_with_cat
cols = tac.table.c
ta = db.Authors
tac = db.AuthorsCategories

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
            return page
    else:
        return page


def process_page(d: make_author_wikipages.A, update_only=False, update_text_content_only=False):
    # tid = d.tid
    pagename = d.name_WS
    print(f'{pagename=}')

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
        # todo
        pass
        # if page.exists():  # todo: to reposting
        #
        #     created_before_0326 = page.oldest_revision['timestamp'] < date_of_start_bot_uploading
        #     mybot_creater = page.oldest_revision['user'] == 'TextworkerBot'
        #     db.titles.update({
        #         'id': tid,
        #         'created_before_0326': created_before_0326,
        #         'mybot_creater': mybot_creater
        #     }, ['id'])
        #     if created_before_0326 and not mybot_creater:
        #         print('страница создана не ботом:', pagename)
        #         return
        #
        #     if page.latest_revision['user'] == 'TextworkerBot' and not d.time_update:
        #         update_text_content_only = False
        #     if page.latest_revision['user'] in ['ButkoBot', 'Lozman'] and not d.time_update:
        #         update_text_content_only = True
        #     # if page.latest_revision['user'] == ['TextworkerBot', 'ButkoBot'] and not d.time_update:
        #     #     update_text_content_only = True
        #
        #     text_new = re.sub(r'(<div class="text">).*?(</div>\s*(?:{{PD.*?}})?\s*\[\[Категория)',
        #                       r'\1\n' + d.wikified.replace('\u005c', r'\\') + r'\n\2', page.text, flags=re.DOTALL) \
        #         if update_text_content_only else d.wikipage_text
        #     # if page.text != text_new:
        #     #     print()
        #
        #     if page.latest_revision['user'] not in ['TextworkerBot', 'ButkoBot', 'Lozman']:
        #         db.titles.update({'id': tid,
        #                           'uploaded': True,
        #                           # 'updated_as_named_proposed': True,
        #                           'title_ws_as_uploaded': pagename,
        #                           # 'time_update': datetime_now,
        #                           'is_lastedit_by_user': True,
        #                           }, ['id'])
        #         print("page.latest_revision['user'] not in ['TextworkerBot', 'ButkoBot']")
        #
        #
        #     else:
        #         if ok := posting_page(page, text_new, summary):
        #             db.titles.update({'id': tid,
        #                               'uploaded': True,
        #                               'updated_as_named_proposed': True,
        #                               'title_ws_as_uploaded': pagename,
        #                               'time_update': datetime_now,
        #                               }, ['id'])
        #             print(f'{tid=}, {d.year_dead=}')
        #             return True
        # else:
        #     print('страница не сущ.:', pagename)

    else:
        if page.exists():  # todo: to reposting
            print(f'page exists {pagename=}')
            # if page.isRedirectPage():
            #     pass
            # else:
            #     pass

            created_before_0603 = page.oldest_revision['timestamp'] < date_of_start_bot_uploading
            mybot_creater = page.oldest_revision['user'] == 'TextworkerBot'
            if created_before_0603 and not mybot_creater:
                print('страница создана не ботом:', pagename)
                db.authors.update({
                    'id': d.author_id,
                    'already_created': 1,
                    'pagename_as_uploaded': pagename,
                    'pid_ws': page.pageid,
                }, ['id'])
                return
            elif not created_before_0603 and mybot_creater:
                if page := posting_page(page, d.wikipage_text, summary):
                    try:
                        db.authors.update({
                            'id': d.author_id,
                            'uploaded': 1,
                            'pagename_as_uploaded': pagename,
                            'pid_ws': page.pageid,
                            'image_filename_wiki': d.image_filename_wiki,
                        }, ['id'])
                        # print(f'{d.year_dead=}')
                        return True
                    except sa.exc.IntegrityError as e:
                        return
            else:
                return
        else:
            if page := posting_page(page, d.wikipage_text, summary):
                db.authors.update({
                    'id': d.author_id,
                    'uploaded': 1,
                    'pagename_as_uploaded': pagename,
                    'pid_ws': page.pageid,
                    'image_filename_wiki': d.image_filename_wiki,
                }, ['id'])
                # print(f'{d.year_dead=}')
                return True
            else:
                print()


def posting():
    year_limited = 2022 - 70 - 1
    stmt = sa.select(ta, tac.name_ws).outerjoin(tac, ta.litarea == tac.name_site).where(
        ta.year_dead <= year_limited,
        ta.is_author == 1, ta.do_upload == 1, ta.uploaded == 0, ta.already_created == 0
        # ta.id == 12256
    )  # .limit(10)
    res = db.db_.conn.execute(stmt).fetchall()

    for r in res:
        x = dict(r)
        d = make_author_wikipages.A.parse_obj(x)
        d.make_wikipage()
        process_page(d, update_only=False)
        # process_page(d, update_only=True, update_text_content_only=True)



if __name__ == '__main__':
    posting()
    # db.titles.update({'id': 91593, 'uploaded': True}, ['id'])
