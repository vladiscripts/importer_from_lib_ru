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
from threading import RLock

import db.schema as db
import make_author_wikipages
from db_set_titles_ws import Level

lock = RLock()

SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')
summary = '[[Викитека:Проект:Импорт текстов/Lib.ru]] подкатегория автора'

tat = db.AllTables
tt = db.Titles
ta = db.Authors


class D(BaseModel):
    tid: int = Field(alias='id')
    author_id: int
    author_pagename: str = Field(alias='name_WS')
    author_family: str = Field(alias='family_parsed')
    title_ws_as_uploaded_2: str
    pageauthor_subcategory_added: int
    author_subcategory_posted: bool
    author_subcategory_tpl_posted: bool

    class Config:
        # validate_assignment = True
        extra = Extra.allow
        # arbitrary_types_allowed = True


def db_update_author_pagename(db_author_page_id, pagename_new):
    stmt = sa.update(ta).values({ta.name_WS: pagename_new}).where(ta.id == db_author_page_id)
    db.db_.s.execute(stmt)
    db.db_.s.commit()


def db_update_pagename(db_page_id, pagename_new):
    stmt = sa.update(tt).values({tt.title_ws_as_uploaded: pagename_new}).where(tt.id == db_page_id)
    db.db_.s.execute(stmt)
    db.db_.s.commit()


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


def posting_category_page(d, cat_name):
    if not d.author_subcategory_posted:
        cat_page = pwb.Page(SITE, cat_name)
        if not cat_page.exists():
            cat_text = f'__HIDDENCAT__\n' \
                       f'{{{{Импорт текстов/az.lib.ru/категория автора}}}}\n' \
                       f'[[Категория:Импорт/lib.ru/Категории авторов|{d.author_family}]]\n' \
                       f'[[Категория:{d.author_pagename}]]\n'
            posting_page(cat_page, cat_text, summary='[[Викитека:Проект:Импорт текстов/Lib.ru]]')
        if cat_page.exists():
            stmt = sa.update(ta).values({ta.author_subcategory_posted: 1}).where(ta.id == d.author_id)
            db.db_.s.execute(stmt)
            db.db_.s.commit()


def process_page(d: D):
    pagename = d.title_ws_as_uploaded_2
    print(f'{pagename=}')

    if pagename is None:
        print('pagename is None')
        return

    page = pwb.Page(SITE, pagename)

    while not page.exists():
        try:
            page = page.moved_target()
        except pwb.exceptions.NoMoveTargetError as e:
            print('no page, NoMoveTargetError')
            stmt = sa.update(tt).values({tt.pageauthor_subcategory_added: -1}).where(tt.id == d.tid)
            db.db_.s.execute(stmt)
            db.db_.s.commit()
            return
        except Exception as e:
            print()
            pass

    if page.exists():
        # print(f'page exists {pagename=}')
        while page.isRedirectPage():
            print(f'page isRedirectPage, getRedirectTarget()')
            page = page.getRedirectTarget()

        db_update_pagename(d.tid, page.title())

        cat_name = f'Категория:Импорт/az.lib.ru/{d.author_pagename}'
        text_new = f'{page.text}\n[[{cat_name}]]\n'

        with lock:
            if f'[[{cat_name}]]' not in page.text:
                page = posting_page(page, text_new, summary)
            if page:
                posting_category_page(d, cat_name)

            if f'[[{cat_name}]]' in page.text:
                stmt = sa.update(tt) \
                    .values({tt.pageauthor_subcategory_added: 1, tt.title_ws_as_uploaded: page.title()}) \
                    .where(tt.id == d.tid)
                db.db_.s.execute(stmt)
                db.db_.s.commit()

            return True

    else:
        print(f'is not exists {pagename=}')
        # db_update_pagename(d.tid, None)


def worker():
    stmt = sa.select(ta.name_WS, ta.family_parsed, ta.author_subcategory_posted, tt.id, tt.title_ws_as_uploaded,
                     tt.author_id, tt.pageauthor_subcategory_added).outerjoin(ta).where(
        ta.is_author == 1,
        tt.uploaded == 1,
        tt.pageauthor_subcategory_added == 0,
        # tt.id == 93258,
    )
    # res = db.db_.s.execute(stmt).fetchall()
    while True:
        res = db.db_.s.execute(stmt).fetchmany(size=5)
        for r in res:
            x = dict(r)
            d = D.parse_obj(x)
            process_page(d)
        if len(res) == 0:
            print('db: not found rows to work')
            break


class AddCategorytreeOnAuthorPages:
    def db_set_author_subcategory_tpl_posted(self, db_author_id):
        stmt = sa.update(ta).values({ta.author_subcategory_tpl_posted: 1}).where(ta.id == db_author_id)
        db.db_.s.execute(stmt)
        db.db_.s.commit()

    def add_categorytree_on_author_pages_process(self, pagename: str):
        print(f'{pagename=}')

        if pagename is None:
            print('pagename is None')
            return

        page = pwb.Page(SITE, pagename)
        while not page.exists():
            try:
                page = page.moved_target()
            except pwb.exceptions.NoMoveTargetError as e:
                print('no page, NoMoveTargetError')
                # stmt = sa.update(tt).values({tt.pageauthor_subcategory_added: -1}).where(tt.id == d.tid)
                # db.db_.s.execute(stmt)
                # db.db_.s.commit()
                return
            except Exception as e:
                print()
                pass

        if page.exists():
            # print(f'page exists {pagename=}')
            while page.isRedirectPage():
                print(f'page isRedirectPage, getRedirectTarget()')
                page = page.getRedirectTarget()

            text_new = page.text

            # db_update_author_pagename(d.author_id, page.title())

            tpl_name_prefix = '{{Импорт текстов/az.lib.ru/Список неразобранных страниц автора|подкатегория='

            # удалить некоррентные шаблоны categorytree со страницы, которые ссылаются на несущ. категории
            re_tpl_name_exist = re.compile(r'%s(.+?)}}' % tpl_name_prefix.replace('|', '\|'))
            for subcategory_author_pagename in re_tpl_name_exist.findall(text_new):
                subcategory_author_pagename_full = f'Категория:Импорт/az.lib.ru/{subcategory_author_pagename}'
                subcategory_author_page = pwb.Page(SITE, subcategory_author_pagename_full)
                if not subcategory_author_page.exists():
                    # удаляем категорию
                    tpl_name_full = r'%s%s}}' % (tpl_name_prefix, subcategory_author_pagename)
                    text_new = re.sub(r'\n\n%s\n' % tpl_name_full.replace('|', '\|'), '', text_new)

            tpl_name = '%s%s}}' % (tpl_name_prefix, pagename)  # d.author_pagename

            # добавить шаблон
            if tpl_name not in text_new:
                p_ = [f'({s})' for s in ('\n== Примечания', '\n== Ссылки', '\n{{АП', '== См. также') if s in page.text]
                pattern = p_[0] if p_ else '($)'
                text_new = re.sub(r'\n*' + pattern, r'\n\n%s\n\n\1' % tpl_name, text_new)

            summary = 'шаблон со списком страниц, [[Викитека:Проект:Импорт текстов/Lib.ru]]'
            page = posting_page(page, text_new, summary)

            if tpl_name in page.text:
                # self.db_set_author_subcategory_tpl_posted(d.author_id)
                return True

        else:
            print(f'is not exists {pagename=}')

    def worker(self):
        stmt = sa.select(ta.name_WS, ta.family_parsed, ta.author_subcategory_posted, ta.author_subcategory_tpl_posted,
                         tt.id, tt.title_ws_as_uploaded, tt.author_id, tt.pageauthor_subcategory_added) \
            .outerjoin(ta).where(
            ta.author_subcategory_posted == 1,
            ta.author_subcategory_tpl_posted == 0,
            tt.pageauthor_subcategory_added == 1,
            # tt.id == 93258,
        ).group_by(ta.name_WS)
        # res = db.db_.s.execute(stmt).fetchall()
        # for r in res:
        #     x = dict(r)
        #     d = D.parse_obj(x)
        #     self.add_categorytree_on_author_pages_process(d)

        # cc = list(pwb.Category(SITE, 'Категория:Импорт/lib.ru/Категории авторов').subcategories())
        for page in pwb.Category(SITE, 'Категория:Импорт/lib.ru/Категории авторов').subcategories():
            # for autor_pagename in ['Владимир Александрович Азов']:
            autor_pagename = page.title().replace('Категория:Импорт/az.lib.ru/', '')
            self.add_categorytree_on_author_pages_process(autor_pagename)


if __name__ == '__main__':
    # worker()

    w = AddCategorytreeOnAuthorPages()
    w.worker()
