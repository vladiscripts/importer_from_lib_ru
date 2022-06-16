#!/usr/bin/env python3
from dataclasses import dataclass
import time
import re
from urllib.parse import urlsplit
import threading, queue
from pathlib import Path
from typing import Optional, Union, Tuple, List, NamedTuple
import pypandoc
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra, dataclasses
import html as html_
import mwparserfromhell as mwp

import db_schema as db
from get_parsed_html import get_html
from html2wiki import LibRu
from parser_html_to_wiki import *
from parse_works_metadata_from_htmls_to_db import Category, CategoryText


class CategoriesbyAuthors(BaseModel):
    id: int
    name_site: str
    name_ws: str
    text_cat_by_author: Optional[str]
    text_lang_by_author: Optional[str]


class CommonData:
    # categories_authors_cached = [CategoriesbyAuthors.parse_obj(r) for r in db.authors_categories.find()]
    # categories_authors_cached = {(r['cname_site'], r['cname_ws']) for r in db.authors_categories.all()}
    categories_authors_cached = {r['cname_site']: r['cname_ws'] for r in db.authors_with_cat.all()}


class A(BaseModel):
    author_id: int = Field(alias='id')
    # slug_author: str  # = Field(alias='slug_author') = Field(alias='uploaded_author') = Field(alias='do_uploaded_author')
    slug: str
    name: str
    family_parsed: str
    names_parsed: Optional[str]
    name_WS: str
    pagename_as_uploaded: Optional[str]
    live_time: Optional[str]
    born: str = ''
    dead: str = ''
    year_dead: Optional[str] = ''
    town: Optional[str]
    desc: Optional[str]
    litarea: Optional[str]
    # author_cat: Optional[str]
    cname_ws: Optional[str] = Field(alias='name_ws')
    categories: list = []
    image_url_filename: Optional[str]
    image_filename_wiki: Optional[str]
    is_author: bool
    uploaded: bool
    do_upload: bool
    already_created: bool
    # uploaded_author: bool
    # do_uploaded_author: bool

    class Config:
        extra = Extra.allow

    def __init__(self, **d):
        if t := d.get('live_time'):
            d["born"], _, d["dead"] = [x.strip() for x in t.partition('—')]
        if f := d.get('image_url_filename'):
            d['image_filename_wiki'] = d['slug'].split('/')[-1] + Path(f).suffix
        if n := d.get('pagename_as_uploaded'):
            d['name_WS'] = n
        super().__init__(**d)

    def make_wikipage(self):
        # self.clean_desc()
        # self.set_dates_published()
        # self.text_replaces()
        self.categorization(CommonData)
        self.fill_wikipage_template()

    def fill_wikipage_template(self):
        # imported/lib.ru - Данный текст импортирован с сайта lib.ru. Он может содержать ошибки распознавания и оформления.
        # После исправления ошибок просьба убрать данный шаблон-предупреждение.

        # titles = [r['title'] for r in db.titles.find(author_id=)]

        self.wikipage_text = f"""\
{{{{Обавторе
|ФАМИЛИЯ       = {self.family_parsed}
|ИМЕНА         = {self.names_parsed or ''}
|ВАРИАНТЫИМЁН  = 
|ОПИСАНИЕ      = {self.desc or ''}
|ДРУГОЕ        = 
|ДАТАРОЖДЕНИЯ  = {self.born or ''}
|МЕСТОРОЖДЕНИЯ = {self.town or ''}
|ДАТАСМЕРТИ    = {self.dead or ''}
|МЕСТОСМЕРТИ   = 
|ИЗОБРАЖЕНИЕ   = {self.image_filename_wiki or ''}
|ВИКИПЕДИЯ     = 
|ВИКИЦИТАТНИК  = 
|ВИКИСКЛАД     = 
|ВИКИВИДЫ      =
|ВИКИНОВОСТИ   =
|ВИКИЛИВРУ     = 
|ЭСБЕ          = 
|Google        = 
|НЕОДНОЗНАЧНОСТЬ = 
}}}}

== Произведения ==


{{{{АП|ГОД={self.year_dead} |ГОДРЕАБИЛИТАЦИИ= |ВОВ= }}}}

{self.categories_string}
"""

    def categorization(self, C):
        # conditions = [
        #     ('[#' in text, 'Страницы с внутренними ссылками по анкорам'),
        #     ('pre' in text, 'Страницы с тегами pre'),
        #     ('<ref' in text, 'Страницы со сносками'),
        #     ('http' in text, 'Страницы с внешними ссылками'),
        #     ([e for pre in soup.find_all('pre') for e in pre.find_all('ref')], 'Теги ref внутри pre'),
        # ]
        # cats = [f'[[Категория:Импорт/lib.ru/{name}]]' for cond, name in conditions if cond]

        cats = [
            # f'{self.name_WS}|{self.family_parsed}',
            self.cname_ws,
            f'Импорт/lib.ru/Авторы',
        ]
        if c := C.categories_authors_cached.get(self.litarea):
            cats.append(c)

        # for c in C.categories_authors_cached:
        #     if c.name_site == self.litarea:
        #         cats.append(c.name_ws)
        # cats = [c.name_ws for c in C.categories_authors_cached if c.name_site == self.litarea]

        self.categories = cats
        self.categories_string = '\n'.join([f'[[Категория:{c}]]' for c in cats])


# def main():
#     t = db.authors_with_cat
#     cols = t.table.c
#     # for r in t.find(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None)):
#     for r in t.find():
#         a = A.parse_obj(r)
#         a.make_wikipage()


if __name__ == '__main__':
    # main()
    pass
