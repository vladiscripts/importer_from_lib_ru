#!/usr/bin/env python3
from dataclasses import dataclass
import time
import re
import threading, queue
from typing import Optional, Union, Tuple, List, NamedTuple
import pypandoc
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra,dataclasses
import html as html_
import mwparserfromhell as mwp

import db
from get_parsed_html import get_html
from html2wiki import LibRu
from parser_html_to_wiki import *
from parse_works_metadata_from_htmls_to_db import Category, CategoryText, D

categories_cached: list = None
categories_authors_cached: list = None
cid_translation: int = None

class CategoriesbyAuthors(BaseModel):
    id: int
    name_site: str
    name_ws: str
    text_cat_by_author: Optional[str]
    text_lang_by_author: Optional[str]

class X(D):
    oo: bool
    title_ws: str
    desc: str = Field(..., alias='text_desc_raw')
    author_tag: Optional[str]
    lang: str = ''
    year_dead: Optional[int]
    date_original: str = ''
    date_translate: str = ''
    size: Optional[int]
    categories: list = []

    class Config:
        extra = Extra.allow

    # @validator('oo', check_fields=False)
    # @validator('oo')
    # def make_wikititle(cls, v):
    #     if v:
    #         self.wiki_title += '/ДО'
    # return wiki_title

    @validator('desc')
    def clean_desc(cls, v):
        v = re.sub(r'^<dd><font[^>]*>(.+?)</font></dd>$', r'\1', v, flags=re.DOTALL)
        v = re.sub(r'<dd><small><a[^>]+>Иллюстрации/приложения:.+?</a></small></dd>$', '', v, flags=re.DOTALL)

        # desc = '<dd><font color="#555555"><i>Перевод <a href="http://az.lib.ru/b/blinowa_e_m/">Елизаветы Блиновой</a> и <a href="http://az.lib.ru/k/kuzmin_m_a/">Михаила Кузмина</a> (1913).</i></font></dd><dd><small><a href="/img/g/gurmon_r_d/text_02_emil_verharn/index.shtml">Иллюстрации/приложения: 1 шт.</a></small></dd>'
        for a in re.findall(r'(<a[^>]*?href="[^"]+".*?>.*?</a>)', v):
            m = re.search(r'<a[^>]*?href="([^"]+)".*?>(.*?)</a>', a)
            href_slug = urlsplit(m.group(1)).path.rstrip('/')
            db_a = db.authors.find_one(slug=href_slug)
            if db_a:
                a_new = f'[[{db_a.name_ws}|{m.group(2)}]]'
            else:
                a_new = m.group(2)
            v = v.replace(a, a_new)

        return v

    # def make_wikititle(self):
    #     if self.oo and self.title_ws:
    #         self.title_ws += '/ДО'

    @root_validator
    def change_values(cls, values: dict):
        date = values.get('date')
        lang = values.get('lang')
        if date:
            if lang:
                values['date_original'] = date
            else:
                values['date_translate'] = date

            if values['oo'] and values['title_ws']:
                values['title_ws'] += '/ДО'

        return values

    def categorization(self, C):
        re_headers_check = re.compile(r'<center>(Глава.+?|II.?|\d+.?)</center>', flags=re.I)
        re_refs_check = re.compile(r'(<sup>|\[\d|\{.+?\})', flags=re.I)

        conditions = [
            (re_headers_check.search(self.wiki), 'Страницы с не вики-заголовками'),
            (re_refs_check.search(self.wiki), 'Страницы с не вики-сносками или с тегом sup'),
            ('[#' in self.wiki, 'Страницы с внутренними ссылками по анкорам'),
            ('pre' in self.wiki, 'Страницы с тегами pre'),
            ('<ref' in self.wiki, 'Страницы со сносками'),
            ('http' in self.wiki, 'Страницы с внешними ссылками'),
            # ([e for pre in soup.find_all('pre') for e in pre.find_all('ref')], 'Теги ref внутри pre'),
        ]

        if self.desc:
            conditions += [
                ('перевод' in self.desc.lower(), 'Указан переводчик в параметре ДРУГОЕ'),
                ('<i>' in self.desc, 'Таг i в параметре ДРУГОЕ'),
                ('<b>' in self.desc, 'Таг b в параметре ДРУГОЕ'),
                ('<br' in self.desc, 'Таг br в параметре ДРУГОЕ'),
                # ('<br' in self.desc, 'Указан переводчик и нет категории перевода')
                (len(self.desc) > 100, 'Длина текста в параметре ДРУГОЕ > 100'),
                (len(self.desc) > 200, 'Длина текста в параметре ДРУГОЕ > 200'),
                (len(self.desc) > 300, 'Длина текста в параметре ДРУГОЕ > 300'),
                (len(self.desc) > 400, 'Длина текста в параметре ДРУГОЕ > 400'),
                ('<a ' in self.desc, 'Таг a в параметре ДРУГОЕ'),
                (self.size and self.size > 500, 'Длина текста > 500'),
                (self.size and self.size > 1000, 'Длина текста > 1000'),
                (self.is_same_title_in_ws_already, 'Есть одноимённая страница, проверить на дубль'),
            ]

        cats = [name for cond, name in conditions if cond]

        has_cat_translation = db.texts_categories.find_one(category_id=C.cid_translation, tid=self.tid)
        if not self.translator and has_cat_translation:
            cats.append(f'Не указан переводчик и есть категория перевода')
        if self.translator and not has_cat_translation:
            cats.append(f'Указан переводчик и нет категории перевода')

        if self.author_tag:
            if author_ := re.search(r'<a .+?>\s*(.*?)\s*</a>', self.author_tag):
                if author_.group(1) not in (self.litarea, self.name):  # todo: name or self.name_WS?
                    cats.append('Возможна ошибка указания автора')

        if '/Версия ' in self.title_ws:
            cats.append('Есть одноимённая страница не имевшаяся ранее, проверить на дубль и переименовать')

        cats = [f'Импорт/lib.ru/{c}' for c in cats]
        cats.append('Импорт/lib.ru')

        cats_from_db = [c.name_ws for r in db.texts_categories.find(tid=self.tid) for c in C.categories_cached
                        if c.cid == r['category_id']]
        cats.extend(cats_from_db)

        for c in C.categories_authors_cached:
            if c.name_ws == self.litarea:
                if c.lang:
                    c.lang = self.lang
                if c.text_cat_by_author:
                    cats.append(c.text_cat_by_author)

        cats.append(f'{self.name_WS}')
        cats.append(f'Литература {self.year} года')

        self.categories = [f'[[Категория:{c}]]' for c in cats]
        self.categories_string = '\n'.join(self.categories)


def make_wikipage(d):
    # imported/lib.ru - Данный текст импортирован с сайта lib.ru. Он может содержать ошибки распознавания и оформления.
    # После исправления ошибок просьба убрать данный шаблон-предупреждение.

    wiki_text = \
        f"""{{{{imported/lib.ru}}}}
{{{{Отексте
| АВТОР                 = {d.author}
| НАЗВАНИЕ              = {d.title}
| ПОДЗАГОЛОВОК          = 
| ЧАСТЬ                 = 
| СОДЕРЖАНИЕ            = 
| ИЗЦИКЛА               = 
| ИЗСБОРНИКА            = 
| ДАТАСОЗДАНИЯ          = 
| ДАТАПУБЛИКАЦИИ        = {d.date_translate if d.lang else d.date_original}
| ЯЗЫКОРИГИНАЛА         = {d.lang}
| НАЗВАНИЕОРИГИНАЛА     = 
| ПОДЗАГОЛОВОКОРИГИНАЛА = 
| ПЕРЕВОДЧИК            = {d.translator}
| ДАТАПУБЛИКАЦИИОРИГИНАЛА = {d.date_original if d.lang else ''}
| ИСТОЧНИК              = [{d.source} lib.ru]
| ВИКИДАННЫЕ            = <!-- id элемента темы -->
| ВИКИПЕДИЯ             = 
| ВИКИЦИТАТНИК          = 
| ВИКИНОВОСТИ           = 
| ВИКИСКЛАД             = 
| ДРУГОЕ                = {d.other}
| ПРЕДЫДУЩИЙ            = 
| СЛЕДУЮЩИЙ             = 
| КАЧЕСТВО              = 2 <!-- оценка по 4-х бальной шкале -->
| НЕОДНОЗНАЧНОСТЬ       = 
| ДРУГИЕПЕРЕВОДЫ        = 
| ЛИЦЕНЗИЯ              = {{{{PD-old}}}}
| СТИЛЬ                 = text
}}}}
<div class="text">
{d.text}
</div>

{d.categories_string}
"""
    return wiki_text


class CommonData:
    categories_cached = [Category.parse_obj(r) for r in db.texts_categories_names.find()]
    categories_authors_cached = [CategoriesbyAuthors.parse_obj(r) for r in db.authors_categories.find()]
    cid_translation = db.texts_categories_names.find_one(name='Переводы')


def make_wikipages_to_db():
    ta = db.all_tables
    col = ta.table.c
    g = [r for r in ta.find(col.wiki.isnot(None), col.do_upload.is_true(), col.title_ws.isnot(None), col.year <= 2022-71,
                            _limit=10)]
    for r in g:
        d = X.parse_obj(r)
        # d.make_wikititle()
        # d.wiki_title = make_wikititle(r)
        d.categorization(CommonData)
        wikipage = make_wikipage(d)
        # db.db_htmls.upsert({'tid': d.tid, 'wikified': d.text}, ['tid'])
        # db.htmls.upsert({'tid': d.tid, 'wiki_page': wikipage, 'wiki_title': d.wiki_title}, ['tid'], ensure=True)
        print()


if __name__ == '__main__':
    make_wikipages_to_db()