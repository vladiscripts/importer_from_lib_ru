#!/usr/bin/env python3
from dataclasses import dataclass
import datetime as dt
import time
import re
from urllib.parse import urlsplit
import threading, queue
from typing import Optional, Union, Tuple, List, NamedTuple
import pypandoc
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra, dataclasses
import html as html_
import mwparserfromhell as mwp

from db import *
from converter_html_to_wiki.get_parsed_html import get_html
from converter_html_to_wiki.html2wiki import LibRu
from converter_html_to_wiki.parser_html_to_wiki import *
from converter_html_to_wiki.parse_works_metadata_from_htmls_to_db import Category, CategoryText, D

categories_cached: list = None
categories_authors_cached: list = None
cid_translation: int = None


class CategoriesbyAuthors(BaseModel):
    cid: int
    name_site: str
    name_ws: str
    text_cat_by_author: Optional[str]
    text_lang_by_author: Optional[str]


class CommonData:
    categories_cached = [Category.parse_obj(r) for r in dbd.texts_categories_names.find()]
    categories_authors_cached = [CategoriesbyAuthors.parse_obj(r) for r in dbd.authors_categories.find()]
    cid_translation = dbd.texts_categories_names.find_one(name='Переводы')


class X(D):
    wikified: str
    oo: bool
    title_ws_proposed: Optional[str]
    title_ws_as_uploaded: Optional[str]
    renamed_manually: Optional[bool]
    author_tag: Optional[str]
    lang: Optional[str]
    year_dead: Optional[int]
    date_original: str = ''
    date_translate: str = ''
    size: Optional[int]
    desc: Optional[str] = Field(..., alias='text_desc_wikified')
    categories: list = []
    wikipage_text: str = ''
    family_parsed: Optional[str]
    time_update: Optional[dt.datetime]
    is_author: Optional[bool]
    is_already_this_title_in_ws: Optional[bool]

    class Config:
        extra = Extra.allow
        orm_mode = True

    def make_wikipage(self):
        self.clean_desc()
        self.set_dates_published()
        self.text_replaces()
        self.categorization(CommonData)
        self.fill_wikipage_template()

    def set_dates_published(self):
        if self.lang:
            self.date_original = self.year
        else:
            self.date_published = self.year

    def text_replaces(self):
        self.wikified = self.wikified.replace('<sup></sup>', '').replace('<sub></sub>', '')

    def clean_desc(self):
        if v := self.desc:
            v = re.sub(r'^<dd><font[^>]*>(.+?)</font></dd>$', r'\1', v, flags=re.DOTALL)
            v = re.sub(r'<dd><small><a[^>]+>Иллюстрации/приложения:.+?</a></small></dd>$', '', v, flags=re.DOTALL)

            # href → викиссылки авторов
            # desc = '<dd><font color="#555555"><i>Перевод <a href="http://az.lib.ru/b/blinowa_e_m/">Елизаветы Блиновой</a> и <a href="http://az.lib.ru/k/kuzmin_m_a/">Михаила Кузмина</a> (1913).</i></font></dd><dd><small><a href="/img/g/gurmon_r_d/text_02_emil_verharn/index.shtml">Иллюстрации/приложения: 1 шт.</a></small></dd>'
            for a in re.findall(r'(<a[^>]*?href="[^"]+".*?>.*?</a>)', v):
                m = re.search(r'<a[^>]*?href="([^"]+)".*?>(.*?)</a>', a)
                href_slug = urlsplit(m.group(1)).path.rstrip('/')
                db_a = dbd.authors.find_one(slug=href_slug)
                if db_a:
                    a_new = f"[[{db_a['name_WS']}|{m.group(2)}]]"
                else:
                    a_new = m.group(2)
                v = v.replace(a, a_new)
        self.desc = v

    def categorization(self, C):
        re_headers_check = re.compile(r'<center>(Глава.+?|II.?|\d+.?)</center>', flags=re.I)
        re_refs_check = re.compile(r'(<sup>|\[\d|\{.+?\})')

        conditions = [
            (re_headers_check.search(self.wikified), 'Страницы с не вики-заголовками'),
            (re_refs_check.search(self.wikified), 'Страницы с не вики-сносками или с тегом sup'),
            ('[#' in self.wikified, 'Страницы с внутренними ссылками по анкорам'),
            ('<pre>' in self.wikified, 'Страницы с тегами pre'),
            ('<ref' in self.wikified, 'Страницы со сносками'),
            ('http' in self.wikified, 'Страницы с внешними ссылками'),
            ('ru.wikisource.org/wiki' in self.wikified, 'Страницы с внешними ссылками на Викитеку'),
            # ([e for pre in soup.find_all('<pre>') for e in pre.find_all('ref')], 'Теги ref внутри pre'),
        ]

        if self.desc:
            conditions += [
                ('перевод' in self.desc.lower(), 'Указан переводчик в параметре ДРУГОЕ'),
                (self.translator and 'перевод' in self.translator.lower(),
                 'Слово "перевод" в параметре имени переводчика'),
                ('<i>' in self.desc, 'Тег i в параметре ДРУГОЕ'),
                ('<b>' in self.desc, 'Тег b в параметре ДРУГОЕ'),
                ('<br' in self.desc, 'Тег br в параметре ДРУГОЕ'),
                # ('<br' in self.desc, 'Указан переводчик и нет категории перевода')
                (len(self.desc) > 100, 'Длина текста в параметре ДРУГОЕ более 100'),
                (len(self.desc) > 200, 'Длина текста в параметре ДРУГОЕ более 200'),
                (len(self.desc) > 300, 'Длина текста в параметре ДРУГОЕ более 300'),
                (len(self.desc) > 400, 'Длина текста в параметре ДРУГОЕ более 400'),
                ('<a ' in self.desc, 'Тег a в параметре ДРУГОЕ'),
                (self.size and self.size > 500, 'Длина текста более 500 Кб'),
                (self.size and self.size > 1000, 'Длина текста более 1000 Кб'),
                (self.is_already_this_title_in_ws, 'Есть одноимённая страница, проверить на дубль'),
            ]

        cats = [name for cond, name in conditions if cond]

        has_cat_translation = dbd.texts_categories.find_one(category_id=C.cid_translation, tid=self.tid)
        if not self.translator and has_cat_translation:
            cats.append(f'Не указан переводчик и есть категория перевода')
        if self.translator and not has_cat_translation:
            cats.append(f'Указан переводчик и нет категории перевода')

        if self.author_tag:
            if author_ := re.search(r'<a .+?>\s*(.*?)\s*</a>', self.author_tag):
                if author_.group(1) not in (self.litarea, self.name):  # todo: name or self.name_WS?
                    cats.append('Возможна ошибка указания автора')

        if self.title_ws_as_uploaded and '/Версия ' in self.title_ws_as_uploaded:
            cats.append('Есть одноимённая страница не имевшаяся ранее, проверить на дубль и переименовать')

        if not self.is_author:
            cats.append('Возможна ошибка распознавания имени автора')

        if re.search(r'[\[\]]', self.title):
            cats.append('Некорректные символы в заглавии')

        if len(self.title.encode()) >= 255:
            cats.append('Слишком длинный заголовок')

        cats = [f'Импорт/lib.ru/{c}' for c in cats]

        cats_from_db = [c.name_ws for r in dbd.texts_categories.find(tid=self.tid) for c in C.categories_cached
                        if c.cid == r['category_id']]
        cats.extend(cats_from_db)

        for c in C.categories_authors_cached:
            if c.name_ws == self.litarea:
                if c.text_lang_by_author:
                    c.text_lang_by_author = self.lang
                if c.text_cat_by_author:
                    cats.append(c.text_cat_by_author)

        cats.append(f'{self.name_WS}')
        cats.append(f'Литература {self.year} года')

        if self.oo:
            cats.append(f'Дореформенная орфография')

        cats.append('Импорт/lib.ru')

        self.categories = cats
        self.categories_string = '\n'.join([f'[[Категория:{c}]]' for c in cats])

    def fill_wikipage_template(self):
        self.wikipage_text = f"""\
{{{{imported/lib.ru}}}}
{{{{Отексте
| АВТОР                 = {self.name_WS}
| НАЗВАНИЕ              = {self.title}
| ПОДЗАГОЛОВОК          = 
| ЧАСТЬ                 = 
| СОДЕРЖАНИЕ            = 
| ИЗЦИКЛА               = 
| ИЗСБОРНИКА            = 
| ДАТАСОЗДАНИЯ          = 
| ДАТАПУБЛИКАЦИИ        = {self.date_translate if self.lang else self.date_published}
| ЯЗЫКОРИГИНАЛА         = {self.lang or ''}
| НАЗВАНИЕОРИГИНАЛА     = 
| ПОДЗАГОЛОВОКОРИГИНАЛА = 
| ПЕРЕВОДЧИК            = {self.translator or ''}
| ДАТАПУБЛИКАЦИИОРИГИНАЛА = {self.date_original if self.lang else ''}
| ИСТОЧНИК              = [{self.text_url} az.lib.ru]
| ВИКИДАННЫЕ            = <!-- id элемента темы -->
| ВИКИПЕДИЯ             = 
| ВИКИЦИТАТНИК          = 
| ВИКИНОВОСТИ           = 
| ВИКИСКЛАД             = 
| ДРУГОЕ                = {self.desc or ''}
| ОГЛАВЛЕНИЕ            = 
| ПРЕДЫДУЩИЙ            = 
| СЛЕДУЮЩИЙ             = 
| КАЧЕСТВО              = 1 <!-- оценка по 4-х бальной шкале -->
| НЕОДНОЗНАЧНОСТЬ       = 
| ДРУГИЕПЕРЕВОДЫ        = 
| ЛИЦЕНЗИЯ              = PD-old
| СТИЛЬ                 = text
}}}}
<div class="text">
{self.wikified}
</div>

{self.categories_string}
"""

# test
def make_wikipage(r) -> str:
    d = X.parse_obj(r)
    d.make_wikipage()
    return d.wikipage_text


# test
def make_wikipages_to_db():
    ta = dbd.all_tables
    col = ta.table.c
    res = ta.find(col.wikified.isnot(None), col.title_ws.isnot(None), col.year <= 2022 - 4 - 71, do_upload=True,
                  _limit=10)
    # res = list(res)
    for r in res:
        wikipage = make_wikipage(r)
        # db.db_htmls.upsert({'tid': d.tid, 'wikified': d.text}, ['tid'])
        # db.htmls.upsert({'tid': d.tid, 'wiki_page': wikipage, 'wiki_title': d.wiki_title}, ['tid'], ensure=True)
        print()


if __name__ == '__main__':
    pass
    # wiki_text = make_wikipages_to_db()
