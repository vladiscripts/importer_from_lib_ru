#!/usr/bin/env python3
import time
import re
import threading, queue
from pathlib import Path
from typing import Optional, Union, Tuple, List
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import sqlalchemy.exc
import html as html_
import mwparserfromhell as mwp

import db
# from get_parsed_html import get_html
# from html2wiki import LibRu, categorization
# from parser_html_to_wiki import *


class A(BaseModel):
    author_id: int
    slug_author: str
    name: str
    family_parsed: Optional[str]
    names_parsed: Optional[str]
    name_WS: str
    live_time: Optional[str]
    born: str = ''
    dead: str = ''
    town: Optional[str]
    author_desc: Optional[str]
    litarea: Optional[str]
    author_cat: Optional[str]
    categories: Optional[str]
    image_url_filename: Optional[str]
    image_filename_wiki: Optional[str]
    is_author: bool
    uploaded_author: bool

    class Config:
        extra = Extra.allow

    def __init__(self, **d):
        if isinstance(t, str):
            d["born"], d["dead"] = [x.strip() for x in a.live_time.partition('—')]
        super().__init__(**d)


def set_categories(a):
    cats = [
        f"[[Категория:{a.name}]]",
        f'[[Категория:Импорт/lib.ru/авторы/{a.name}]]']
    return '\n'.join(cats)


def make_wikipage(a):
    # imported/lib.ru - Данный текст импортирован с сайта lib.ru. Он может содержать ошибки распознавания и оформления.
    # После исправления ошибок просьба убрать данный шаблон-предупреждение.

    # titles = [r['title'] for r in db.titles.find(author_id=)]

    wiki_text = f"""\
{{{{imported/lib.ru}}}}
{{{{Обавторе
|ФАМИЛИЯ = {a.family_parsed}        
|ИМЕНА = {a.names_parsed}
|ВАРИАНТЫИМЁН = 
|ОПИСАНИЕ = {a.author_desc}
|ДРУГОЕ = 
|ДАТАРОЖДЕНИЯ = {a.born}
|МЕСТОРОЖДЕНИЯ = {a.town}
|ДАТАСМЕРТИ = {a.dead}
|МЕСТОСМЕРТИ = 
|ИЗОБРАЖЕНИЕ = {a.image_filename_wiki}
|ВИКИПЕДИЯ = 
|ВИКИЦИТАТНИК = 
|ВИКИСКЛАД = 
|ВИКИВИДЫ =
|ВИКИНОВОСТИ =
|ВИКИЛИВРУ = 
|ЭСБЕ = 
|Google = 
|НЕОДНОЗНАЧНОСТЬ = 
}}}}

== Произведения ==
{titles}

{{{{PD-old}}}}

{categories_string}
"""
    return text


def categorization(text, a):
    # conditions = [
    #     ('[#' in text, 'Страницы с внутренними ссылками по анкорам'),
    #     ('pre' in text, 'Страницы с тегами pre'),
    #     ('<ref' in text, 'Страницы со сносками'),
    #     ('http' in text, 'Страницы с внешними ссылками'),
    #     ([e for pre in soup.find_all('pre') for e in pre.find_all('ref')], 'Теги ref внутри pre'),
    # ]
    # cats = [f'[[Категория:Импорт/lib.ru/{name}]]' for cond, name in conditions if cond]
    cats = []
    cats.append('[[Категория:Импорт/lib.ru/Страницы авторов]]')
    if a.author.cat:
        cats.append(a.author.cat)

    return '\n'.join(cats)


def make_wikipages_to_db():
    for r in db.db_ahtmls.find(db.htmls.table.c.wiki.isnot(None)):
        d = D(author=r['name'],
              title=r['title'],
              date=r['year'],
              # translator=translator,
              source=r['text_url'],
              other=r['title_desc'],
              text=r['wiki'],
              )
        d['categories_string'] = categorization(text, parser.soup, r)
        text = make_wikipage(d)
        db.htmls.upsert({'tid': r['tid'], 'wikified': d.text}, ['tid'])


def main():
    t = db.authors_with_cat
    cols = t.table.c
    # for r in t.find(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None)):
    for r in t.find():
        a = A.parse_obj(r)
        print()
        a.categories = set_categories(a)
        text = make_wikipage(a)


class Titles(BaseModel):
    __tablename__ = 'titles'
    id: int
    slug: str
    author_id: int
    year: str
    size: str
    title: str
    desc: str
    oo: bool
    uploaded: bool
    do_upload: bool
    text_url: str
    title_ws: str

if __name__ == '__main__':
    # main()
    pass