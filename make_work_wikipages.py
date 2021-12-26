#!/usr/bin/env python3
from dataclasses import dataclass
import time
import re
import threading, queue
import pypandoc
import html as html_
import mwparserfromhell as mwp

import db
from get_parsed_html import get_html
from html2wiki import LibRu
from parser_html_to_wiki import *


@dataclass
class D:
    """
    from collections import namedtuple
    d = namedtuple('D', 'author title date translator source other text categories')
        """
    author: str = ''
    title: str = ''
    date: str = ''
    translator: str = ''
    source: str = ''
    other: str = ''
    text: str = ''
    categories: str = ''
    categories_string: str = ''
    wiki_title: str = ''

    # def __post_init__(self):
    #     self.name: str = self.to_upper(self.name)


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
| ДАТАПУБЛИКАЦИИ        = {d.date}
| ЯЗЫКОРИГИНАЛА         = 
| НАЗВАНИЕОРИГИНАЛА     = 
| ПОДЗАГОЛОВОКОРИГИНАЛА = 
| ПЕРЕВОДЧИК            = {d.translator}
| ДАТАПУБЛИКАЦИИОРИГИНАЛА = 
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


re_headers_check = re.compile(r'<center>(Глава.+?|II.?|\d+.?)</center>', flags=re.I)
re_refs_check = re.compile(r'(<sup>|\[\d|\{.+?\})', flags=re.I)


def categorization(text, r):
    conditions = [
        (re_headers_check.search(text), 'Страницы с не вики-заголовками'),
        (re_refs_check.search(text), 'Страницы с не вики-сносками'),
        ('[#' in text, 'Страницы с внутренними ссылками по анкорам'),
        ('pre' in text, 'Страницы с тегами pre'),
        ('<ref' in text, 'Страницы со сносками'),
        ('http' in text, 'Страницы с внешними ссылками'),
        ('http' in text, 'Страницы с внешними ссылками'),
        # ([e for pre in soup.find_all('pre') for e in pre.find_all('ref')], 'Теги ref внутри pre'),
    ]
    cats = [f'[[Категория:Импорт/lib.ru/{name}]]' for cond, name in conditions if cond]
    cats.append("[[Категория:Импорт/lib.ru]]")
    cats.append(f"[[Категория:{r['name']}]]")
    cats.append(f"[[Категория:Литература {r['year']} года]]")
    return '\n'.join(cats)


def make_wikititle(r):
    title = f"{r['title']} ({r['family_parsed_for_WS']})"
    if r['oo']:
        d.wiki_title += '/ДО'
    return title


def make_wikipages_to_db():
    for r in db.db_all_tables.find(db.db_all_tables.table.c.wiki.isnot(None)):
        d = D(author=r['name'],
              title=r['title'],
              date=r['year'],
              # translator=translator,
              source=r['text_url'],
              other=r['title_desc'],
              text=r['wiki'],
              # text=r['wikified'],
              )
        d.categories_string = categorization(d.text, r)
        d.wiki_title = make_wikititle(r)
        wikipage = make_wikipage(d)
        # db.db_htmls.upsert({'tid': r['tid'], 'wikified': d.text}, ['tid'])
        db.db_htmls.upsert({'tid': r['tid'], 'wiki_page': wikipage, 'wiki_title': d.wiki_title}, ['tid'], ensure=True)
        print()
