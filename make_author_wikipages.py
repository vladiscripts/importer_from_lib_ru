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
from html2wiki import LibRu, categorization
from parser_html_to_wiki import *


def make_wikipage(r):
    # imported/lib.ru - Данный текст импортирован с сайта lib.ru. Он может содержать ошибки распознавания и оформления.
    # После исправления ошибок просьба убрать данный шаблон-предупреждение.

    born, death = r['live_time'].split('—') if r['live_time'] else '', ''

    cats = [
        f"[[Категория:{r['name']}]]",
        f'[[Категория:Импорт/lib.ru/авторы/{name}]]',
    ]
    categories_string = '\n'.join(cats)

    wiki_text = \
        f"""{{{{imported/lib.ru}}}}
{{{{Обавторе
|НЕОДНОЗНАЧНОСТЬ = 
|ФАМИЛИЯ = {r['family_parsed_for_WS']}        
|ИМЕНА = {r['names_parsed_for_WS']}
|ВАРИАНТЫИМЁН = 
|ОПИСАНИЕ = {r['author_desc']}
|ДРУГОЕ = 
|ДАТАРОЖДЕНИЯ = {born}
|МЕСТОРОЖДЕНИЯ = {r['town']}
|ДАТАСМЕРТИ = {death}
|МЕСТОСМЕРТИ = 
|ИЗОБРАЖЕНИЕ = {r['author_image']}
|ВИКИПЕДИЯ = 
|ВИКИЦИТАТНИК = 
|ВИКИСКЛАД = 
|ВИКИВИДЫ =
|ВИКИНОВОСТИ =
|ВИКИЛИВР = 
|ВИКИЛИВРУ = 
|ЭСБЕ = 
|Google = 
}}}}

== Произведения ==


{{{{PD-old}}}}

{categories_string}
"""
    return text


def categorization(text, soup, r):
    conditions = [
        ('[#' in text, 'Страницы с внутренними ссылками по анкорам'),
        ('pre' in text, 'Страницы с тегами pre'),
        ('<ref' in text, 'Страницы со сносками'),
        ('http' in text, 'Страницы с внешними ссылками'),
        ([e for pre in soup.find_all('pre') for e in pre.find_all('ref')], 'Теги ref внутри pre'),
    ]
    cats = [f'[[Категория:Импорт/lib.ru/{name}]]' for cond, name in conditions if cond]

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
