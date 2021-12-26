#!/usr/bin/env python3
from dataclasses import dataclass
import time
import re
import threading, queue
import pypandoc
import html as html_
import mwparserfromhell as mwp
import pywikibot as pwb

site = pywikibot.Site('ru', 'wikipedia')
page = pywikibot.Page(site, 'Page name')
textPage = page.get()


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


def make_wikititle(r):
    title = f"{r['title']} ({r['family_parsed_for_WS']})"
    if r['oo']:
        d.wiki_title += '/ДО'
    return title

def posting():
    for r in db.db_all_tables.find(db.db_all_tables.table.c.wiki_page.isnot(None)):

        # d = D(author=r['name'],
        #       title=r['title'],
        #       date=r['year'],
        #       # translator=translator,
        #       source=r['text_url'],
        #       other=r['title_desc'],
        #       text=r['wiki'],
        #       # text=r['wikified'],
        #       )

        page = pwb.Page(title=r['wiki_title'])
        if page.exists():
            page.title +='/Дубль'
            page.text = r['wiki_page'] + '\n[[Категория:Импорт/lib.ru/Дубли имён существующих страниц]]'
        else:
            page.text = r['wiki_page']

        d.categories_string = categorization(d.text, r)
        d.wiki_title = make_wikititle(r)
        wikipage = make_wikipage(d)
        # db.db_htmls.upsert({'tid': r['tid'], 'wikified': d.text}, ['tid'])
        db.db_wiki.upsert({'tid': r['tid'], 'wiki_page': wikipage, 'wiki_title': d.wiki_title}, ['tid'], ensure=True)
        print()
