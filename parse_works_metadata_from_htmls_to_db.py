#!/usr/bin/env python3
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import json
import time
import sqlite3
import os
from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass
import re
import parsel
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
import html as html_

import db


@dataclass
class D:
    """
    from collections import namedtuple
    d = namedtuple('D', 'author title date translator source other text categories')
        """
    author: str = ''
    categories = []
    year = ''
    desc = ''


def add_categories(categories, d):
    categories_to_add = []
    for slug_d, name_d in d.categories:
        for cid, slug, name in categories:
            if slug == slug_d:
                break
        else:
            db.texts_categories_names.insert({'slug': slug_d, 'name': name_d}, ensure=True)
            cid = db.texts_categories_names.find_one(slug=slug_d)['id']
            categories.append([cid, slug_d, name_d])
        categories_to_add.append({'tid': r['tid'], 'category_id': cid})

    db.texts_categories.insert_many(categories_to_add, ensure=True)
    print()


class Parse_metadata_from_html:
    # def parse_metadata_from_html(r):
    d = D()

    def get_li_block(self, desc_block) -> Optional[tuple]:
        # for ul in desc_block.find_all('ul'):
        #     for _ in ul.find_all(text=lambda x: isinstance(x, NavigableString) and 'Оставить комментарий' in x):
        ul = desc_block.find_all('ul')[0]
        li = ul.find_all('li')
        if len(li) not in [5, 6]:
            raise RuntimeError('tid:', self.r['tid'], 'длинна <li> описания не равна 6')
        else:
            return li[1], li[2], li[4]  # author_line, year_line, categories_line

    def get_annotation_blocks(self, desc_block):
        for e in desc_block.find_all(text=lambda x: isinstance(x, NavigableString) and 'Аннотация:' in x):
            li = e.find_parent('li')
            z = e
            if e.parent.name == 'font':
                z = e.parent
                if e.parent.parent.name == 'b':
                    z = e.parent.parent
            z.extract()
            li.find('i').unwrap()
            return li

    def parse_year(self, year_line):
        for y in year_line.find_all(text=lambda x: isinstance(x, NavigableString) and 'Год:' in x):
            self.d.year = y.text.split('Год:')[1].strip()
            break

    def parse_annotation(self, annotation_):
        # todo
        self.d.desc = annotation_.text
        # self.d.year=str(annotation_)

    def parse_categories(self, categories_line):
        if x := categories_line.find('a'):
            if x.attrs.get('href', '').startswith("/type/"):
                self.d.categories = [(a.attrs.get('href'), a.text) for a in categories_line.find_all('a')
                                     if a.attrs.get('href').startswith("/")]
        for e in categories_line.find_all('font', attrs={'size': '-1'}):
            e.unwrap()

    def parse_author(self, author_line):
        r = self.r

        """<li><a href="http://az.lib.ru/d/defo_d/">Дефо Даниель</a>
         (перевод: Я . . . . въ Л . . . . .нъ)
         (<u>yes@lib.ru</u>)
        </li>"""
        author_line_s = re.sub(r'\(?[\w.]+@\w+\.ru\)?', '', author_line.text)
        # if email := author_line.find(text=lambda x: isinstance(x, NavigableString) and '@lib.ru' in x):
        #     email.extract()
        for a in author_line.find_all('a'):
            if href := a.attrs.get('href'):
                if urlsplit(href).path.rstrip('/') == r['author_slug']:
                    self.d.author = r['name_for_WS']
                    break
            else:
                print('href != author_slug')
        else:
            self.d.author = re.search(r'(.+?)\s*\(перевод:\s*(.+?)\s*\)', author_line_s)
            print('нет <a> в строке автора')

        # if translator := re.search(r'\(перевод:\s*(.+?)\s*\)', author_line_s):
        #     d.translator = translator.group(1)

        # if email := author_line.find(text=lambda x: isinstance(x, NavigableString) and '@lib.ru' in x):
        #     email.extract()

    def parse_translator(self, annotation_):
        r = self.r
        """Перевод <a href="http://az.lib.ru/z/zhurawskaja_z_n/">Зинаиды Журавской</a>"""

        # for s in annotation_.descendants:
        #     if isinstance(s, NavigableString) and 'Перевод ' in s \
        #             and isinstance(s.next_element, Tag) and s.next_sibling.name == 'a':
        #         a = s.next_sibling

        # if translator := re.search(r'(.+?)\s*\(Перевод \s*(.+?)\s*\)', annotation_):
        #     d.translator = translator.group(1)

        a_ = [s.next_sibling for e in annotation_.find_all('i') for s in e.contents
              if isinstance(s, NavigableString) and 'Перевод ' in s
              and isinstance(s.next_element, Tag) and s.next_sibling.name == 'a']
        if a_:
            if href := a_[0].attrs.get('href'):
                if r := db.all_tables.find_one(author_slug=urlsplit(href).path.rstrip('/')):
                    self.d.translator = r['name_for_WS']
        else:
            d.author = re.search(r'(.+?)\s*\(перевод:\s*(.+?)\s*\)', author_line_s)
            print('нет <a> в строке автора')

    def parse(self, r):
        self.r = r
        soup = BeautifulSoup(r['html'], 'html5lib')

        desc_block = [t for t in soup.find_all('table') for e in t.find_all(
            text=lambda x: isinstance(x, Comment) and 'Блок описания произведения (слева вверху)' in x)][0]
        author_line, year_line, categories_line = self.get_li_block(desc_block)
        annotation_ = self.get_annotation_blocks(desc_block)

        self.parse_author(author_line)
        self.parse_translator(annotation_)
        self.parse_year(year_line)
        self.parse_categories(categories_line)

        self.parse_annotation(annotation_)

        return self.d


def parse_metadata_from_html_parsel(tid, html):
    """
    # todo
    """
    d = D()

    dom = parsel.Selector(html)

    desc_block = dom.xpath(
        '//table//comment()[contains(.,"Блок описания произведения (слева вверху")]/ancestor::table')
    info_li = desc_block.xpath('.//li/a[contains(.,"Оставить комментарий")]/ancestor::ul//li')
    if len(info_li) != 6:
        print('tid:', tid, 'длинна <li> описания не равна 6')
        return

    author_ = info_li[1]
    year_ = info_li[2]
    categories_ = info_li[3]

    annotation_ul = desc_block.xpath('./a[contains(.,"Аннотация:")]/ancestor::ul')

    categories_ = desc_block.xpath('.//a[starts-with(@href,"/type/")]/ancestor::li//a').css('a[href^="/"]')
    categories = [(z.css('::attr(href)').get(), z.css('::text').get()) for z in categories_]

    return d


def main():
    categories = [{'slug': (r['id'], r['slug'], r['name'])} for r in db.texts_categories_names.find()]

    # for r in db.db_all_tables.find(db.db_all_tables.table.c.wiki_page.isnot(None)):
    # for r in db.db_all_tables.find(wiki=None):
    # r = db.db_all_tables.find_one(tid=7487)
    tid = 7488  # http://az.lib.ru/d/defo_d/text_0014.shtml  В аннотации <i>Перевод <a href="http://az.lib.ru/z/zhurawskaja_z_n/">Зинаиды Журавской</a></i>
    r = db.all_tables.find_one(tid=tid)

    # d = parse_metadata_from_html(r)
    d = Parse_metadata_from_html().parse(r)
    # d = parse_metadata_from_html_parsel(r['tid'], r['html'])
    if not d:
        return

    db.htmls.update({'tid': r['tid'], 'author': d.author, 'year': d.year, 'desc': d.desc}, ['tid'], ensure=True)
    add_categories(categories, d)


if __name__ == '__main__':
    main()
