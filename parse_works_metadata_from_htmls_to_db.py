#!/usr/bin/env python3
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import json
import time
# import sqlite3
import sqlalchemy.exc
import os
import threading, queue
from pathlib import Path
from typing import Optional, Union, Tuple, List
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import re
# import parsel
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
import html as html_

import db
from pandoc_parser import convert_page
from get_parsed_html import get_content_from_html, get_content_from_html_soup


class Image(BaseModel):
    tid: int
    url: str
    filename: str
    filename_wiki: str


class D(BaseModel):
    """
    from collections import namedtuple
    d = namedtuple('D', 'author title date translator source other text categories')
        """
    tid: int
    title: str
    author: str | None
    translator: str | None
    year: str | None
    desc: str | None
    author_tag: str | None
    year_tag: str | None
    annotation_tag: str | None
    litarea: str | None
    name: str | None
    author_slug: str | None = Field(..., alias='slug_author')
    name_WS: str | None
    text_url: str
    # db_row: dict
    # source: Optional[str]
    # other: Optional[str]
    # text: str | None

    categories: list[tuple[str, str]] = []
    categories_string: str | None
    wiki_title: str | None
    images_urls: list[Image] = []

    html: str
    wiki: str | None
    soup: BeautifulSoup | None

    class Config:
        validate_assignment = True
        extra = Extra.ignore
        arbitrary_types_allowed = True

    @validator('*')
    def none_instead_empty_values(cls, v):
        if isinstance(v, str):
            v = v.strip()
            v = None if v == '' else v
        return v


class Parse_metadata_from_html:

    def get_desc_block(self, soup):
        for t in soup.find_all('table'):
            for e in t.find_all(
                    text=lambda x: isinstance(x, Comment) and 'Блок описания произведения (слева вверху)' in x):
                return t

    def get_li_block(self, desc_block) -> Optional[tuple]:
        # for ul in desc_block.find_all('ul'):
        #     for _ in ul.find_all(text=lambda x: isinstance(x, NavigableString) and 'Оставить комментарий' in x):
        ul = desc_block.find_all('ul')[0]
        li = ul.find_all('li')
        return li

    def get_annotation_block(self, desc_block):
        if a := desc_block.find_all(text=lambda x: isinstance(x, NavigableString) and 'Аннотация:' in x):
            e = a[0]
            li = e.find_parent('li')
            z = e
            if e.parent.name == 'font':
                z = e.parent
                if e.parent.parent.name == 'b':
                    z = e.parent.parent
            z.extract()
            if i := li.find('i'):
                i.unwrap()
            return li

    def parse_year(self, year_line, d)->D:
        for y in year_line.find_all(text=lambda x: isinstance(x, NavigableString) and 'Год:' in x):
            d.year = y.text.split('Год:')[1].strip()
            break
        return d

    def parse_annotation(self, annotation_, d)->D:
        # todo
        if annotation_ and annotation_.text.strip() != '':
            # self.d.desc = annotation_.text  # удаляет теги, заменяет <br> на \n
            if m := re.search(r'<li>(\s|<br[\s/]*>|\.)*(.*?)(\s|<br[\s/]*>)*</li>', str(annotation_), flags=re.DOTALL):
                t = m.group(2)
                if not re.search(r'^[.,?!]]*$', t):  # исключаем пустое значение
                    d.desc = t
        # self.d.year=str(annotation_)
        return d

    def parse_categories(self, categories_line, d):
        if x := categories_line.find('a'):
            if x.attrs.get('href', '').startswith("/type/"):
                d.categories = [(a.attrs.get('href'), a.text) for a in categories_line.find_all('a')
                                     if a.attrs.get('href').startswith("/")]

        for e in categories_line.find_all('font', attrs={'size': '-1'}):
            e.unwrap()

    def parse_author(self, author_line, d)->D:

        """<li><a href="http://az.lib.ru/d/defo_d/">Дефо Даниель</a>
         (перевод: Я . . . . въ Л . . . . .нъ)
         (<u>yes@lib.ru</u>)
        </li>"""
        author_line_s = re.sub(r'\(?[\w.]+@\w+\.ru\)?', '', author_line.text)
        # if email := author_line.find(text=lambda x: isinstance(x, NavigableString) and '@lib.ru' in x):
        #     email.extract()
        for a in author_line.find_all('a'):
            if d.name.replace('_', ' ') == d.litarea:
                if a.text.strip() not in ([d.litarea, d.name]):
                    d.author = a.text.strip()
                break

            elif href := a.attrs.get('href'):
                href_slug = urlsplit(href).path.rstrip('/')
                if href_slug == d.author_slug:
                    d.author = d.name_WS
                    break
                else:
                    db_a = db.authors.find_one(slug=href_slug)
                    if db_a:
                        d.author = d.name_WS
                        break
            else:
                print('href != author_slug')
        else:
            print('нет <a> в строке автора')
            if author_ := re.search(r'(.+?)\s*\(перевод:\s*(.+?)\s*\)', author_line_s):
                d.author = author_.group(1)
            else:
                raise RuntimeError('tid:', d.tid, 'автор не распарсен')

        # if translator := re.search(r'\(перевод:\s*(.+?)\s*\)', author_line_s):
        #     d.translator = translator.group(1)

        # if email := author_line.find(text=lambda x: isinstance(x, NavigableString) and '@lib.ru' in x):
        #     email.extract()
        return d

    def parse_translator(self, r, annotation_, author_line, d)->D:
        """Перевод <a href="http://az.lib.ru/z/zhurawskaja_z_n/">Зинаиды Журавской</a>"""

        # for s in annotation_.descendants:
        #     if isinstance(s, NavigableString) and 'Перевод ' in s \
        #             and isinstance(s.next_element, Tag) and s.next_sibling.name == 'a':
        #         a = s.next_sibling

        # if translator := re.search(r'(.+?)\s*\(Перевод \s*(.+?)\s*\)', annotation_):
        #     d.translator = translator.group(1)

        if annotation_:
            for e in annotation_.find_all('i'):
                for s in e.contents:
                    if isinstance(s, NavigableString) and 'Перевод ' in s:
                        if isinstance(s.next_element, Tag) and s.next_element.name == 'a':
                            a_ = s.next_element
                            href = a_.attrs.get('href', '').strip()
                            if href and href != '':
                                if r := db.all_tables.find_one(
                                        author_slug=urlsplit(href).path.replace('/editors', '').rstrip('/')):
                                    d.translator = d.name_WS
                                    break
                            else:
                                pass
                            a_.unwrap()
                            if d.translator is None:
                                # http://az.lib.ru/d/degen_e_w/
                                # raise RuntimeError('tid:', self.d.tid, 'не определён переводчик в <a>')
                                print('tid:', d.tid, 'не определён переводчик в <a>')
                                d.translator = e.text
                                e.extract()
                        # elif translator_ := re.search(r'Перевод (.+)', s):
                        #     self.d.translator = translator_.group(1)
                        #     s.extract()
                        # else:
                        #     raise RuntimeError('tid:', self.d.tid, 'не определён переводчик')
                        else:
                            d.translator = s
                            s.extract()
                            if e.contents == []:
                                e.extract()

        # if self.d.translator and self.d.translator.strip() == '':
        #     self.d.translator = None

        if d.translator is None:
            if translator_ := re.search(r'\(перевод:\s*(.+?)\s*\)', author_line.text):
                d.translator = translator_.group(1)
            else:
                # print('нет <a> в строке автора')
                pass

        is_cat_transl = [True for slug, name in d.categories if name == 'Переводы']
        if is_cat_transl and d.translator is None:
            # raise RuntimeError('tid:', self.d.tid, 'не определён переводчик')
            print('tid:', d.tid, 'не определён переводчик', d.text_url)
        elif not is_cat_transl and d.translator:
            # raise RuntimeError('tid:', self.d.tid, 'переводчик без категории перевода')
            print('tid:', d.tid, 'переводчик без категории перевода', d.text_url)

        return d

    def parse_urls_of_images(self, r, soup, d):
        images_urls = set()
        for a in soup.find_all('img'):
            if url := a.attrs.get('src'):
                if not 'list.ru' in url:
                    images_urls.add(url)

        # self.d.images_urls = []
        for url in images_urls:
            p = Path(url)
            img = Image(tid=d.tid, url=url, filename=p.name, filename_wiki=p.stem)
            d.images_urls.append(img)

        return d

    def parse(self, r):
        # self.d = D(tid=r['tid'], html=r['html'] )
        try:
            d = D.parse_obj(r)
        except Exception as e:
            print('Exception')
            return
        if r['title'] != d.title and r['name'] != d.name:
            print('Exception')
            return
        # soup = BeautifulSoup(r['html'], 'html5lib')

        d.soup = soup = BeautifulSoup(d.html, 'html5lib')
        # d.wiki = convert_page(d)

        desc_block = self.get_desc_block(soup)
        annotation_ = self.get_annotation_block(desc_block)
        li = self.get_li_block(desc_block)
        if len(li) not in [5, 6]:
            # raise RuntimeError('tid:', self.r['tid'], 'длинна <li> описания не равна 6')
            print('tid:', d.tid, 'длинна <li> описания не равна 5-6')
            return d
        else:
            author_line, year_line, categories_line = li[1], li[2], li[4]
        for store, tag in zip(('author_tag', 'year_tag', 'annotation_tag'), (author_line, year_line, annotation_)):
            if tag:
                t = re.search(r'<li>(\s|<br[\s/]*>)*(.*?)(\s|<br[\s/]*>)*</li>', str(tag), flags=re.DOTALL)
                if t.group(2) != '':
                    d.__setattr__(store, t.group(2))

        self.parse_categories(categories_line, d)

        d = self.parse_author(author_line, d)
        d = self.parse_translator(r, annotation_, author_line, d)
        d = self.parse_year(year_line, d)

        d=self.parse_annotation(annotation_, d)

        d = self.parse_urls_of_images(r, soup, d)

        return d


def add_text_data(d):
    db.htmls.update(d.dict(
        include={'tid', 'author', 'translator', 'year', 'desc', 'author_tag', 'year_tag', 'annotation_tag',
                 # 'wiki'
                 }),
        ['tid'], ensure=True)

def add_categories(d, categories_cached):
    categories_to_add = []
    for slug_d, name_d in d.categories:
        for cid, slug, name in categories_cached:
            if slug == slug_d:
                break
        else:
            # print('to db insert', d.tid)
            db.texts_categories_names.insert({'slug': slug_d, 'name': name_d}, ensure=True)
            # print('to db find_one', d.tid)
            cid = db.texts_categories_names.find_one(slug=slug_d)['id']
            categories_cached.append([cid, slug_d, name_d])
        categories_to_add.append({'tid': d.tid, 'category_id': cid})

    try:
        # print('to db insert_many', d.tid)
        db.texts_categories.insert_many(categories_to_add, ensure=True)
    except sqlalchemy.exc.IntegrityError:
        # print('to db delete', d.tid)
        db.texts_categories.delete(tid=d.tid)
        # print('to db insert_many', d.tid)
        db.texts_categories.insert_many(categories_to_add, ensure=True)


def add_images_urls(d):
    rows = [img.dict() for img in d.images_urls]
    try:
        db.images.insert_many(rows, ensure=True)
    except sqlalchemy.exc.IntegrityError as e:
        print(e, f'\n{d.tid=}')
        db.images.delete(tid=d.tid)
        db.images.insert_many(rows, ensure=True)


def main_one_thread():
    lock = threading.RLock()
    categories_cached = [(r['id'], r['slug'], r['name']) for r in db.texts_categories_names.find()]

    print('find db for rows to work, initial treades')
    t = db.all_tables
    cols = t.table.c
    print('1')
    while True:
        r = t.find_one(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None))
        if not r:
            break
        print(r['tid'])
        parser = Parse_metadata_from_html()
        d = parser.parse(r)
        if d.wiki:
            print('to db', d.tid)
            with lock:
                add_categories(d, categories_cached)
                add_text_data(d)
                add_images_urls(d)

    print('All work completed')



def main():
    lock = threading.RLock()
    q = queue.Queue(maxsize=40)  # fifo_queue
    db_q = queue.Queue(maxsize=10)

    categories_cached = [(r['id'], r['slug'], r['name']) for r in db.texts_categories_names.find()]
    parser = Parse_metadata_from_html()

    def db_save_pool():
        while True:
            while db_q.empty():
                # print(f'db_q.empty sleep')
                time.sleep(1)
            # print(f'{db_q.unfinished_tasks=}')
            d = db_q.get()
            print('to db', d.tid)

            with lock:
                if d.wiki:
                    add_categories(d, categories_cached)
                    add_text_data(d)
                    add_images_urls(d)

            db_q.task_done()

    def worker():
        while True:
            # print(f'{q.unfinished_tasks=}')
            while q.empty():
                # print(f'q.empty sleep')
                time.sleep(1)
            r = q.get()
            print(r['tid'])
            d = parser.parse(r)
            if d:
                db_q.put(d)

            q.task_done()

    threading.Thread(target=db_save_pool, name='db_save', daemon=True).start()

    # turn-on the worker thread

    # for r in db.all_tables.find(db.all_tables.table.c.wiki_page.isnot(None)):
    # for r in db.all_tables.find(wiki=None):
    # r = db.all_tables.find_one(tid=7487)
    # tid = 7488  # http://az.lib.ru/d/defo_d/text_0014.shtml  В аннотации <i>Перевод <a href="http://az.lib.ru/z/zhurawskaja_z_n/">Зинаиды Журавской</a></i>
    # tid = 7487  # http://az.lib.ru/d/defo_d/text_0013_robinson_crusoe-oldorfo.shtml  В строк автор "Дефо Даниель (перевод: Я . . . . въ Л . . . . .нъ)"
    # tid = 7491  # http://az.lib.ru/d/defo_d/text_0100oldorfo.shtml С картинками Робинзон
    # tid = 7
    for r in range(100):
        threading.Thread(target=worker, daemon=True).start()

    print('find db for rows to work, initial treades')
    # for tid in [5643]:
    # for r in db.htmls.find(db.htmls.table.c.wiki.is_not(None)):
    # for r in db1['all_tables'].find(author=None):  # другое db.connection если используется однопоточное SQLite
    t = db.all_tables
    cols = t.table.c
    print('1')
    for r in t.find(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None)):
        # for r in db.all_tables.find(tid =654):
        print(r['tid'])
        while q.full():
            # print(f'q.full sleep')
            time.sleep(1)
        q.put(r)

    # block until all tasks are done
    q.join()
    db_q.join()
    print('All work completed')


if __name__ == '__main__':
    main()
    # main_one_thread()


# def parse_metadata_from_html_parsel(tid, html):
#     """
#     # todo
#     """
#     d = D()
#
#     dom = parsel.Selector(html)
#
#     desc_block = dom.xpath(
#         '//table//comment()[contains(.,"Блок описания произведения (слева вверху")]/ancestor::table')
#     info_li = desc_block.xpath('.//li/a[contains(.,"Оставить комментарий")]/ancestor::ul//li')
#     if len(info_li) != 6:
#         print('tid:', tid, 'длинна <li> описания не равна 6')
#         return
#
#     author_ = info_li[1]
#     year_ = info_li[2]
#     categories_ = info_li[3]
#
#     annotation_ul = desc_block.xpath('./a[contains(.,"Аннотация:")]/ancestor::ul')
#
#     categories_ = desc_block.xpath('.//a[starts-with(@href,"/type/")]/ancestor::li//a').css('a[href^="/"]')
#     categories = [(z.css('::attr(href)').get(), z.css('::text').get()) for z in categories_]
#
#     return d
