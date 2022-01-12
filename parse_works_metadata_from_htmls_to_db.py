#!/usr/bin/env python3
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import json
import time
import sqlite3
import sqlalchemy.exc
import os
import threading, queue
from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass
import re
import parsel
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
import html as html_

import db

lock = threading.RLock()
q = queue.Queue(maxsize=20)  # fifo_queue
db_q = queue.Queue(maxsize=20)

categories_cached = [(r['id'], r['slug'], r['name']) for r in db.texts_categories_names.find()]


@dataclass
class D:
    """
    from collections import namedtuple
    d = namedtuple('D', 'author title date translator source other text categories')
        """
    tid: int
    author: str = None
    translator: str = None
    categories = []
    year = None
    desc = None
    author_tag = None
    year_tag = None
    annotation_tag = None


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
        if annotation_ and annotation_.text.strip() != '':
            # self.d.desc = annotation_.text  # удаляет теги, заменяет <br> на \n
            if m := re.search(r'<li>(\s|<br[\s/]*>|\.)*(.*?)(\s|<br[\s/]*>)*</li>', str(annotation_), flags=re.DOTALL):
                t = m.group(2)
                if not re.search(r'^[.,?!]]*$', t):  # исключаем пустое значение
                    self.d.desc = t
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
            if r['name'].replace('_', ' ') == r['litarea']:
                if a.text.strip() not in ([r['litarea'], r['name']]):
                    self.d.author = a.text.strip()
                break

            elif href := a.attrs.get('href'):
                if urlsplit(href).path.rstrip('/') == r['author_slug']:
                    self.d.author = r['name_for_WS']
                    break
            else:
                print('href != author_slug')
        else:
            print('нет <a> в строке автора')
            if author_ := re.search(r'(.+?)\s*\(перевод:\s*(.+?)\s*\)', author_line_s):
                self.d.author = author_
            else:
                raise RuntimeError('tid:', self.r['tid'], 'автор не распарсен')

        # if translator := re.search(r'\(перевод:\s*(.+?)\s*\)', author_line_s):
        #     d.translator = translator.group(1)

        # if email := author_line.find(text=lambda x: isinstance(x, NavigableString) and '@lib.ru' in x):
        #     email.extract()

    def parse_translator(self, annotation_, author_line):
        r = self.r
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
                            href = a_.attrs.get('href')
                            if href and a_.attrs.get('href') != '':
                                if r := db.all_tables.find_one(
                                        author_slug=urlsplit(href.strip()).path.replace('/editors', '').rstrip('/')):
                                    self.d.translator = r['name_for_WS']
                                    break
                            else:
                                pass
                            a_.unwrap()
                            if self.d.translator is None:
                                # http://az.lib.ru/d/degen_e_w/
                                # raise RuntimeError('tid:', self.r['tid'], 'не определён переводчик в <a>')
                                print('tid:', self.r['tid'], 'не определён переводчик в <a>')
                                self.d.translator = e.text
                                e.extract()
                        # elif translator_ := re.search(r'Перевод (.+)', s):
                        #     self.d.translator = translator_.group(1)
                        #     s.extract()
                        # else:
                        #     raise RuntimeError('tid:', self.r['tid'], 'не определён переводчик')
                        else:
                            self.d.translator = s
                            s.extract()
                            if e.contents == []:
                                e.extract()

        if self.d.translator is None:
            if translator_ := re.search(r'\(перевод:\s*(.+?)\s*\)', author_line.text):
                self.d.translator = translator_.group(1)
            else:
                # print('нет <a> в строке автора')
                pass

        is_cat_transl = [True for slug, name in self.d.categories if name == 'Переводы']
        if is_cat_transl and self.d.translator is None:
            # raise RuntimeError('tid:', self.r['tid'], 'не определён переводчик')
            print('tid:', self.r['tid'], 'не определён переводчик', r['text_url'])
        elif not is_cat_transl and self.d.translator:
            # raise RuntimeError('tid:', self.r['tid'], 'переводчик без категории перевода')
            print('tid:', self.r['tid'], 'переводчик без категории перевода', r['text_url'])

    def parse(self, r):
        self.r = r

        self.d = D(tid=r['tid'])
        soup = BeautifulSoup(r['html'], 'html5lib')

        desc_block = self.get_desc_block(soup)
        annotation_ = self.get_annotation_block(desc_block)
        li = self.get_li_block(desc_block)
        if len(li) not in [5, 6]:
            # raise RuntimeError('tid:', self.r['tid'], 'длинна <li> описания не равна 6')
            print('tid:', self.d.tid, 'длинна <li> описания не равна 5-6')
            return
        else:
            author_line, year_line, categories_line = li[1], li[2], li[4]
        for store, tag in zip(('author_tag', 'year_tag', 'annotation_tag'), (author_line, year_line, annotation_)):
            if tag:
                t = re.search(r'<li>(\s|<br[\s/]*>)*(.*?)(\s|<br[\s/]*>)*</li>', str(tag), flags=re.DOTALL)
                if t.group(2) != '':
                    self.d.__setattr__(store, t.group(2))

        self.parse_categories(categories_line)

        self.parse_author(author_line)
        self.parse_translator(annotation_, author_line)
        self.parse_year(year_line)

        self.parse_annotation(annotation_)

        return self.d


tids = []


def add_categories(d, categories_cached):
    tid = d.tid

    if tid in tids:
        raise RuntimeError('tid:', tid, 'tid уже обрабатывался, дублирование в threads')
        # print('ой', tid)
    else:
        tids.append(tid)

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
        categories_to_add.append({'tid': tid, 'category_id': cid})

    try:
        # print('to db insert_many', d.tid)
        db.texts_categories.insert_many(categories_to_add, ensure=True)
    except sqlalchemy.exc.IntegrityError:
        # print('to db delete', d.tid)
        db.texts_categories.delete(tid=tid)
        # print('to db insert_many', d.tid)
        db.texts_categories.insert_many(categories_to_add, ensure=True)


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
    # from collections import namedtuple
    # Cat = namedtuple('Cat', '')

    # for r in db.all_tables.find(db.all_tables.table.c.wiki_page.isnot(None)):
    # for r in db.all_tables.find(wiki=None):
    # r = db.all_tables.find_one(tid=7487)
    # tid = 7488  # http://az.lib.ru/d/defo_d/text_0014.shtml  В аннотации <i>Перевод <a href="http://az.lib.ru/z/zhurawskaja_z_n/">Зинаиды Журавской</a></i>
    tid = 7487  # http://az.lib.ru/d/defo_d/text_0013_robinson_crusoe-oldorfo.shtml  В строк автор "Дефо Даниель (перевод: Я . . . . въ Л . . . . .нъ)"

    # tid = 7

    def db_save():
        while True:
            while db_q.empty():
                time.sleep(1)
            d = db_q.get()
            print('to db', d.tid)

            with lock:
                add_categories(d, categories_cached)

                db.htmls.update(
                    {'tid': d.tid, 'author': d.author, 'translator': d.translator, 'year': d.year, 'desc': d.desc,
                     'author_tag': d.author_tag, 'year_tag': d.year_tag, 'annotation_tag': d.annotation_tag},
                    ['tid'], ensure=True)

            db_q.task_done()

    def worker():
        while True:
            while q.empty():
                time.sleep(1)
            r = q.get()
            print(r['tid'])
            parser = Parse_metadata_from_html()
            d = parser.parse(r)
            if d:
                db_q.put(d)

            q.task_done()

    threading.Thread(target=db_save, name='db_save', daemon=True).start()

    # turn-on the worker thread
    for r in range(20):
        threading.Thread(target=worker, daemon=True).start()

    import dataset
    db1 = dataset.connect('sqlite:////home/vladislav/var/db/from_lib_ru.sqlite',
                          engine_kwargs={'connect_args': {'check_same_thread': False}})
    # for tid in [5643]:
    # for r in db.db_htmls.find(db.db_htmls.table.c.wiki.isnot(None)):
    for r in db1['all_tables'].find(author=None):
        # for r in db.all_tables.find(tid =654):
        while q.full():
            time.sleep(1)
        q.put(r)

    # block until all tasks are done
    q.join()
    db_q.join()
    print('All work completed')


if __name__ == '__main__':
    main()
