#!/usr/bin/env python3
import collections
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import json
import time
# import sqlite3
import sqlalchemy.exc
import os
import threading, queue
from pathlib import Path
from typing import Optional, Union, Tuple, List, NamedTuple
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import re
# import parsel
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
import html as html_

import db
from pandoc_parser import convert_page
from get_parsed_html import get_content_from_html, get_content_from_html_soup, re_spaces_many_no_newlines
from pandoc_parser import Image


class CategoryText(BaseModel):
    slug: str
    name: str


class Category(CategoryText):
    cid: int = Field(..., alias='id')
    name_ws: str

    class Config:
        allow_population_by_field_name = True


class D(BaseModel):
    tid: int
    title: str
    author: Optional[str]
    translator: Optional[str]
    year: Optional[str]
    desc: Optional[str]
    author_tag: Optional[str]
    year_tag: Optional[str]
    annotation_tag: Optional[str]
    litarea: Optional[str]
    name: Optional[str]
    author_slug: Optional[str] = Field(..., alias='slug_author')
    name_WS: Optional[str]
    text_url: str
    # db_row: dict
    # source: Optional[str]
    # other: Optional[str]
    # text: str | None

    categories: List[CategoryText] = []
    categories_string: Optional[str]
    wiki_title: Optional[str]
    images: List[Image] = []

    html: str
    wiki: Optional[str]
    soup: Optional[BeautifulSoup]

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


class CommonData:
    categories_cached = []
    authors_cache_db = []


re_get_desc_html = re.compile(
    r'<!---+ Блок описания произведения \(слева вверху\) -+>\s*(.+?)\s*<!---+ дизайнерские красоты вставляем здесь',
    flags=re.S)


def get_desc_from_html(html: str) -> str:
    if m := re_get_desc_html.search(html):
        html = m.group(1)
        html = re.sub(r'<form .*?</form>', '', html, flags=re.DOTALL)
        html = re.sub(r'<script .*?</script>', '', html, flags=re.DOTALL)
        html = html_.unescape(html)
        html = re_spaces_many_no_newlines.sub(' ', html)  #   и множественные пробелы, без переводов строк
        html = re.sub(r'<p( [^>]*)?>\s*(<br>)+', r'<p\1>', html, flags=re.I)
        return html


def get_desc_block(soup):
    for t in soup.find_all('table'):
        for e in t.find_all(
                text=lambda x: isinstance(x, Comment) and 'Блок описания произведения (слева вверху)' in x):
            return t


def get_li_block(desc_block, d) -> Optional[tuple]:
    # for ul in desc_block.find_all('ul'):
    #     for _ in ul.find_all(text=lambda x: isinstance(x, NavigableString) and 'Оставить комментарий' in x):
    ul = desc_block.find_all('ul')[0]
    lis = ul.find_all(lambda e: e.name == 'li' and
                                not [s for s in ('Оставить комментарий', 'Обновлено:', 'Комментарии:') if s in e.text.strip()])
    if len(lis) not in [3, 4]:
        raise RuntimeError(f'tid: {d.tid}, длинна <li> описания не равна 5-6')
    return lis


def get_annotation_block(desc_block):
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


def parse_year(year_line, d) -> D:
    for y in year_line.find_all(text=lambda x: isinstance(x, NavigableString) and 'Год:' in x):
        d.year = y.text.split('Год:')[1].strip()
        break
    return d


def parse_annotation(annotation_, d) -> D:
    # todo
    if annotation_ and annotation_.text.strip() != '':
        # self.d.desc = annotation_.text  # удаляет теги, заменяет <br> на \n
        if m := re.search(r'<li>(\s|<br[\s/]*>|\.)*(.*?)(\s|<br[\s/]*>)*</li>', str(annotation_), flags=re.DOTALL):
            t = m.group(2)
            if not re.search(r'^[.,?!]]*$', t):  # исключаем пустое значение
                d.desc = t
    # self.d.year=str(annotation_)
    return d


def parse_categories(categories_line, d):
    if x := categories_line.find('a'):
        if x.attrs.get('href', '').startswith("/type/"):
            d.categories = [CategoryText(slug=a.attrs.get('href'), name=a.text) for a in categories_line.find_all('a')
                            if a.attrs.get('href').startswith("/")]

    for e in categories_line.find_all('font', attrs={'size': '-1'}):
        e.unwrap()


def parse_author(author_line, d) -> D:
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


def parse_translator(annotation_, author_line, d) -> D:
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
                            for a in CommonData.authors_cache_db:
                                if a.slug == urlsplit(href).path.replace('/editors', '').rstrip('/'):
                                    d.translator = a.name_WS
                            else:
                                d.translator = d.name_WS
                            break

                            # if r := db.authors.find_one(
                            #         slug=urlsplit(href).path.replace('/editors', '').rstrip('/')):
                            #     d.translator = r['name_WS']
                            # else:
                            #     d.translator = d.name_WS
                            # break
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


# def parse_urls_of_images(d):
#     images_urls = set()
#
#     if m := re.search('<img src="([^"]+)"', d.html):
#         url = m.group(1)
#         if not 'list.ru' in url:
#             images_urls.add(url)
#
#     # for a in soup.find_all('img'):
#     #     if url := a.attrs.get('src'):
#     #         if not 'list.ru' in url:
#     #             images_urls.add(url)
#
#     # self.d.images_urls = []
#     for url in images_urls:
#         p = Path(url)
#         name_ws = re.search(r'^(text_\d+_).+', p.parts[-2]).group(1) + p.name
#         img = Image(tid=d.tid, url=url, filename=p.name, filename_wiki=p.stem, name_ws=name_ws)
#         d.images.append(img)
#
#     return d


def parse(d):
    desc_html = get_desc_from_html(d.html)
    # d.soup = soup = desc_block = BeautifulSoup(desc_html, 'html5lib')
    desc_block = BeautifulSoup(desc_html, 'html5lib')
    # d.wiki = convert_page(d)

    # desc_block = get_desc_block(soup)
    annotation_ = get_annotation_block(desc_block)
    lis = get_li_block(desc_block, d)
    author_line, year_line, categories_line = lis[0], lis[1], lis[2]
    for store, tag in zip(('author_tag', 'year_tag', 'annotation_tag'), (author_line, year_line, annotation_)):
        if tag:
            t = re.search(r'<li>(\s|<br[\s/]*>)*(.*?)(\s|<br[\s/]*>)*</li>', str(tag), flags=re.DOTALL)
            if t.group(2) != '':
                d.__setattr__(store, t.group(2))

    parse_categories(categories_line, d)

    d = parse_author(author_line, d)
    d = parse_translator(annotation_, author_line, d)
    d = parse_year(year_line, d)

    d = parse_annotation(annotation_, d)

    # d = parse_urls_of_images(d)

    return d


def db_add_text_data(d, upsert=False):
    row = d.dict(include={'tid', 'author', 'translator', 'year', 'desc', 'author_tag', 'year_tag', 'annotation_tag'})
    if upsert:
        db.desc.upsert(row, ['tid'], ensure=True)
    else:
        db.desc.insert(row, ensure=True)


def db_add_categories(d):
    categories_cached = CommonData.categories_cached
    categories_to_add = []
    for dc in d.categories:
        # for cid, slug, name in categories_cached:
        for c in categories_cached:
            if c.slug == dc.slug:
                break
        else:
            # добавить новую категорию сайта в БД
            # print('to db insert', d.tid)
            tn = db.texts_categories_names
            tn.insert(dc.dict(), ensure=True)
            # print('to db find_one', d.tid)
            cid = tn.find_one(slug=dc.slug)['id']
            categories_cached.append(Category(cid=cid, slug=dc.slug, name=dc.name))
        categories_to_add.append({'tid': d.tid, 'category_id': c.cid})

    tc = db.texts_categories
    tc.delete(tid=d.tid)
    # tc.insert_many(categories_to_add, ensure=True)
    db.db.begin()
    try:
        for r in categories_to_add:
            tc.insert(r, ensure=True)
        db.db.commit()
    except:
        db.db.rollback()


# def db_add_images_urls(d):
#     rows = [img.dict() for img in d.images]
#     try:
#         db.images.insert_many(rows, ensure=True)
#     except sqlalchemy.exc.IntegrityError as e:
#         print(e, f'\n{d.tid=}')
#         db.images.delete(tid=d.tid)
#         db.images.insert_many(rows, ensure=True)


def main_one_thread():
    lock = threading.RLock()
    # categories_cached = [(r['id'], r['slug'], r['name']) for r in db.texts_categories_names.find()]
    categories_cached = [Category.parse_obj(r) for r in db.texts_categories_names.find()]

    print('find db for rows to work, initial treades')
    t = db.all_tables
    cols = t.table.c
    print('1')
    while True:
        r = t.find_one(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None))
        if not r:
            break
        d = D.parse_obj(r)
        rint(d.tid)
        d = parse(d, None)
        if d.wiki:
            print('to db', d.tid)
            with lock:
                db_add_categories(d, categories_cached)
                db_add_text_data(d)
                db_add_images_urls(d)

    print('All work completed')


def main():
    lock = threading.RLock()
    q = queue.Queue(maxsize=40)  # fifo_queue
    db_q = queue.Queue(maxsize=5)

    class AuthorsCacheDB(NamedTuple):
        slug: str
        name_WS: str

    # categories_cached = [(r['id'], r['slug'], r['name']) for r in db.texts_categories_names.find()]
    # categories_cached = [CategoryCached(cid=r['id'], slug=r['slug'], name=r['name']) for r in db.texts_categories_names.find()]
    CommonData.categories_cached = [Category.parse_obj(r) for r in db.texts_categories_names.find()]
    CommonData.authors_cache_db = [AuthorsCacheDB(slug=r['slug'], name_WS=r['name_WS']) for r in
                                   db.authors.find()]

    def db_fill_pool():
        ta = db.all_tables.table
        tc = db.texts_categories.table

        # is_cats = {r['tid'] for r in db.texts_categories.find()}

        offset = 0
        while True:

            while not db_q.empty():
                time.sleep(1)

            stm = f"SELECT slug_text,text_url,{ta.c.tid} as tid,`year`,size,title,title_ws,text_desc_raw,oo,uploaded_text,do_upload,slug_author,author_id,`name`,family_parsed,names_parsed,name_WS,live_time,town,litarea,image_url_filename,image_filename_wiki,author_desc,is_author,uploaded_author,html,wiki,desc_tid,author_cat " \
                  f'FROM {ta.name} LEFT JOIN {tc.name} ON {ta.c.tid}={tc.c.tid} WHERE {tc.c.tid} IS NULL ' \
                  f'AND {ta.c.title} IS NOT NULL AND {ta.c.html} IS NOT NULL ' \
                  f'LIMIT {q.maxsize} OFFSET {offset};'  # f'LIMIT 100;'
            rows = db.db.query(stm)
            pool = [D.parse_obj(r) for r in rows]
            offset += q.maxsize
            if not pool:
                break
            for d in pool:
                while q.full():
                    time.sleep(1)
                q.put(d)

    def db_fill_pool_():
        t = db.db.get_table('all_cat_join')
        # t = db.all_tables
        ta = t.table

        # offset = 0
        while True:

            # for r in t.find(tid=114740):  # 87482  87492
            pool = [D.parse_obj(r) for r in t.find(
                ta.c.title.is_not(None), ta.c.html.is_not(None), do_upload=1,
                _limit=q.maxsize)]  # cols.desc_tid.is_(None),  , _offset=offset

            if not pool:
                break
            # if not pool:
            #     if offset == 0:
            #         break
            #     else:
            #         offset = 0
            # offset += q.maxsize
            for d in pool:
                while q.full():
                    time.sleep(1)
                q.put(d)

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
                    db_add_categories(d)
                    db_add_text_data(d, upsert=True)
                    # db_add_images_urls(d)

            db_q.task_done()

    def worker():
        while True:
            # print(f'{q.unfinished_tasks=}')
            while q.empty():
                # print(f'q.empty sleep')
                time.sleep(1)
            d = q.get()
            print(d.tid)
            d = parse(d)
            if d:
                while db_q.full():
                    time.sleep(1)
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
    for r in range(q.maxsize):
        threading.Thread(target=worker, daemon=True).start()

    print('find db for rows to work, initial threads')
    # for tid in [5643]:
    # for r in db.htmls.find(db.htmls.table.c.wiki.is_not(None)):
    # for r in db1['all_tables'].find(author=None):  # другое db.connection если используется однопоточное SQLite
    t = db.all_tables
    cols = t.table.c

    # self.d = D(tid=r['tid'], html=r['html'] )
    # try:
    #     d = D.parse_obj(r)
    # except Exception as e:
    #     print('Exception')
    #     return
    # if r['title'] != d.title and r['name'] != d.name:
    #     print('Exception')
    #     return

    # offset = 0
    # while True:
    #     # for r in t.find(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None)):
    #     # for r in t.find(cols.title.is_not(None), cols.html.is_not(None)):
    #     # for r in t.find(tid=114740):
    #     # for r in t.find(tid=87482):
    #     # for r in t.find(tid=87492):
    #     pool = [D.parse_obj(r) for r in
    #             t.find(cols.title.is_not(None), cols.html.is_not(None), _limit=q.maxsize, _offset=offset)]
    #     if not pool:
    #         if offset == 0:
    #             break
    #         else:
    #             offset = 0
    #     offset += q.maxsize
    #     for d in pool:
    #         print(d.tid)
    #         q.put(d)
    #
    #     while q.unfinished_tasks > 0:
    #         time.sleep(3)
    db_fill_pool()

    # block until all tasks are done
    # db_q_pool.join()
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
