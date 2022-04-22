#!/usr/bin/env python3
import time
from datetime import datetime
from math import floor
import re
import threading, queue
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import concurrent.futures
from multiprocessing import cpu_count, Queue  # Вернет количество ядер процессора
import multiprocessing
import socket
import sqlalchemy.exc
from pathlib import Path
from typing import Optional, Union, Tuple, List
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import html as html_
import mwparserfromhell as mwp
import pypandoc
from bs4 import BeautifulSoup, Tag

import db_schema as db
from get_parsed_html import get_html, get_content_from_html, get_content_from_html_soup
from html2wiki import LibRu
from parser_html_to_wiki import *


class Image(BaseModel):
    tid: int
    urn: str
    filename: str
    name_ws: str


class H(BaseModel):
    tid: int
    html: str
    soup: Optional[BeautifulSoup]
    wikicode: mwp.wikicode.Wikicode = None
    wiki: Optional[str] = None
    wiki2: Optional[str] = None
    wiki_new: Optional[str] = None
    images: List[Image] = []

    class Config:
        # validate_assignment = True
        extra = Extra.ignore
        arbitrary_types_allowed = True


def tags_a_refs(soup):
    re_ref_id = re.compile(r'^#?((.*?)\.([aq])(\d+))$')

    def get_ref_id(link: str):
        if not link:
            return
        if re_ := re_ref_id.match(link):  # ref_id
            ref_id = '%s.a%s' % (re_.group(2), re_.group(4))
            link = '#%s' % ref_id
            ref_back_id = '%s.q%s' % (re_.group(2), re_.group(4))
            ref_back_link = '#%s' % ref_back_id

            return dict(re_=re_, link=link, id=ref_id, back_link=ref_back_link, back_id=ref_back_id)

    for e in soup.find_all('a'):

        if link := e.attrs.get('href'):
            if ref := get_ref_id(link):  # ref_id

                if e.attrs.get('href') == ref['link']:

                    if e.parent.name == 'sup':
                        e.parent.unwrap()

                    ne = e.next_sibling
                    if isinstance(ne, Tag) and ne.name == 'a' and ne.attrs.get('name') == ref['back_id']:
                        ne.extract()

                    e.replace_with('[ref name="%s" /]' % ref['id'])

    for e in soup.find_all('a'):
        if ref := get_ref_id(e.attrs.get('name')):

            # num = e.next_element
            # if num.name == 'b' and re.match(r'^\d+\.$', num.text):
            #     num.extract()
            #
            # strip_tag(e)
            #
            # for rb in e.next_siblings:
            #     if rb.name == 'a' and rb.attrs.get('href') == ref['back_link'] and rb.text == 'Обратно':
            #
            #         e.insert_before('[ref follow="%s"]' % ref['id'])
            #         rb.replace_with('[/ref]')
            #
            #         e.unwrap()
            #         break

            num = e.next_element
            if num.name == 'b' and re.match(r'^\d+\.$', num.text):
                num.extract()

            for rb in e.next_siblings:
                if rb.name == 'a' and rb.attrs.get('href') == ref['back_link'] and rb.text == 'Обратно':
                    # note_tag = soup.new_tag('ref', attrs={'follow': ref['id']})
                    note_tag = soup.new_tag('span', attrs={'class': 'refnote', 'follow': ref['id']})
                    condition_break = lambda x: x != rb
                    wrap_several_elements(e, condition_break, note_tag)
                    rb.extract()
                    e.extract()
                    strip_tag(note_tag)

                    note_tag.insert(0, '[ref follow="%s"]' % ref['id'])
                    note_tag.append('[/ref]')

                    note_tag.unwrap()

                    break
            e.extract()
    return soup


async def pypandoc_converor(html) -> str:
    text = pypandoc.convert_text(html, 'mediawiki', format='html', verify_format=False)
    return text


async def convert_page(h: H):
    parser = LibRu()

    input_html = get_content_from_html(h.html)
    parser.soup = BeautifulSoup(input_html, 'html5lib')

    # parser.input_html = get_content_from_html(h.html)
    # parser.soup = d.soup

    # parser.make_soup(input_html)
    # parser.soup = BeautifulSoup(input_html, 'html5lib')
    # parser.soup = d.soup

    # parser.soup = move_corner_spaces_from_tags(parser.soup)
    parser.soup = tags_a_refs(parser.soup)
    parser.soup = parser.parsing_extentions(parser.soup)
    parser.soup = parser.inline_tags(parser.soup)
    # parser.soup.smooth()
    parser.soup = remove_spaces_between_tags(parser.soup, additinals_tags=[])

    # for e in parser.soup('pre'):
    #     e.name = 'poem'  # 'poem'
    #     e.append(parser.soup.new_tag('p'))
    # for e in parser.soup('blockquote'):
    #     e.unwrap()

    for t in parser.soup.find_all('dd'):
        t.name = 'p'

    html = parser.soup.decode()
    # html1 = re.sub(r'(<(?:p|dd)(?:| [^>]+?)>)\s+', r'\1', html)  # убрать пробелы после <p>, <dd>
    # html2 = re.sub(r'<(p|dd)(?:| [^>]+?))>\s+(?!</\1>)', r'<\1>\2', html)  # убрать пробелы после <p>, <dd>
    # html3 = re.sub(r'<(p|dd)( [^>]+)?>(</\1>)', r'<\1\2> \3',
    #        re.sub(r'<(p|dd)( [^>]+)?>[^\S\r\n]+', r'<\1\2>', html, flags=re.DOTALL))  # убрать пробелы после <p>, <dd>
    html = re.sub(r'<(p|dd)( [^>]+)?>[^\S\r\n]+', r'<\1\2>', html, flags=re.DOTALL)  # убрать пробелы после <p>, <dd>

    text = await pypandoc_converor(html)
    text = html_.unescape(html_.unescape(text.strip('/')))  # бывают вложенные сущности, вроде 'бес&amp;#1123;дку

    # out2 = re.sub('<span id="[^"]+"></span>', '', out2)

    # text = categorization(out2, parser.soup)

    # <span> to <ref>
    text = re.sub(r'\[(ref (?:name|follow)="[^"]+"(?: ?\\)?)\]', r'<\1>', text)
    text = re.sub(r'\[(/ref)\]', r'<\1>', text)
    if '<ref' in text:
        text = '%s\n\n== Примечания ==\n{{примечания}}\n' % text

    text = re.sub(r'(<div.*?>)(\s+)', r'\2\1', text)
    text = re.sub(r'(\s+)(</div>)', r'\2\1', text)

    text = re.sub(r'<div align="center">(.+?)</div>', r'<center>\1</center>', text, flags=re.DOTALL)
    text = re.sub(r'^(?:<center>\s*)?(\* \* \*|\*\*\*)(?:\s*</center>)?$', r'{{***}}', text, flags=re.MULTILINE)
    text = re.sub(r'^==+\s*\*\s*\*\s*\*\s*==+$', r'{{***}}', text, flags=re.MULTILINE)
    text = re.sub(r'^<center>\s*-{5}\s*</center>$', r'{{---|width=6em}}', text, flags=re.MULTILINE)
    text = re.sub(r'^<center>\s*-{6,}\s*</center>$', r'{{---|width=10em}}', text, flags=re.MULTILINE)
    text = re.sub(r'<center>\s*(\[\[File:[^]]+?)\]\]\s*</center>', r'\1|center]]', text)

    text = re.sub(r'([Α-Ω]+)', r'{{lang|grc|\1}}', text, flags=re.I)

    text = re.sub(r'(\n==+[^=]+?<br[/ ]*>)\n+', r'\1', text)  # fix: \n после <br> в заголовках

    text = re.sub(r"([^'])''''([^'])", r'\1\2', text)  # вики-курсив нулевой длинны
    text = re.sub(r"([^'])''([-—.\" ]+)''([^'])", r'\1\2\3', text)  # излишний курсив вокруг пробелов и знак.преп.
    text = re.sub(r"^(''+) +", r'\1', text, flags=re.MULTILINE)  # лишние пробелы в курсиве в начале строки
    # todo: удаляет '' в начале строки, начиная строку с пробела, портя текст. 
    # https://ru.wikisource.org/w/index.php?title=%D0%A2%D1%80%D0%B8_%D0%B3%D0%BB%D0%B0%D0%B2%D1%8B_%D0%B8%D0%B7_%D0%B8%D1%81%D1%82%D0%BE%D1%80%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%BE%D0%B9_%D0%BF%D0%BE%D1%8D%D1%82%D0%B8%D0%BA%D0%B8_(%D0%92%D0%B5%D1%81%D0%B5%D0%BB%D0%BE%D0%B2%D1%81%D0%BA%D0%B8%D0%B9)&diff=prev&oldid=4298606
    

    text = re.sub("([^\n])({{right\|)\s*", r"\1\n\2", text, flags=re.DOTALL)

    text = re.sub(r"([а-яa-z])", r'\1' + '\u0301', text, flags=re.I)  # ударение
    text = re.sub(r'(&#|#)?1122;', 'Ѣ', text)  # яти, с поврежденными кодами
    text = re.sub(r'(&#|#)?1123;', 'ѣ', text)
    text = re.sub(r'([а-я])122;', r'\1Ѣ', text, flags=re.I)
    text = re.sub(r'([а-я])123;', r'\1ѣ', text, flags=re.I)

    wc = mwp.parse(text)

    # удалить <span id=""> на которые нет ссылок
    links_ids = [l.title.lstrip('#') for l in wc.filter_wikilinks() if l.title.startswith('#')]
    spans = [t for t in wc.filter_tags() if t.tag == 'span'
             for a in t.attributes if a.name == 'id' and a.value not in links_ids]
    for span in spans: wc.remove(span)
    # for span in spans: wc.remove(span)
    # out2 = re.sub('<span class="footnote"></span>', '', out2)

    # <span "class=underline"> → '<u>'. Такие теги делает pandoc.
    for t in wc.filter_tags():
        if t.tag == 'span':
            for a in t.attributes:
                if a.name == 'class' and a.value == 'underline':
                    t.tag = 'u'
                    t.attributes.remove(a)

    # strip параметр в {{right|}}
    for t in wc.filter_templates(matches=lambda x: x.name == 'right'):
        t.params[0].value = t.params[0].value.strip()
    for t in wc.filter_templates(matches=lambda x: x.name == 'right' and x.params[0].value == ''):
        wc.remove(t)

    # for t in [t for t in wc.filter_tags(matches=lambda x: x.tag == 'div')]:
    #     t.params[0].value = t.params[0].value.strip()
    #
    # for t in wc.filter_tags(matches=lambda x: x.tag == 'div'):
    #     if t.tag == 'div' and t.get('align').value== 'center':
    #         t.attributes = []

    # g = [t for t in wc.filter_tags(matches=lambda x: x.tag == 'div' and x.get('align').value== 'right')]
    # [t for t in wc.filter_templates(matches=lambda x: x.name == 'right')]

    # mwp.parse('{{right|}}').nodes[0]

    h.wikicode = wc
    return h


def process_images(h):
    """ to simple names of images """
    for f in h.wikicode.filter_wikilinks(matches=lambda x: x.title.lower().startswith('file')):
        link = re.sub(r'^[Ff]ile:', '', str(f.title))
        if not link.startswith('/img/'):  # удалить ссылки на картинки которые не начинаются на '/img/
            del(f)
            continue
        p = Path(link)
        # f.title = re.sub(r'^.+?/(text_\d+_).*?/([^/]+)$', r'File:\1\2', str(f.title))
        # name_ws = re.search(r'^(text_\d+_).+', p.parts[-2]).group(1) + p.name
        try:
            name_ws = f'{p.parts[-3]}_{p.parts[-2]}_{p.name}'
        except IndexError as e:
            continue
        f.title = 'File:' + name_ws
        img = Image(tid=h.tid, urn=link, filename=p.name, name_ws=name_ws)
        h.images.append(img)
    return h


count_pages_per_min = 0
last_time = datetime.now()
PROCESSES = 10


class AsyncWorker:
    # offset = 0  # db_feel_pool    # q.maxsize
    chunk_size = 100  # db_feel_pool query rows limit

    # chunk = 100  # db_feel_pool query rows limit
    # PAGES_PER_CORE :int
    # i_core: int

    def __init__(self, i_core):
        self.i_core = i_core
        # self.offset_base = self.chunk_size * i_core  # стартовые значения offset для запроса из БД
        # self.offset = 0

    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.do_tasks(loop))
        # finished, unfinished = loop.run_until_complete(self.asynchronous(loop))
        # if len(unfinished):
        #     logging.error('have unfinished async tasks')
        loop.close()

    async def do_tasks(self, loop):
        loop = 0
        while True:
            offset = self.i_core * self.chunk_size + loop * 1000
            rows = await self.feeder(offset)
            loop += 1
            if not rows:
                break
            tasks = [self.work_row(r) for r in rows]
            await asyncio.gather(*tasks)

    async def process_images(self, h) -> H:
        return process_images(h)

    async def feeder(self, offset) -> Optional[List[dict]]:
        # t = db.all_tables
        t = db.htmls
        cols = t.table.c
        # for i in range(1, NUM_CORES + 1):
        print(f'{offset=} {self.i_core=}')
        res = t.find(
            cols.html.is_not(None),
            # cols.wiki.is_(None),
            # cols.wiki2.is_(None),
            wiki_differ_wiki2=1,
            wiki2_converted=0,
            # tid=88278,
            # tid=87499,
            # html={'like': '%%'},  # wiki2={'like': '%[[File:%'},
            _limit=self.chunk_size, _offset=offset)
        # _limit=limit, _offset=offset)
        rows = [r for r in res]
        return rows

    async def _feeder(self, offset) -> Optional[List[dict]]:
        # t = db.all_tables
        t = db.htmls
        cols = t.table.c
        # self.offset = self.chunk * self.i_core
        # offset = self.i_core * self.chunk_size
        # offset = 0
        is_refetched = False

        # start, end = 0, self.chunk_size
        # while start < len(z):
        #     for i in range(1, treads + 1):
        #         chunk = [r for r in data[start:end]]
        #         # for r in res:
        #         #     q.put(r)
        #         print(chunk)
        #         print(start, end)
        #         start += self.chunk_size
        #         end += self.chunk_size

        # offset = self.chunk_size * self.i_core

        while True:
            # for i in range(1, NUM_CORES + 1):
            print(f'{offset=} {self.i_core=}')
            res = t.find(
                cols.html.is_not(None),
                # cols.wiki.is_(None),
                # cols.wiki2.is_(None),
                wiki2_converted=1,
                # tid=88278,
                # tid=87499,
                # html={'like': '%%'},  # wiki2={'like': '%[[File:%'},
                _limit=self.chunk_size, _offset=offset)
            # _limit=limit, _offset=offset)
            if res.result_proxy.rowcount == 0 or res.result_proxy.rowcount < self.chunk_size:
                if offset == 0 or is_refetched:
                    break
                else:
                    if res.result_proxy.rowcount < self.chunk_size:
                        is_refetched = True
                    offset = 0
                    continue
            break
        # offset += self.chunk_size * self.i_core # self.offset_base

        # проблема в том, что нельзя сохранить плавающее значение offset для разных процессов.
        # даже если обозначить вверху скрипта, это будет считаться главным процессом.
        # в каждом процесс оно будет повторятся, и поэтому из базы данных запрашиваться одно и тоже, работа дублируется
        # таже проблема с очередями

        # self.offset += (self.chunk * (self.i_core + 1))
        # self.offset += 1000
        rows = [r for r in res]
        return rows

    async def db_save(self, h) -> None:
        rows = [img.dict() for img in h.images]
        with db.db as tx1:
            tx1['images'].delete(tid=h.tid)
            for row in rows:
                tx1['images'].insert_ignore(row, ['urn'])
            # tx1['images'].insert_many([row for row in rows], ['tid', 'name_ws'])
            r = {'tid': h.tid, 'wiki': h.wiki_new, 'wiki2_converted': 1}
            # if h.wiki_new != h.wiki2:
            #     r.update({'wiki_differ_wiki2': 1})
            tx1['htmls'].update(r, ['tid'])

    async def work_row(self, r):
        h = H.parse_obj(r)
        h = await convert_page(h)
        h = await self.process_images(h)
        h.wiki_new = strip_wikitext(str(h.wikicode))
        h.html = None
        h.wikicode = None
        if h.wiki_new:
            print('converted, to db', h.tid)
            await self.db_save(h)
        else:
            print('no wiki', h.tid)


def start(i_core: int):
    w = AsyncWorker(i_core)
    # w.PAGES_PER_CORE = num_pages
    w.start()


def main():
    # NUM_PAGES = 100  # Суммарное количество страниц для скрапинга
    # PROCESSES = 10  # cpu_count() - 1  # Количество ядер CPU (влкючая логические)
    # PAGES_PER_CORE = floor(NUM_PAGES / NUM_CORES)

    with concurrent.futures.ProcessPoolExecutor(max_workers=PROCESSES) as executor:
        futures = [executor.submit(start, i_core=i) for i in range(PROCESSES)]
        ok = concurrent.futures.wait(futures)

        for future in ok.done:
            # получаем результат
            result = future.result()
            print(f'Результат {result}')


if __name__ == '__main__':
    main()
