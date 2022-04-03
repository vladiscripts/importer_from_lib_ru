#!/usr/bin/env python3
import time
from datetime import datetime
from math import floor
import re
import threading, queue
import asyncio
import concurrent.futures  # Позволяет создать новый процесс
from multiprocessing import cpu_count  # Вернет количество ядер процессора
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
    wiki: Optional[str] = Field(..., alias='wiki2')
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
    text = html_.unescape(text.strip('/'))

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
    text = re.sub(r'^<center>\s*-{5}\s*</center>$', r'{{---|width=6em}}', text, flags=re.MULTILINE)
    text = re.sub(r'^<center>\s*-{6,}\s*</center>$', r'{{---|width=10em}}', text, flags=re.MULTILINE)
    text = re.sub(r'<center>\s*(\[\[File:[^]]+?)\]\]\s*</center>', r'\1|center]]', text)

    text = re.sub(r'([Α-Ω]+)', r'{{lang|grc|\1}}', text, flags=re.I)

    text = re.sub(r'(\n==+[^=]+?<br[/ ]*>)\n+', r'\1', text)  # fix: \n после <br> в заголовках

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
    text = str(wc)
    h.wiki = strip_wikitext(text)

    return h


def process_images(h):
    """ to simple names of images """
    wc = h.wikicode
    for f in wc.filter_wikilinks(matches=lambda x: x.title.lower().startswith('file')):
        link = re.sub(r'^[Ff]ile:', '', str(f.title))
        p = Path(link)
        # f.title = re.sub(r'^.+?/(text_\d+_).*?/([^/]+)$', r'File:\1\2', str(f.title))
        # name_ws = re.search(r'^(text_\d+_).+', p.parts[-2]).group(1) + p.name
        try:
            p.parts[-2]
        except IndexError as e:
            continue
        name_ws = f'{p.parts[-2]}_{p.name}'
        f.title = 'File:' + name_ws
        img = Image(tid=h.tid, urn=link, filename=p.name, name_ws=name_ws)
        h.images.append(img)
    return h


count_pages_per_min = 0
last_time = datetime.now()

# processed = set()

q_feeder = queue.Queue(maxsize=50)


def db_feeder():
    # t = db.all_tables
    t = db.htmls
    cols = t.table.c

    chunk_size = 500  # rows
    offset = 0

    while True:
        res = t.find(
            cols.html.is_not(None),
            # cols.wiki.is_(None),
            cols.wiki2.is_(None),
            # tid=144927,  # wiki={'like':'%[[File:%'},
            _limit=chunk_size, _offset=offset)
        # _limit=limit, _offset=offset)
        if res.result_proxy.rowcount == 0:
            if offset == 0:
                q_feeder.put(None)
                break
            else:
                offset = 0
        else:
            offset += chunk_size
            for r in res:
                q_feeder.put(r)
        while q_feeder.unfinished_tasks:
            time.sleep(0.5)
    # q_feeder.task_done()


class AsyncWorker:
    # offset = 0  # db_feel_pool    # q.maxsize
    # limit = 100  # db_feel_pool
    # PAGES_PER_CORE :int
    i_core: int

    def start(self):
        self.offset = self.limit * self.i_core  # стартовые значения offset для запроса из БД
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.asynchronous(loop))
        loop.close()

    async def asynchronous(self, loop):
        while True:
            # if q_feeder.all_tasks_done:
            #     break
            # while not q_feeder.unfinished_tasks:
            # while q_feeder.empty():
            #     time.sleep(0.5)
            tasks = []
            # for _ in range(q_feeder.unfinished_tasks):
            for _ in range(2):
                r = q_feeder.get()
                if r is None:
                    break
                tasks.append(asyncio.create_task(self.work_row(r)))
            # tasks = [asyncio.create_task(self.work_row(r)) for r in range(100)]
            finished, unfinished = await asyncio.wait(tasks)
            if len(unfinished):
                logging.error('have unfinished async tasks')

    async def convert_page(self, h) -> H:
        return convert_page(h)

    async def process_images(self, h) -> H:
        return process_images(h)

    async def db_feeder(self) -> Optional[List[dict]]:
        # t = db.all_tables
        t = db.htmls
        cols = t.table.c

        while True:
            res = t.find(
                cols.html.is_not(None),
                # cols.wiki.is_(None),
                cols.wiki2.is_(None),
                # tid=144927,  # wiki={'like':'%[[File:%'},
                _limit=self.limit, _offset=self.offset)
            # _limit=limit, _offset=offset)

            if res.result_proxy.rowcount == 0:
                if offset == 0:
                    break
                else:
                    offset = 0
            else:
                self.offset += (self.limit * (self.i_core + 1))
                rows = [r for r in res]
                return rows

    async def db_save_pool(self, h) -> None:
        rows = [img.dict() for img in h.images]
        with db.db as tx1:
            tx1['images'].delete(tid=h.tid)
            for row in rows:
                tx1['images'].insert_ignore(row, ['tid', 'name_ws'])
            tx1['htmls'].update({'tid': h.tid, 'wiki2': h.wiki}, ['tid'])

    async def work_row(self, r):
        # r = await q_feeder.get(r)
        h = H.parse_obj(r)
        h = await convert_page(h)
        # h = await self.convert_page(h)
        h = await self.process_images(h)
        h.html = None
        h.wikicode = None
        if h.wiki:
            print('converted, to db', h.tid)
            # if h.tid in processed:
            #     print('!!!! in processed already')
            # else:
            #     processed.add(h.tid)

            # await self.db_save_pool(h)
        else:
            print('no wiki', h.tid)
        q_feeder.task_done()


def start(i_core: int):
    w = AsyncWorker()
    # w.PAGES_PER_CORE = num_pages
    w.i_core = i_core
    w.start()


def main():
    # NUM_PAGES = 100  # Суммарное количество страниц для скрапинга
    NUM_CORES = cpu_count() - 1  # Количество ядер CPU (влкючая логические)
    # PAGES_PER_CORE = floor(NUM_PAGES / NUM_CORES)

    threading.Thread(target=db_feeder, daemon=True, name='db_feeder').start()

    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_CORES) as executor:
        futures = [executor.submit(start, i_core=i) for i in range(NUM_CORES)]
        ok = concurrent.futures.wait(futures)

        for future in ok.done:
            # получаем результат
            result = future.result()
            print(f'Результат {result}')
    q_feeder.join()


if __name__ == '__main__':
    main()
