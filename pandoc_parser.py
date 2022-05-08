#!/usr/bin/env python3
import time
from datetime import datetime
from math import floor
import re
import threading, queue
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


def pypandoc_converor(html) -> str:
    text = pypandoc.convert_text(html, 'mediawiki', format='html', verify_format=False)
    return text


def convert_page(h: H):
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

    text = pypandoc_converor(html)
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

    text = re.sub('([^\n])({{right\|)\s*', r'\1\n\2', text, flags=re.DOTALL)

    text = re.sub(r'([а-яa-z])', r'\1' + '\u0301', text, flags=re.I)  # ударение
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
            del (f)
            continue
        p = Path(link)
        replaces = {'.png': '---.jpg', '.gif': '----.jpg'}
        if p.suffix in replaces:
            p = p.with_name(p.name.replace(p.suffix, replaces[p.suffix]))
        try:
            name_ws = f'{p.parts[-3]}_{p.parts[-2]}_{p.name}'
        except IndexError as e:
            continue
        f.title = 'File:' + name_ws
        img = Image(tid=h.tid, urn=link, filename=p.name, name_ws=name_ws)
        h.images.append(img)
    return h


def main():
    q = queue.Queue(maxsize=500)  # fifo_queue
    db_q = queue.Queue(maxsize=5)

    def feeder() -> Optional[List[dict]]:
        # t = db.all_tables
        t = db.htmls
        cols = t.table.c

        chunk_size = 50
        offset = 0
        is_refetched = False

        while True:
            res = t.find(
                cols.html.is_not(None),
                # cols.wiki.is_(None),
                # cols.wiki2.is_(None),
                wiki_converted=1,
                # tid=88278,
                # tid=87499,
                # html={'like': '%%'},  # wiki2={'like': '%[[File:%'},
                _limit=chunk_size, _offset=offset)
            # _limit=limit, _offset=offset)
            for r in res:
                q.put(r)
            if res.result_proxy.rowcount == 0 or res.result_proxy.rowcount < chunk_size:
                if offset == 0 or is_refetched:
                    q.put(None)
                    break
                else:
                    if res.result_proxy.rowcount < chunk_size:
                        is_refetched = True
                    offset = 0
                    continue
            offset += chunk_size

    def db_save(h) -> None:
        rows = [img.dict() for img in h.images]
        with db.db as tx1:
            tx1['images'].delete(tid=h.tid)
            for row in rows:
                tx1['images'].insert_ignore(row, ['tid', 'name_ws'])
            # tx1['images'].insert_many([row for row in rows], ['tid', 'name_ws'])
            r = {'tid': h.tid, 'wiki': h.wiki_new, 'wiki_converted': 1}
            if h.wiki_new != h.wiki2:
                r.update({'wiki_differ_wiki2': 1})
            tx1['htmls'].update(r, ['tid'])

    def saver() -> None:
        while True:
            h = db_q.get()
            if h is None:
                break
            if h.wiki_new:
                db_save(h)
            db_q.task_done()

    def work_row(r):
        h = H.parse_obj(r)
        h = convert_page(h)
        h = process_images(h)
        h.wiki_new = strip_wikitext(str(h.wikicode))
        h.html = None
        h.wikicode = None
        if h.wiki_new:
            print('converted, to db', h.tid)
            # db_save(h)
        else:
            print('no wiki', h.tid)
        return h

    def worker():
        while True:
            r = q.get()
            if r is None:
                db_q.put(None)
                break
            h = work_row(r)
            db_q.put(h)
            q.task_done()

    with ThreadPoolExecutor(q.maxsize) as exec, \
            ThreadPoolExecutor(thread_name_prefix='db_save') as exec_db_save, \
            ThreadPoolExecutor(thread_name_prefix='feeder') as exec_feeder:
        # futures = [exec.submit(worker) for i in range(q.maxsize)]
        futures = [exec.submit(worker) for i in range(100)]
        futures += [
            exec_feeder.submit(feeder),
            exec_db_save.submit(saver)
        ]
        # results_db_save = exec_feeder.submit(feeder())
        # results_db_save = exec_db_save.submit(db_save)
        # results_workers = exec.submit(worker)
        for future in concurrent.futures.as_completed(futures):
            # url = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (future, exc))
                # print('%r generated an exception: %s' % (url, exc))
            else:
                print(future.result())
                pass

    # q.join()
    # db_q.join()


if __name__ == '__main__':
    main()
