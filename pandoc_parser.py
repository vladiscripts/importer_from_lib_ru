#!/usr/bin/env python3
import time
import re
import threading, queue
from typing import Optional, Union, Tuple, List
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import html as html_
import mwparserfromhell as mwp
import pypandoc
from bs4 import BeautifulSoup, Tag

import db
from get_parsed_html import get_html, get_content_from_html, get_content_from_html_soup
from html2wiki import LibRu
from parser_html_to_wiki import *


class H(BaseModel):
    tid: int
    html: str
    soup: Optional[BeautifulSoup]
    wiki: Optional[str]

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


def convert_page(source):
    parser = LibRu()

    # match source:
    #     case BeautifulSoup() | Tag():
    #         soup = source
    #     case str():
    #         # parser.content = get_content_from_html_soup(d.soup)
    #         input_html = get_content_from_html(source)
    #         parser.soup = BeautifulSoup(input_html, 'html5lib')
    #     case _:
    #         raise Exception('source :BeautifulSoup | Tag | str')

    if isinstance(source, (BeautifulSoup, Tag)):
        soup = source
    elif isinstance(source, str):
        # parser.content = get_content_from_html_soup(d.soup)
        input_html = get_content_from_html(source)
        input_html
        parser.soup = BeautifulSoup(input_html, 'html5lib')
    else:
        raise Exception('source :BeautifulSoup | Tag | str')

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
    text = pypandoc.convert_text(html, 'mediawiki', format='html', verify_format=False)
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
    text = re.sub(r'^(?:<center>\s*)?(\* \* \*|\*\*\*)(?:\s*</center>)?$', '{{***}}', text, flags=re.MULTILINE)
    text = re.sub(r'^<center>\s*-{5}\s*</center>$', '{{bar}}', text, flags=re.MULTILINE)
    text = re.sub(r'^<center>\s*-{6,}\s*</center>$', '{{---}}', text, flags=re.MULTILINE)
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

    # strip параметр в {{right|}}
    for t in [t for t in wc.filter_templates(matches=lambda x: x.name == 'right')]:
        if t.params[0].value.strip() == '':
            wc.remove(t)
        else:
            t.params[0].value = t.params[0].value.strip()

    # for t in [t for t in wc.filter_tags(matches=lambda x: x.tag == 'div')]:
    #     t.params[0].value = t.params[0].value.strip()
    #
    # for t in wc.filter_tags(matches=lambda x: x.tag == 'div'):
    #     if t.tag == 'div' and t.get('align').value== 'center':
    #         t.attributes = []

    # g = [t for t in wc.filter_tags(matches=lambda x: x.tag == 'div' and x.get('align').value== 'right')]
    # [t for t in wc.filter_templates(matches=lambda x: x.name == 'right')]

    # mwp.parse('{{right|}}').nodes[0]

    for f in wc.filter_wikilinks(matches=lambda x: x.title.lower().startswith('file')):
        f.title = re.sub(r'^.+?/(text_\d+_).*?/([^/]+)$', r'File:\1\2', str(f.title))

    text = str(wc)

    # strip text
    text = strip_wikitext(text)

    return text


def convert_pages_to_db_with_pandoc_on_several_threads():
    lock = threading.RLock()
    q = queue.Queue(maxsize=100)
    db_q = queue.Queue(maxsize=10)

    def db_save_pool():
        while True:
            while db_q.empty():
                # print(f'db_q.empty')
                time.sleep(1)
            h = db_q.get()
            db.htmls.upsert({'tid': h.tid, 'wiki': h.wiki}, ['tid'])
            db_q.task_done()

    def worker():
        while True:
            while q.empty():
                # print(f'q.empty')
                time.sleep(1)
            h = q.get()
            # print(h.tid)
            # print(f'Finished {item}')
            # content_html = get_content_from_html(h.html)
            h.wiki = convert_page(h.html)
            h.html = None
            if h.wiki:
                # with lock:
                #     db.db_htmls.upsert({'tid': tid, 'wiki': text}, ['tid'])
                print('converted, to db', h.tid)
                db_q.put(h)
            else:
                print('no wiki', h.tid)
            q.task_done()

    threading.Thread(target=db_save_pool, daemon=True, name='db_save_pool').start()
    # for r in range(50):
    for r in range(100):
        threading.Thread(target=worker, daemon=True).start()

    count_pages_per_min = 0
    last_time = datetime.now()

    # for tid in [5643]:
    # for r in db.db_htmls.find(db.db_htmls.table.c.wiki.isnot(None)):
    # for r in db.htmls.find(wiki=None):
    # for r in db.db_htmls.find():
    t = db.all_tables
    cols = t.table.c
    tids_got = set()
    # for r in t.find(cols.title.is_not(None), cols.html.is_not(None), cols.wiki.is_(None)):
    while True:
        while q.full():
            # print('not q.empty()')
            time.sleep(10)
        if q.empty():
            pool = [r for r in t.find(cols.wiki.is_(None), cols.html.is_not(None), _limit=q.maxsize)]
            if not pool:
                break
            for r in pool:
                h = H.parse_obj(r)
                tids_got.add(h.tid)
                if h.tid in tids_got:
                    continue

                if last_time < datetime.now():
                    if last_time.minute < datetime.now().minute:
                        print(count_pages_per_min)
                        count_pages_per_min = 0
                        last_time = datetime.now()
                    else:
                        count_pages_per_min += 1

                q.put(h)

    # block until all tasks are done
    q.join()
    db_q.join()
    print('All work completed')


if __name__ == '__main__':
    from datetime import datetime

    convert_pages_to_db_with_pandoc_on_several_threads()

    # tid, html, url = get_html(tid=tid)
    # content_html = get_content_from_html(html)
    # text = convert_page(content_html)
    # print()
