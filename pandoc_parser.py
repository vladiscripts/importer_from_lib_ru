#!/usr/bin/env python3
import time
import re
import threading, queue
import pypandoc
import html as html_
import mwparserfromhell as mwp

import db
from get_parsed_html import get_html
from html2wiki import LibRu
from parser_html_to_wiki import *


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


def convert_page(tid):
    parser = LibRu()

    tid, input_html, url = get_html(tid=tid)

    parser.make_soup(input_html)
    move_corner_spaces_from_tags(parser.soup)
    tags_a_refs(parser.soup)
    parser.parsing_extentions()
    parser.inline_tags()
    # parser.soup.smooth()
    remove_spaces_between_tags(parser.soup, additinals_tags=[])

    # for e in parser.soup('pre'):
    #     e.name = 'poem'  # 'poem'
    #     e.append(parser.soup.new_tag('p'))
    # for e in parser.soup('blockquote'):
    #     e.unwrap()

    for t in parser.soup.find_all('dd'):
        t.name = 'p'

    html = parser.soup.decode()
    text = pypandoc.convert_text(html, 'mediawiki', format='html')
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

    text = str(wc)

    # strip text
    text = strip_wikitext(text)

    return text


def convert_pages_to_db_with_pandoc_on_several_threads():
    lock = threading.RLock()
    q = queue.Queue(maxsize=20)
    db_q = queue.Queue(maxsize=20)

    def db_save():
        while True:
            while fifo_queue.empty():
                time.sleep(1)
            r = db_q.get()
            db.htmls.upsert({'tid': r[0], 'wiki': r[1]}, ['tid'])
            print('to db', tid)
            db_q.task_done()

    def worker():
        while True:
            while fifo_queue.empty():
                time.sleep(1)
            r = q.get()
            # print(f'Working on {item}')
            # print(f'Finished {item}')
            tid = r['tid']
            html = r['html']
            text = convert_page(tid)
            # with lock:
            #     db.db_htmls.upsert({'tid': tid, 'wiki': text}, ['tid'])
            db_q.put((tid, text))
            print('converted', tid)
            q.task_done()

    threading.Thread(target=db_save, daemon=True).start()

    # turn-on the worker thread
    for r in range(20):
        threading.Thread(target=worker, daemon=True).start()

    # for tid in [5643]:
    # for r in db.db_htmls.find(db.db_htmls.table.c.wiki.isnot(None)):
    for r in db.htmls.find(wiki=None):
        # for r in db.db_htmls.find():
        while q.full():
            time.sleep(1)
        q.put(r)

    # block until all tasks are done
    q.join()
    print('All work completed')


if __name__ == '__main__':
    text = convert_page(tid=1)
    print()