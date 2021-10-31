from bs4 import BeautifulSoup, Comment, NavigableString
from db import *


def selector_from_html5(response):
    response = response.replace(
        encoding='utf-8',
        # body=str(BeautifulSoup(response.body, 'html.parser', from_encoding='cp1251')))
        body=str(BeautifulSoup(response.body, 'html5lib', from_encoding='cp1251')))
    return response


def get_html():
    # filepath = 'text_old_orph.html'
    # html = Path(filepath).read_text(encoding='cp1251')

    # s = BeautifulSoup('''<dd> <div class="bold center"> <i>Первая <b>публикация</b>: </i></div><div class="a"> <i>"Русское слово" / 5 июля 1913 г.</i><i></i>
    # </div></dd>''', 'html5lib')

    # r = db_all_tables.find_one(tid=5643)  # Ашар Амедей. В огонь и в воду. ДО
    # r = db_all_tables.find_one(tid=210)  # Ашар Амедей. В огонь и в воду. ДО
    # r = db_all_tables.find_one(tid=429)  # Ашар Амедей. В огонь и в воду. ДО
    # r = db_all_tables.find_one(tid=155)  # Ашар Амедей. В огонь и в воду. ДО
    r = db_all_tables.find_one(tid=6734)  # <pre>
    # html = r['content']
    html = r['html']
    global text_url
    text_url = r['text_url']
    # for r in db_texts.find():  # Ашар Амедей. В огонь и в воду. ДО
    #     html = r['html']
    #     #
    #     # wiki =
    #     db_wiki.insert({'tid': r['tid'], 'wiki': wiki}, ensure=True)
    return html


def make_soup(html):
    soup_html = BeautifulSoup(html, 'html5lib')
    begin = soup_html.find(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x)
    parent_tag = begin.parent
    begin.extract()

    end = parent_tag.find(text=lambda x: isinstance(x, Comment) and '-------------------------------------------' in x)
    ends = list(end.find_all_next())
    end.extract()
    for e in ends:
        if isinstance(e, NavigableString):
            e.extract()
        else:
            e.decompose()
    soup = BeautifulSoup(str(parent_tag), 'html5lib')
    return soup


def db_write_content_html():
    for r in db_htmls.all():
        html = r['html'].replace('&#1122;', 'Ѣ').replace('&#1123;', 'ѣ')
        soup = BeautifulSoup(html, 'html5lib')
        content = soup.find(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x). \
            find_parent('noindex').prettify()
        # dom = parsel.Selector(html)
        # content = dom.xpath('//noindex//comment()[contains(.,"Собственно произведение")]/parent::noindex').get()
        db_htmls.update({'tid': r['tid'], 'content': content}, ['tid'], ensure=True)

    # db_write_content_html()
