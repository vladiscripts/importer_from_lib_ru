import re
import html as html_
from bs4 import BeautifulSoup, Comment, NavigableString

# import db

re_spaces_many_no_newlines = re.compile(r'[^\S\r\n]+')


def selector_from_html5(response):
    response = response.replace(
        encoding='utf-8',
        # body=str(BeautifulSoup(response.body, 'html.parser', from_encoding='cp1251')))
        body=str(BeautifulSoup(response.body, 'html5lib', from_encoding='cp1251')))
    return response


re_get_content_html = re.compile(r'<noindex>\s*<!---+ Собственно произведение -+>\s*(.+?)\s*<!-+--->\s*</noindex>',
                                 flags=re.S)


def get_html(tid=None):
    # filepath = 'text_old_orph.html'
    # html = Path(filepath).read_text(encoding='cp1251')

    # s = BeautifulSoup('''<dd> <div class="bold center"> <i>Первая <b>публикация</b>: </i></div><div class="a"> <i>"Русское слово" / 5 июля 1913 г.</i><i></i>
    # </div></dd>''', 'html5lib')

    if not tid:
        # tid = 5643
        # tid = 210
        # tid = 429
        tid = 6734  # <pre>
        # tid = 155

    r = all_tables.find_one(tid=tid)
    # html = r['content']
    html = r['html']
    global text_url
    text_url = r['text_url']
    # for r in db_texts.find():  # Ашар Амедей. В огонь и в воду. ДО
    #     html = r['html']
    #     #
    #     # wiki =
    #     db_wiki.insert({'tid': r['tid'], 'wiki': wiki}, ensure=True)

    if m := re_get_content_html.search(html):
        html = get_content_from_html(html)

        return tid, html, text_url


def get_content_from_html(html: str) -> str:
    if m := re_get_content_html.search(html):
        html = m.group(1)
        html = re.sub(r'</?xxx7>', '', html)
        html = html_.unescape(html)
        html = re_spaces_many_no_newlines.sub(' ', html)  #   и множественные пробелы, без переводов строк
        html = re.sub(r'<p( [^>]*)?>\s*(<br>)+', r'<p\1>', html, flags=re.I)

        # inline tags, пробелы и запятые/точки за тег
        html = re.sub(r'([\s.,]+)</(b|i|em|strong|emphasis)>', r'</\2>\1', html, flags=re.DOTALL)

        return html


def get_content_from_html_soup(soup) -> str:
    content = soup.find(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x). \
        find_parent('noindex')
    return content


def db_write_content_html():
    for r in db.htmls.all():
        html = r['html'].replace('&#1122;', 'Ѣ').replace('&#1123;', 'ѣ')
        soup = BeautifulSoup(html, 'html5lib')
        content = soup.find(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x). \
            find_parent('noindex').prettify()
        # dom = parsel.Selector(html)
        # content = dom.xpath('//noindex//comment()[contains(.,"Собственно произведение")]/parent::noindex').get()
        db.htmls.update({'tid': r['tid'], 'content': content}, ['tid'], ensure=True)

    # db_write_content_html()
