from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import requests
from vladi_helpers.file_helpers import json_save_to_file, json_load_from_file, file_savetext, file_readtext
from vladi_helpers.vladi_helpers import url_params_str_to_dict, url_params_str_to_list
import parsel
import os
from pathlib import Path
import re
from bs4 import BeautifulSoup, Comment, NavigableString

from get_parsed_html import get_html, make_soup
from parser_html_to_wiki import *


def headers_clean(soup):
    for h_ in header_tag_names:
        for h in soup.find_all(h_):

            # битые теги h пропускаем и удаляем. без завершающего тега или поставленного далеко, которые включают текст
            if h.find_all('dd'):
                h.unwrap()
                continue

            # иначе чистим
            for b in h.find_all('b'):
                b.unwrap()

            for e in h.find_all('div', {'align': 'center'}):
                if isinstance(e.attrs, dict) and len(e.attrs) == 1:
                    e.unwrap()

            # вынос картинок из <h>
            for img in h.find_all('img'):
                if img.parent.name == 'p':
                    img.parent.unwrap()

                h.insert_after(img)
                nt = soup.new_tag('center')
                img.wrap(nt)

            for p in h.find_all('p'):
                if p.text.strip() == '':
                    p.unwrap()

            strip_tag(h)  # удаление крайных \s

    # Если на странице нет h2 или h3, то менять h4 на h3
    h2s = list(soup.find_all('h2'))
    h3s = list(soup.find_all('h2'))
    h4s = list(soup.find_all('h2'))
    if h4s and not h2s and not h3s:
        for h in h4s:
            h.name = 'h3'


html = get_html()
soup = make_soup(html)

# Убирка лишнего
# теги
# soup.find('noindex').unwrap()

# Переводы строк '\n', исключая в <pre>
remove_newlines(soup)

# пустые inline-теги ['i', 'b', 'em', 'strong']
unwrap_empty_inline_tags(soup)

unwrap_tag(soup, 'xxx7')
unwrap_tag(soup, 'div', {'align': 'justify'})

# комментарии
# for c in soup.find_all(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x):
#     print(c)
#     c.extract()

# заголовки
headers_clean(soup)

find_and_combine_tags(soup, 'i')
find_and_combine_tags(soup, 'b')
replace_tag_i(soup)
replace_tag_b(soup)
move_corner_spaces_from_inline_tags(soup)

find_and_combine_tags(soup, 'div', {'align': 'center'})
for e in soup.find_all(lambda e: e.name == 'div' and e.attrs == {'align': 'center'}):
    e.name = "center"
    # e.attrs['align'].remove('center')
    e.attrs = None

# remove empty <a>  todo: проверить, бывает так что это якорь для сносок
for e in soup.find_all('a'):
    if anchor := e.attrs.get('name'):
        if e.attrs and 'href' in e.attrs:
            print()
        e.append("{{якорь|%s}}" % anchor)
        e.unwrap()
replace_tag_a(soup)

br_add_newline_after(soup)

for tag in soup.find_all('dd'):
    tag.name = 'p'
tag_p_to_newline(soup)

print()

if __name__ == '__main__':
    # Regexp: Поиск с negative lookahead: двойных теги <p> внутри <h4>
    s = '''<dd>   
    <h4><div align="center">
    <p>
    <b>ЧАСТЬ ПЕРВАЯ.</b>
    </p>
    </div></h4>
    <div align="center">
    <p>
    I.<br/><h4><b>Игорный <p>домъ</p> въ<p> осеннюю</p> ночь.</b></h4>
    </p>
    </div>
    </dd>'''
    f = re.findall('<h4>(?:(?!</p>).)*?<p>(?:(?!<p>).)*?</h4>', s, flags=re.DOTALL)
    f1 = re.findall('<h4>(?:(?!</h4>).)*?</p>(?:(?!</h4>).)*?<p>', s, flags=re.DOTALL)
    """
    SELECT 'http://az.lib.ru' || authors.slug || '/' || titles.slug, html from htmls
    join titles on titles.id = htmls.tid
    join authors on authors.id = titles.author_id
    where content REGEXP '<h4>(?:(?!</h4>).)*</p>(?:(?!</h4>).)*<p>'
    -- limit 1
    """
