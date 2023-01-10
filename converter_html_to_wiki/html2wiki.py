from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
# import requests
# import parsel
import os
from pathlib import Path
import re
from bs4 import BeautifulSoup, Comment, NavigableString
import html as html_

# from db import *
from .get_parsed_html import get_html
from .parser_html_to_wiki import *


class LibRu(HtmltoWikiBase):

    def make_soup(self, html):
        self.html_origin = html
        soup = BeautifulSoup(html, 'html5lib')
        return soup

    def make_soup_via_parser(self, html):
        self.html_origin = html

        soup = BeautifulSoup(html, 'html5lib')
        begin = soup.find(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x)
        parent_tag = begin.parent
        begin.extract()

        end = parent_tag.find(
            text=lambda x: isinstance(x, Comment) and '-------------------------------------------' in x)
        try:
            ends = list(end.find_all_next())
        except:
            print()
        end.extract()
        for e in ends:
            if isinstance(e, NavigableString):
                e.extract()
            else:
                e.decompose()

        self.html_bs = str(parent_tag)
        soup = BeautifulSoup(self.html_bs, 'html5lib')

        noindex = next(soup.find('body').children)
        if noindex.name == 'noindex':
            noindex.unwrap()

        self.soup = soup

    def parsing_extentions(self,soup):
        find_and_combine_tags(soup, 'div', {'align': 'center'})

        # ошибочные <div> в [<p>, <dd>], вынуть
        for e in soup.find_all('div'):
            if e.parent.name == 'dd':
                if e.next_sibling is None \
                        or all(
                    (is_string(t) and re_spaces_whole_string_with_newlines.match(t) for t in e.next_siblings)):
                    e.parent.insert_after(e)

        unwrap_tag(soup, 'xxx7')
        unwrap_tag(soup, 'div', {'align': 'justify'}, attr_value_exactly=True)
        unwrap_tag(soup, 'mytag')
        unwrap_tag(soup, 'div', {'id': 'tipBox'}, attr_value_exactly=True)
        for e in soup.find_all('img'):
            if e.attrs.get('src') == "/stixiya/img/spacer.gif":
                e.extract()
            if e.attrs.get('alt') and re.match(r'^\W+$', e.attrs.get('alt')):
                e.attrs['alt'] = ''

        for table in soup.find_all('table'):
            for e1 in table.descendants:
                if e1.name == 'img' and e1.attrs.get('alt') in ["TopList", "Rambler's Top100"]:
                    table.extract()
                    break

        for e in soup.find_all('a'):
            if link := e.attrs.get('href'):
                if link == 'http://az.lib.ru' and link == e.text:
                    e.extract()
                elif e.text.strip() == '' or link.rstrip('/') == e.text.strip().rstrip('/'):
                    e.unwrap()
                elif link == e.text or 'smalt.karelia.ru' in link or '.da.ru' in link:
                    e.unwrap()
                elif link.startswith('file:'):
                    e.unwrap()



        unwrap_tag(soup, 'font', None, attr_value_exactly=True)
        unwrap_empty_inline_tags(soup, additinals_tags=['blockquote'])


        # комментарии
        # for c in soup.find_all(text=lambda x: isinstance(x, Comment) and 'Собственно произведение' in x):
        #     print(c)
        #     c.extract()

        # заголовки
        soup = self.headers_clean(soup)

        # for tag in soup.find_all('dd'):
        #     tag.name = 'p'

        for e in soup.find_all(lambda e: e.name == 'div' and e.attrs == {'align': 'right'}):
            # strip_tag(e)
            e.insert_before('\n\n')
            e.insert(0, '{{right|')
            e.append('}}')
            e.unwrap()

    # [e for e in parser.soup.find_all(text=lambda x: is_string(x) and x in ['* * *', '***'])]
        return soup

    def headers_clean(self, soup):
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
                    nt = self.soup.new_tag('center')
                    img.wrap(nt)

                for p in h.find_all('p'):
                    if p.text.strip() == '':
                        p.unwrap()

                # strip_tag(h)  # удаление крайных \s

        # Если на странице нет h2 или h3, то менять h4 на h3
        h2s = list(soup.find_all('h2'))
        h3s = list(soup.find_all('h3'))
        h4s = list(soup.find_all('h4'))
        if h4s and not h2s and not h3s:
            for h in h4s:
                h.name = 'h3'

        def p_in_headers(soup):
            # unwrap <p> в h если это одиночный <p>
            for h_ in header_tag_names:
                hs = soup.find_all(h_)
                for h in hs:
                    sub_p = h.find_all('p')
                    if len(sub_p) == 1:
                        sub_p[0].unwrap()
                    else:
                        continue
                    # strip_tag(h)
                    # strip_newlines_from_tag(h)

        p_in_headers(soup)

        return soup

if __name__ == '__main__':
    tid, html, url = get_html()
    parser = LibRu()
    parser.make_soup(html)
    parser.parsing()
    text = parser.to_wiki()

    print()

    # db_wiki.upsert({'tid': tid, 'text': text}, ['tid'], ensure=True)
    # if not re.search(r'<(?!h[1-6]).*?>'):
    #     db_wiki.insert_ignore({'tid':tid,'text': text}, ['tid'], ensure=True)

if __name__ == '***__main__':
    # if __name__ == '__main__':
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
