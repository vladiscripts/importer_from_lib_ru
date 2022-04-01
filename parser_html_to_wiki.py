import types
from typing import Union, List
import re
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
import html as html_

from get_parsed_html import get_html

tags_mapping = {
    'i': "''",
    'b': "'''",
}

re_spaces = re.compile(r'\s*')
re_spaces_many_no_newlines = re.compile(r'[^\S\r\n]+')
re_spaces_whole_string = re.compile(r'^[^\S\r\n]*$')
re_spaces_whole_string_with_newlines = re.compile(r'^\s*$')
re_begin_spaces = re.compile(r'^([^\S\r\n]+)')
re_end_spaces = re.compile(r'([^\S\r\n]+)$')
re_begin_spaces_with_newlines = re.compile(r'^(\s+)')
re_end_spaces_with_newlines = re.compile(r'(\s+)$')
re_corner_spaces = re.compile(r'^[^\S\r\n]+?(.*?)[^\S\r\n]+?$')

re_multi_newlines = re.compile(r'\n\n+')
re_begin_spaces_with_newlines_br = re.compile(r'^(\s|<br[\s/]*>)+')
re_end_spaces_with_newlines_br = re.compile(r'(\s|<br[\s/]*>)+$')

header_tag_names = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']


class HtmltoWikiBase:
    # def __init__(self, html: str):
    #
    #     # html = move_corner_spaces_from_tags_regex(html, add_tags=header_tag_names)
    #     # html = strip_tags_regex(html)
    #     # html = move_corner_spaces_from_tags_regex(html, default_tags=['p', 'dd'])
    #
    #     self.parsing()
    re_body_tag_remove = re.compile(r'^<body>\s*?(.+?)\s*?</body>$', flags=re.DOTALL)

    def parsing(self):
        soup = self.soup

        # self.html = html_.unescape(self.html)  # замена HTML-мнемоник на unicode
        move_corner_spaces_from_tags(soup)  # запускать перед inline_tags() для лучшего качества вики-текста
        self.inline_tags()

        tags_a(soup)

        # запуск расширения
        self.parsing_extentions()

        # пробелы и переводы строк между тегами
        unwrap_empty_inline_tags(soup, additinals_tags=['blockquote', 'p'])
        self.spaces_newlines_strip()

        tag_p_to_newline(soup)

        dev_to_center(soup)

    def inline_tags(self, soup):
        # *** inline-теги
        unwrap_empty_inline_tags(soup)  # пустые inline-теги ['i', 'b', 'em', 'strong']

        for tag_name in ['b', 'strong', 'i', 'em', 'emphasis', 'sup', 'sub']:  # 'span'
            soup = find_and_combine_tags(soup, tag_name)

        # Main markup
        replace_tag_with_wikimarkup(soup, 'i', "''")
        replace_tag_with_wikimarkup(soup, 'em', "''")
        replace_tag_with_wikimarkup(soup, 'b', "'''")
        replace_tag_with_wikimarkup(soup, 'strong', "'''")
        return soup

    def spaces_newlines_strip(self):
        soup = self.soup

        # *** пробелы и переводы строк
        replace_newlines_with_spaces(soup)
        strip_tags(soup)
        spaces_and_newlines_between_tags(soup)

        # <br>
        replace_br_with_newline_in_pre(soup)  # Переводы строк '\n' , исключая в <pre>
        # br_add_newline_after(soup)
        strip_empty_line_before_br(soup)

        print()

    def make_soup(self, html):
        self.html = html
        self.soup = BeautifulSoup(html)

    def parsing_extentions(self):
        """ Для перегрузки расширениями, выполняемым после базового парсинга """
        pass

    def to_wiki(self) -> str:
        # self.html = html_.unescape(self.html)  # замена HTML-мнемоник на unicode
        # text = str(self.soup.find('body'))
        text = self.soup.find('body').decode(formatter=None)
        text = self.re_body_tag_remove.search(text).group(1)
        return text


def find_tag_with_attr_value_exactly(soup, tag: str, attr: str, attr_values: Union[str, List[str]]):
    if isinstance(attr_values, list):
        """ Ищет теги с определённым значением атрибутов, исключая имеющие дополнительные значения.

        Значения атрбутов могут стоять в теге в разном порядке, не таком как задан в искомом. 
        Поэтому нужен перебор обоих списков значений (искомого и у просматриваемого тега.
        Здесь надо смотреть, поскольку часто бывает различное число значений и атрибутов, 
        например, множественные измения параметров шрифтов при том же классе и др. Требуются исследования таких случаев.
        """
        # def g(e):
        #     # d = [e for a, values in {'class': ['center', 'bold']}.items() for v in values if v in e.attrs.get(a, [])]
        #     # print(d)
        #     have_attrs = []
        #     for a, values in {'class': ['center', 'bold']}.items():
        #         if a in e.attrs:
        #             e_attr_values = e.attrs.get(a, [])
        #             for v in values:
        #                 if v in e_attr_values:
        #                     print(e)
        #                     have_attrs.append(True)
        #                     break
        #     return all(have_attrs)
        #
        # j = soup.find_all(lambda e: e.name == 'div' and g(e))

        raise NotImplementedError

    if isinstance(attr_values, str):
        return soup.find_all(lambda e: e.name == tag and e.attrs == {attr: attr_values})


def unwrap_tag(soup, tag: str, attrs: dict = None, attr_value_exactly=False):
    "By only exactly attrs values"
    attrs = attrs or {}
    for e in soup.find_all(tag, attrs):
        if attr_value_exactly and e.attrs != attrs:
            continue
        e.unwrap()


def replace_tag(soup, tag: str, attrs: dict, tag_repl: str, remove_attrs=True):
    """ Заменияет теги с определёнными значениями атрибутов.
    Применять с осторожностью. Поскольку тег может иметь несколько атрибутов или атрибут несколько значений,
    которые надо транслировать в разные wiki-сущности.
    """
    for e in soup.find_all(tag, attrs or {}):
        e.name = tag_repl
        if attrs and remove_attrs:
            del e[attrs]


def replace_tag_by_class_attr_exactly(soup, tag: str, class_names: list, tag_repl: str):
    """ Замениет теги с определёнными значениями атрибутов.
    Применять поиск `soup.find_all(tag_name, {attr: value})` нельзя, поскольку это даст все теги имеющие искомое
    значение атрибута. У атрибута `class` может быть несколько значений, поэтому в результате могут быть
    сторонние теги, измененение или удаление повредит их.

    replace_tag_by_class_attr_exactly(soup, 'div', 'class', 'center', 'center')

        <div class="center">text</div><div class="bold center">text</div>
        ->
        <center>text</center><div class="bold center">text</div>
    """
    elements = find_tag_with_attr_value_exactly(soup, tag, attr="class", attr_values=class_names)
    for e in elements:
        e.name = tag_repl
        e.attrs["class"].remove(class_names)


# def remove_tag_attr_value_exactly( soup, tag: str, attr: str, value: list):
#     elements = find_tag_with_attr_value_exactly(soup, tag, attr=attr, attr_value=value)
#     for e in elements:
#         e.name = tag_repl
#         e.attrs[tag].remove(value)


def replace_tag_by_attr_value_exactly(soup, tag: str, attr: str, attr_values: list, tag_repl: str):
    """ Ищет и заменияет теги с определёнными значениями атрибутов.
    Применять поиск `soup.find_all(tag_name, {attr: value})` нельзя, поскольку это даст все теги имеющие искомое
    значение атрибута. У атрибута `class` может быть несколько значений, поэтому в результате могут быть
    сторонние теги, измененение или удаление повредит их.

    replace_tag_by_class_attr_exactly(soup, 'div', 'class', 'center', 'center')

        <div class="center">text</div><div class="bold center">text</div>
        ->
        <center>text</center><div class="bold center">text</div>
    """
    elements = find_tag_with_attr_value_exactly(soup, tag, attr=attr, attr_values=attr_values)
    for e in elements:
        e.name = tag_repl
        e.attrs[tag].remove(attr_values)


def replace_bold_by_attr_value_exactly(soup, tag: str, attr: str, attr_values: list):
    """
    replace_bold_by_attr_value_exactly(soup, 'div', 'class', 'bold')

        <div class="bold center">text</div><div class="italic center">text</div>
        ->
        <div class="center">'''text'''</div><div class="italic center">text</div>
    """
    for e in soup.find_all(lambda e: e.name == tag and e.get(attr) == [attr_values]):
        e.insert(0, "'''")
        e.append("'''")
        e.attrs[attr].remove(attr_values)


def replace_italic_by_attr_value_exactly(soup, tag: str, attr: str, attr_values: list):
    """
    replace_italic_by_attr_value_exactly(soup, 'div', 'class', 'italic')

        <div class="italic center">text</div><div class="center">text</div>
        ->
        <div class="center">''text''</div><div class="center">text</div>
    """
    for e in soup.find_all(lambda e: e.name == tag and e.get(attr) == [attr_values]):
        e.insert(0, "''")
        e.append("''")
        e.attrs[attr].remove(attr_values)


def replace_center_by_attr_value_exactly(soup, tag: str, attr: str, attr_values: list):
    """
    replace_center_by_attr_value_exactly(soup, 'div', 'class', 'center')

        <div class="bold center">text</div><div class="italic">text</div>
        ->
        <center><div class="bold">text</div></center><div class="italic">text</div>
    """
    for e in soup.find_all(lambda e: e.name == tag and e.get(attr) == [attr_values]):
        e.wrap(soup.new_tag("center"))
        e.attrs['class'].remove('center')


def replace_tag_with_wikimarkup(soup, tag: str, repl: str):
    for e in soup.find_all(tag):
        e.insert(0, repl)
        e.append(repl)
        e.unwrap()


def replace_tag_i_to_wiki(soup):
    replace_tag_with_wikimarkup(soup, 'i', "''")


def replace_tag_b_to_wiki(soup):
    replace_tag_with_wikimarkup(soup, 'b', "'''")


def wrap_several_elements(first_element, condition_break, wrapper_element: Tag):
    """ condition_break - lambda, проверяется каждый элемент next_siblings на соответсвтие,
    False - добавляется в элемент-обёртку
    True - прерывание
    """
    e = first_element
    import itertools
    els = [i for i in itertools.takewhile(condition_break, e.next_siblings)]
    if els:
        e.wrap(wrapper_element)
        for tag in els:
            wrapper_element.append(tag)


def tags_a(soup):
    for e in soup.find_all('a'):

        # remove empty <a>  todo: проверить, бывает так что это якорь для сносок
        if anchor := e.attrs.get('name'):
            if e.attrs and 'href' in e.attrs:
                print()
            e.append("{{якорь|%s}}" % anchor)
            e.unwrap()

            if link.startswith('#'):
                e.insert(0, '[[%s|' % link)
                e.append(']')
            else:
                e.insert(0, '[%s ' % link)
                e.append(']')
            e.unwrap()


def dd_to_p(soup):
    for tag in soup.find_all('dd'):
        tag.name = 'p'
    # return soup


def check_for_parents_tag_name(tag, parent_tag_names: List[str]) -> bool:
    for e in tag.parents:
        if e.name in parent_tag_names:
            return True


def strip_p(soup):
    for tag in soup.find_all('p'):
        tag = tag.strip()


def strip_newlines_from_tag(tag):
    if not tag.contents:
        return
    while tag.contents:
        c = tag.contents[0]
        if is_string(c) and c == '\n':
            tag.contents.pop(0)
        else:
            break
    while tag.contents:
        c = tag.contents[-1]
        if is_string(c) and c == '\n':
            tag.contents.pop()
        else:
            break


def strip_tag(tag):  # , strip_newlines=True
    """ удаление крайных \s из тега
    todo: Что если строки не являются крайними, а скраю есть теги

    """
    if not tag.contents:
        return

    # while tag.contents:
    #     c = tag.contents[0]
    #     if not is_string(c):
    #         break
    #     if re_spaces_whole_string_with_newlines.match(c):
    #         c.extract()
    #         continue
    #     re_ = re_begin_spaces_with_newlines
    #     if re_.search(c):
    #         c.replace_with(re_.sub('', c))
    #         break
    #     else:
    #         break
    #
    # while tag.contents:
    #     c = tag.contents[-1]
    #     if not is_string(c):
    #         break
    #     if re_spaces_whole_string_with_newlines.match(c):
    #         c.extract()
    #         continue
    #     re_ = re_end_spaces_with_newlines
    #     if re_.search(c):
    #         c.replace_with(re_.sub('', c))
    #         break
    #     else:
    #         break

    strip(tag.contents, look_begin=True)
    strip(tag.contents, look_begin=False)


def strip(elems, look_begin=True):
    # o = list(elems)
    re2 = re_begin_spaces_with_newlines if look_begin else re_end_spaces_with_newlines

    def a(c):
        if not is_string(c):
            return
        if re_spaces_whole_string_with_newlines.match(c):
            c.extract()
            return True
        if re2.search(c):
            c.replace_with(re2.sub('', c))
            return
        else:
            return

    for c in elems:
        # if isinstance(elems, types.GeneratorType):
        #     try:
        #         c = next(elems)
        #     except StopIteration:
        #         break
        # else:
        #     c = elems[0] if look_begin else elems[-1]

        if a(c):
            continue
        else:
            break

    if not look_begin and not isinstance(elems, types.GeneratorType):
        while elems:
            c = elems[-1]
            if a(c):
                continue
            else:
                break

    # print()


def strip_tags(soup):
    """ удаление крайных \s из тега
    todo: Что если строки не являются крайними, а скраю есть теги

    Внутренние теги не просматриваются, поэтому предварительно запустить
    move_corner_spaces_from_tags()

    """
    block_tags = ['div', 'center', 'table', 'tbody', 'tr', 'td', 'pre', 'blockquote'] + header_tag_names
    tag_names = block_tags + ['p', 'dd']
    for tag_name in tag_names:
        for tag in soup.find_all(tag_name):
            if not is_excluded_subtag(tag):
                strip_tag(tag)


def strip_wikitext(text):
    text = re_begin_spaces_with_newlines_br.sub('', text)
    text = re_end_spaces_with_newlines_br.sub('', text)
    text = re_multi_newlines.sub('\n\n', text)
    return text


def tag_p_to_newline(soup):
    for e in soup.find_all('p'):
        if check_for_parents_tag_name(e, header_tag_names):
            # <p> внутри <h> обрабатывать особо
            # todo: в текущем врианте - не обрабатывать
            pass
        else:

            # удаление крайных \s из <p></p>
            # strip_tag(e)

            e.unwrap()


# html = '''<p>   <i><b>Первая публикация: </b></i></p><p><i>"Русское слово" / 5 июля 1913 г.</i><i></i>
# </p><p><i>   <b> <strong> <em> Первая публикация: </em> </strong> </b></i></p>'''


def move_corner_spaces_from_tags_regex(html, default_tags: list = None, add_tags: list = []):
    default_tags = default_tags or ['b', 'strong', 'i', 'em', 'emphasis', 'sup', 'sub', 'span']
    tag_names = default_tags + add_tags
    tag_names_re = '|'.join(tag_names)

    re_ = re.compile(r'<(?:%s)\s*>(\s+?)' % tag_names_re, flags=re.I)
    while re_.search(html):
        for n in tag_names:
            html = re.sub(r'<%s\s*>(\s+?)' % n, r'\1<%s>' % n, html)

    re_ = re.compile(r'(\s+?)</(?:%s)\s*>' % tag_names_re, flags=re.I)
    while re_.search(html):
        for n in tag_names:
            html = re.sub(r'(\s+?)</%s\s*>' % n, r'</%s>\1' % n, html)
    return html


def strip_tags_regex(html, default_tags: list = None, add_tags: list = []):
    default_tags = default_tags or header_tag_names + ['p', 'dd', 'div']
    tag_names = default_tags + add_tags

    for n in tag_names:
        html = re.sub(rf'<{n}\s*>\s+', f'<{n}>', html)
        html = re.sub(rf'\s+</{n}\s*>', f'</{n}>', html)
    return html


def move_corner_spaces_from_tags(soup):
    """ Moves spaces from begin and end of tags like <i> to parent tag.
    Example:
    <p><i>   <b>Первая публикация: </b></i>  ggg </p>
    <p>   <i><b>Первая публикация:</b></i>   ggg </p>
    """
    # soup = BeautifulSoup('''<p>   <i><b>Первая публикация: </b></i></p><p><i>"Русское слово" / 5 июля 1913 г.</i><i></i>
    # </p><p><i>   <b>Первая публикация: </b></i></p>''', 'html5lib')
    # u = soup.find_all('i')
    tags_names = ['em', 'strong', 'b', 'i', 'p', 'dd', 'a', 'span']
    # here the multiple 2. Althouth for correct processing 4 tags (example) need 10 iteration. ['i', 'b', 'em', 'strong']  1234 123 12 1, but too litlle chance met such nested tags
    for i in range(len(tags_names) * 2):
        for tag_name in tags_names:
            for e in soup.find_all(tag_name):
                if e.contents:
                    s = e.contents[0]
                    if is_string(s):
                        if spaces := re_begin_spaces_with_newlines.search(s):
                            s.replace_with(spaces.re.sub('', spaces.string))
                            e.insert_before(spaces.group(1))
                    s = e.contents[-1]
                    if is_string(s):
                        if spaces := re_end_spaces_with_newlines.search(s):
                            s.replace_with(spaces.re.sub('', spaces.string))
                            e.insert_after(spaces.group(1))
    return soup


def unwrap_empty_inline_tags(soup, additinals_tags=[]):
    for tag_name in set(['i', 'b', 'em', 'strong'] + additinals_tags):
        for tag in soup.find_all(tag_name):
            if re_spaces_whole_string_with_newlines.match(tag.text) and not tag.attrs:
                tag.unwrap()


def is_string(tag):
    if isinstance(tag, NavigableString):
        return True


def is_excluded_subtag(tag):
    exclude_tags = ('pre', 'code')
    if tag.name in exclude_tags or check_for_parents_tag_name(tag, exclude_tags):
        return True


def replace_newlines_with_spaces(soup):
    """ Remove newlines '\n', except in <pre> """

    def ch(e):
        for i, c in enumerate(e.contents):
            if is_string(c):
                if re.match('^\n*$', c):
                    c.extract()
                elif '\n' in c:
                    c.replace_with(c.replace('\n', ' '))

    # [e for e in soup.find_all('p') for c in e.contents if 'name="poyu-no-razve"' in str(c)]
    # for tag_name in ['p', 'dd', 'a', 'td'] + header_tag_names:
    # tag_name = None
    for tag in soup.find_all():
        if not is_excluded_subtag(tag):
            ch(tag)

            # if not is_excluded_subtag(tag):
            for e in tag.find_all():
                if not is_excluded_subtag(e):
                    ch(e)


def replace_br_with_newline_in_pre(soup):
    for pre in soup.find_all('pre'):
        for e in pre.find_all('br'):
            e.replace_with('\n')


def a(c, look_begin=True):
    if not is_string(c):
        return
    if re_spaces_whole_string_with_newlines.match(c):
        c.extract()
        return True

    re2 = re_begin_spaces_with_newlines if look_begin else re_end_spaces_with_newlines
    if re2.search(c):
        c.replace_with(re2.sub('', c))
        return
    else:
        return


def strip_empty_line_before_br(soup):
    for e in soup.find_all('br'):
        # c = e.previous_element
        # re_ = re_end_spaces_with_newlines
        # if is_string(c):
        #     if re_.search(c):
        #         c.replace_with(re_.sub('', c))
        # # c = e.next_element
        # re_ = re_begin_spaces_with_newlines
        # for c in e.next_elements:
        #     if not is_string(c):
        #         break
        #     if re_.search(c):
        #         c.replace_with(re_.sub('', c))
        list(e.previous_elements)
        # strip(e.previous_elements, look_begin=True)
        strip(e.previous_siblings, look_begin=False)
        # o=list(e.next_elements)
        b = list(e.next_siblings)

        # j = [a for a in e.next_siblings if isinstance(a, Tag) and a.name == 'table']
        # h = [t for t in e.next_siblings
        #  if isinstance(t, Tag) and t.name == 'a' and t.attrs.get('href') == '#byvaet-chas-toska' and not j]

        print()
        # strip(e.next_elements, look_begin=True)
        strip(e.next_siblings, look_begin=True)
        # strip(list(e.next_siblings), look_begin=True)
        print()

        elems = e.next_siblings
        look_begin = True

        # # while elems:
        # for c in elems:
        #     # if isinstance(elems, types.GeneratorType):
        #     #     try:
        #     #         c = next(elems)
        #     #     except StopIteration:
        #     #         break
        #     # else:
        #     #     c = elems[0] if look_begin else elems[-1]
        #
        #     if a(c):
        #         continue
        #     else:
        #         break

        # add newline after <br>
        if not is_string(e.next_element) \
                or (is_string(e.next_element) and not e.next_element.startswith('\n')):
            e.insert_after('\n')


# @deprecation.deprecated
# def br_add_newline_after(soup):
#     for e in soup.find_all('br'):
#         if not (is_string(e.next_element) and e.next_element.startswith('\n')):
#             e.insert_after('\n')


# def add_newline_after_tags(soup, tags_names: list):
#     for tag_name in tags_names:
#         for e in soup.find_all(tag_name):
#             if not (is_string(e.next_element) and e.next_element == '\n'):
#                 e.insert_after('\n')
#
#
# def add_newline_before_tags(soup, tags_names: list):
#     for tag_name in tags_names:
#         for e in soup.find_all(tag_name):
#             if not (is_string(e.previous_element) and e.previous_element == '\n'):
#                 e.insert_before('\n\n')


def remove_spaces_between_tags(soup, additinals_tags=[]):
    for tag_name in ['p', 'dd', 'table', 'tr', 'td', 'pre', 'blockquote'] + header_tag_names + additinals_tags:
        for e in soup.find_all(tag_name):
            a(e.previous_sibling, look_begin=True)
            a(e.next_sibling, look_begin=False)
    return soup


def spaces_and_newlines_between_tags(soup):
    remove_spaces_between_tags(soup, additinals_tags=[])

    # переводы строк между тегами
    for e in soup.find_all():
        if is_string(e):
            continue
        block_tags = ['div', 'center', 'pre', 'blockquote'] + header_tag_names
        if e.name in ['p', 'dd']:
            if e.previous_element:
                if e.previous_sibling.name not in block_tags:  # and not is_string(e.next_sibling)
                    e.insert_before('\n\n')
                elif e.previous_sibling.name in header_tag_names:  # and not is_string(e.next_sibling)
                    e.insert_before('\n\n')
            # if e.next_sibling and e.next_sibling.name not in block_tags:  # and not is_string(e.next_sibling)
            #     e.insert_after('\n\n')
        elif e.name in block_tags:
            if e.name in header_tag_names:
                e.insert_before('\n\n')
            else:
                e.insert_before('\n')
            if e.name in header_tag_names and isinstance(e.next_sibling, Tag) and e.next_sibling.name in ['p']:
                pass
            else:
                e.insert_after('\n\n')
        elif e.name in ['table']:
            e.insert(0, '\n')
            # e.append('\n')
            e.insert_after('\n')
        elif e.name in ['tbody']:
            e.unwrap()
        elif e.name in ['tr']:
            # e.insert_before('\n')
            # e.insert(0, '\n')
            e.append('\n')
            if (e.next_sibling and isinstance(e.next_sibling, Tag) and e.next_sibling.name == 'tr') \
                    or not e.next_sibling:
                e.insert_after('\n')
        elif e.name in ['td']:
            e.insert_before('\n')
            # e.insert_after('\n')


def find_and_combine_tags(soup, init_tag_name: str, init_tag_attrs: dict = None):
    def combine_tags(tag, tags: list):
        # appending the tag chain to the first tag
        for t in tags:
            tag.append(t)

        # unwrapping them
        for t in tag.find_all(init_tag_name):
            if t.name == init_tag_name and t.attrs == init_tag_attrs:
                t.unwrap()

    def fill_next_siblings(tag, init_tag_name: str, ignoring_tags_names: list) -> list:
        next_siblings = []
        next_siblings_all = list(tag.next_siblings)
        for t in tag.next_siblings:
            if is_string(t) and re_symbols_ignore and re_symbols_ignore.match(t):
                next_siblings.append(t)
            # elif is_string(t) and re_symbols_ignore and not re_symbols_ignore.match(t):
            #     next_siblings.append(t)
            elif t.name in ignoring_tags_names and t.attrs == init_tag_attrs:
                next_siblings.append(t)
            else:
                # filling `next_siblings` until another tag met
                break

        has_other_tag_met = False
        for t in next_siblings:
            if t.name == init_tag_name and t.attrs == init_tag_attrs:
                has_other_tag_met = True
                break

        # removing unwanted tags on the tail of `next_siblings`
        if has_other_tag_met:
            while True:
                last_tag = next_siblings[-1]
                if is_string(last_tag):
                    next_siblings.pop()
                elif last_tag.name != init_tag_name and last_tag.attrs != init_tag_attrs:
                    next_siblings.pop()
                else:
                    break
            return next_siblings

    init_tag_attrs = init_tag_attrs or {}

    # Ignore nested tags names
    if init_tag_name in ['b', 'strong', 'i', 'em', 'emphasis']:
        ignoring_tags_names = ['b', 'strong', 'i', 'em', 'emphasis']
    elif init_tag_name in ['div']:
        # A block tags can have many nested tags
        ignoring_tags_names = ['div', 'p', 'span', 'a']
    else:
        ignoring_tags_names = []

    # Some symbols between same tags can add into them. Because they don't changing of font style.
    if init_tag_name == 'i':
        # Italic doesn't change the style of some characters (spaces, period, comma), so they can be combined
        re_symbols_ignore = re.compile(r'^(\s*|[.,-])$')
    elif init_tag_name == 'b':
        # Bold changes the style of all characters
        re_symbols_ignore = re.compile(r'^\s*$')
    elif init_tag_name == 'div':
        # Here should be careful with merging, because a html can have some `\n` between block tags (like `div`s)
        re_symbols_ignore = re.compile(r'^\s*$')
    else:
        re_symbols_ignore = None

    all_wanted_tags = soup.find_all(init_tag_name)
    if all_wanted_tags:
        tag_groups_to_combine = []
        tag = all_wanted_tags[0]
        last_tag = tag
        while True:
            tags_to_append = fill_next_siblings(tag, init_tag_name, ignoring_tags_names)
            if tags_to_append:
                tag_groups_to_combine.append((tag, tags_to_append))  # the first tag and tags to append

            # looking for a next tags group
            last_tag = tags_to_append[-1] if tags_to_append else tag
            for tag in all_wanted_tags:
                if tag.sourceline is None or last_tag.sourceline is None:
                    continue
                if tag.sourceline > last_tag.sourceline \
                        or (tag.sourceline == last_tag.sourceline and tag.sourcepos > last_tag.sourcepos):
                    break
            if last_tag.sourceline == all_wanted_tags[-1].sourceline and last_tag.sourcepos == last_tag.sourcepos:
                break
            last_tag = tag

        for first_tag, tags_to_append in tag_groups_to_combine:
            combine_tags(first_tag, tags_to_append)

    return soup


def dev_to_center(soup):
    for e in soup.find_all(lambda e: e.name == 'div' and e.attrs == {'align': 'center'}):
        e.name = "center"
        e.attrs = None  # e.attrs['align'].remove('center')
        e.insert_before('\n\n')

# if __name__ == '__main__':
#     soup = BeautifulSoup('''<div>
#         <p>A<i>b</i>cd1, <i>a</i><b><i>b</i></b><i>cd2</i> abcd3 <i>ab</p>
#         <p>cd4</i> <i>a</i><i>bcd5</i> <i>ab<span>cd6</span></i></p>
#     </div>''', 'html.parser')
#     print(soup)
#
#     find_and_combine_tags(soup, 'i')
#
#     print(soup)
