from typing import Union, List
import re
from bs4 import BeautifulSoup, NavigableString

tags_mapping = {
    'i': "''",
    'b': "'''",
}

re_spaces = re.compile(r'\s*')
re_begin_spaces = re.compile(r'^\s+')
re_end_spaces = re.compile(r'\s+$')

header_tag_names = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']


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
        return soup.find_all(lambda e: e.name == tag and e.attrs.get(attr) == attr_values)


def unwrap_tag(soup, tag: str, attrs: dict = None):
    "By only exactly attrs values"
    for e in soup.find_all(tag, attrs):
        if isinstance(e.attrs, dict):
            if e.attrs == attrs:
                e.unwrap()
        else:
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
    elements = self.find_tag_with_attr_value_exactly(soup, tag, attr="class", attr_values=class_names)
    for e in elements:
        e.name = tag_repl
        e.attrs["class"].remove(class_names)


# def remove_tag_attr_value_exactly( soup, tag: str, attr: str, value: list):
#     elements = self.find_tag_with_attr_value_exactly(soup, tag, attr=attr, attr_value=value)
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
    elements = self.find_tag_with_attr_value_exactly(soup, tag, attr=attr, attr_values=attr_values)
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


def replace_tag_i(soup):
    replace_tag_with_wikimarkup(soup, 'i', "''")


def replace_tag_b(soup):
    replace_tag_with_wikimarkup(soup, 'b', "'''")


def replace_tag_a(soup):
    for e in soup.find_all('a'):
        if link := e.attrs.get('href'):
            e.insert(0, "[%s" % link)
            e.append("]")
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


def strip_tag(tag):
    """ удаление крайных \s из тега """
    if not tag.contents:
        return
    map = ((tag.contents[0], re_begin_spaces), (tag.contents[-1], re_begin_spaces))
    for c, re_ in map:
        if isinstance(c, NavigableString) and re_.match(c):
            c.replace_with(re_.sub(c, ''))


def tag_p_to_newline(soup):
    for h_ in header_tag_names:
        for h in soup.find_all(h_):
            sub_p = h.find_all('p')
            if len(sub_p) == 1:
                sub_p[0].unwrap()
            else:
                continue

    for p in soup.find_all('p'):
        if not check_for_parents_tag_name(p, header_tag_names):

            # удаление крайных \s из <p></p>
            strip_tag(e)

            # добавление \n.
            # не корректно работает, если предшествует ['', '\n'] - если есть пустая строка вне тега перед <p>, то добавляется лишняя пустая строка.
            # такие пустые строки могут возникать если был unwraped пустой тег
            # надо либо убирать пустые строки. либо удалять \n\n\n+ из конечного текста. но это не лучший вариант, ибо не парсер.
            if isinstance(e.previous_sibling, NavigableString):
                if e.previous_sibling == '\n\n':
                    pass
                elif e.previous_sibling == '\n':
                    e.insert(0, '\n')
            e.append('\n')

            e.unwrap()

            # for e in soup.find_all('p'):
    #     if re.match(r'^\n+$', e.contents[0]):
    #         e.contents.pop(0)
    #     if re.match(r'^\n+$', e.contents[-1]):
    #         e.contents.pop()


def move_corner_spaces_from_inline_tags(soup):
    """ Moves spaces from begin and end of tags like <i> to parent tag.
    Example:
    <p><i>   <b>Первая публикация: </b></i>  ggg </p>
    <p>   <i><b>Первая публикация:</b></i>   ggg </p>
    """
    # soup = BeautifulSoup('''<p>   <i><b>Первая публикация: </b></i></p><p><i>"Русское слово" / 5 июля 1913 г.</i><i></i>
    # </p><p><i>   <b>Первая публикация: </b></i></p>''', 'html5lib')
    # u = soup.find_all('i')
    re_begin_spaces = re.compile(r'^(\s+)', flags=re.DOTALL)
    re_end_spaces = re.compile(r'(\s+)$', flags=re.DOTALL)
    tags_names = ['em', 'strong', 'b', 'i', 'p']
    # here the multiple 2. Althouth for correct processing 4 tags (example) need 10 iteration. ['i', 'b', 'em', 'strong']  1234 123 12 1, but too litlle chance met such nested tags
    for i in range(len(tags_names) * 2):
        for tag_name in tags_names:
            k = soup.find_all(tag_name)
            for e in k:
                if e.contents:
                    if isinstance(e.contents[0], NavigableString):
                        spaces = re_begin_spaces.search(e.contents[0])
                        if spaces:
                            e.contents[0].string.replace_with(spaces.re.sub('', spaces.string))
                            e.insert_before(spaces.group(1))
                    if isinstance(e.contents[-1], NavigableString):
                        spaces = re_end_spaces.search(e.contents[-1])
                        if spaces:
                            e.contents[-1].string.replace_with(spaces.re.sub('', spaces.string))
                            e.insert_after(spaces.group(1))


def unwrap_empty_inline_tags(soup):
    for tag_name in ['i', 'b', 'em', 'strong']:
        for tag in soup.find_all(tag_name):
            if re_spaces.match(tag.text) and not 'name' in tag.attrs:
                tag.unwrap()


def remove_newlines(soup):
    """ Remove newlines '\n', except in <pre> """
    for e in soup.find_all():
        for i, c in enumerate(e.contents):
            if isinstance(c, NavigableString) and '\n' in c and (i > 0 and e.contents[i - 1].name != 'br'):
                if not check_for_parents_tag_name(p, ['pre', 'code']):
                    c.replace_with(c.replace('\n', ' '))


def br_add_newline_after(soup):
    for e in soup.find_all('br'):
        if not (isinstance(e.next_element, NavigableString) and e.next_element == '\n'):
            e.insert_after('\n')


def find_and_combine_tags(soup, init_tag_name: str, init_tag_attrs: dict = None or {}):
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
        # next_siblings_all = list(tag.next_siblings)
        for t in tag.next_siblings:
            if isinstance(t, NavigableString) and not re_symbols_ignore.match(t):
                next_siblings.append(t)
            elif isinstance(t, NavigableString) and re_symbols_ignore.match(t):
                next_siblings.append(t)
            elif t.name in ignoring_tags_names and t.attrs == init_tag_attrs:  # also checking the tag attrs
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
                if isinstance(last_tag, NavigableString):
                    next_siblings.pop()
                elif last_tag.name != init_tag_name and last_tag.attrs != init_tag_attrs:
                    next_siblings.pop()
                else:
                    break
            return next_siblings

    # Ignore nested tags names
    if init_tag_name in ['i', 'b', 'em']:
        ignoring_tags_names = ['i', 'b', 'em']
    elif init_tag_name in ['div']:
        # A block tags can have many nested tags
        ignoring_tags_names = ['div', 'p', 'span', 'a']
    else:
        ignoring_tags_names = []

    # Some symbols between same tags can add into them. Because they don't changing of font style.
    if init_tag_name == 'i':
        # Italic doesn't change the style of some characters (spaces, period, comma), so they can be combined
        re_symbols_ignore = re.compile(r'^[\s.,-]+$')
    elif init_tag_name == 'b':
        # Bold changes the style of all characters
        re_symbols_ignore = re.compile(r'^[\s]+$')
    elif init_tag_name == 'div':
        # Here should be careful with merging, because a html can have some `\n` between block tags (like `div`s)
        re_symbols_ignore = re.compile(r'^[\s]+$')
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
                if tag.sourceline > last_tag.sourceline \
                        or (tag.sourceline == last_tag.sourceline and tag.sourcepos > last_tag.sourcepos):
                    break
            if last_tag.sourceline == all_wanted_tags[-1].sourceline and last_tag.sourcepos == last_tag.sourcepos:
                break
            last_tag = tag

        for first_tag, tags_to_append in tag_groups_to_combine:
            combine_tags(first_tag, tags_to_append)

    return soup


if __name__ == '__main__':
    soup = BeautifulSoup('''<div>
        <p>A<i>b</i>cd1, <i>a</i><b><i>b</i></b><i>cd2</i> abcd3 <i>ab</p>
        <p>cd4</i> <i>a</i><i>bcd5</i> <i>ab<span>cd6</span></i></p>
    </div>''', 'html.parser')
    print(soup)

    find_and_combine_tags(soup, 'i')

    print(soup)
