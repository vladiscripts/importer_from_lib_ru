from typing import Union, List
import re

re_spaces = re.compile(r'\s*')


def wikify(text: str):
    text = text.replace('&#1122;', 'Ѣ').replace('&#1123;', 'ѣ').replace('&amp;#1122;', 'Ѣ').replace('&amp;#1123;', 'ѣ')

    text = re.sub(r'^(--|-)\s*(?!\w)', '— ', text)
    return text


if __name__ == '__main__':
    print(soup)

    # wc = mwp.parse(r.text)
    # tags = wc.filter_tags()
    # j = tags[68].contents.nodes.get()
    # t = []
    # for n in tags[68].contents.nodes:
    #     # print(n)
    #     if isinstance(n, mwp.nodes.Tag):
    #         for n1 in n:
    #             print(n1)
    #             t.append(n1.value)
    #         continue
    #     t.append(n.value)
