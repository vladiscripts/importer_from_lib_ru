# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from itemloaders.processors import *
import re

# MapCompose работает если в паук добавить:
# il = ItemLoader(item=ArticlesItem()); il.add_value('text', text); il.load_item()

re_clear_outer_tag = re.compile(r'^<(\w+)[^>]*>\s*(.*?)\s*</\1>$', flags=re.DOTALL)
re_spacedoubles = re.compile(' {2,}')
re_spaceall = re.compile('\s+', flags=re.DOTALL)


def clean_text(t):
    t = t.replace('­', '')
    t = t.replace('\xa0', '')
    t = re_spaceall.sub(' ', t)
    t = re.sub('</div>\s*<div>', ' ', t, flags=re.DOTALL)
    t = re_spacedoubles.sub(' ', t)
    return t.strip()


def clear_outer_tag(t):
    t = re_clear_outer_tag.sub(r'\2', t)
    return t


def tags2wiki(t):
    t = re.sub('(</?)u>', r'\1i>', t, flags=re.DOTALL)
    # t = re_clear_outer_tag.sub(r'\2', t)
    return t


def dashes(s):
    return re.sub(r'\s(--|-)\s', ' — ', s)


def spaces(s):
    return re.sub(r'\s+', ' ', s)


class UrlsWordlists(Item):
    url = Field()


# url_article = Field()


class ArticlesItem(Item):
    # title = Field()
    text = Field(
        input_processor=MapCompose(clear_outer_tag, clean_text, clear_outer_tag, tags2wiki),
        # output_processor = TakeFirst(),
    )  # lambda s: s.replace('­', ''))


# pagenum = Field()
# filename = Field()
# url_article = Field()
# prim = Field()


class ArticleLoader(ItemLoader):
    title_out = TakeFirst()
    text_in = Join()
    text_out = TakeFirst()
    filename_out = TakeFirst()
    url_article_out = TakeFirst()
    pass


class _WorksItem(Item):
    title = Field()
    text = Field(
        input_processor=MapCompose(clear_outer_tag, clean_text, clear_outer_tag, tags2wiki),
        # output_processor = TakeFirst(),
    )  # lambda s: s.replace('­', ''))
    # pagenum = Field()
    # filename = Field()
    slug = Field()
    categories = Field()  # [[slug, title], ]
    categories_on_author_page = Field()  # [[slug, title], ]
    date = Field()
    annotation = Field()
    author_slug = Field()
    author_name = Field()
    author_name_parsed_for_WS = Field()


class WorksItem(Item):
    title = Field()
    text = Field(
        # input_processor=MapCompose(clear_outer_tag, clean_text, clear_outer_tag, tags2wiki),
        # output_processor = TakeFirst(),
    )  # lambda s: s.replace('­', ''))
    # pagenum = Field()
    # filename = Field()
    slug = Field()
    categories = Field()  # [[slug, title], ]
    categories_on_author_page = Field()  # [[slug, title], ]
    date = Field()
    annotation = Field()
    author_slug = Field()
    author_name = Field()

    author_slug = Field()
    author_id = Field()
    slug = Field()
    title = Field()
    desc = Field()
    oo = Field()  # old orpho
    size = Field()
    year = Field()


class WorksLoader(ItemLoader):
    default_output_processor = TakeFirst()

    title_out = TakeFirst()
    text_in = MapCompose(clear_outer_tag, clean_text, clear_outer_tag, tags2wiki)
    text_out = TakeFirst()
    filename_out = TakeFirst()
    url_article_out = TakeFirst()


class AuthorAboutItem(Item):
    author_slug = Field()
    slug = Field()
    image_url = Field()
    desc = Field()


class AuthorAboutLoader(ItemLoader):
    default_output_processor = TakeFirst()
    desc_in = MapCompose(str.strip, spaces, dashes)
    desc_out = Compose(Join(), lambda s: s.replace(' )', ')').replace(' ,', ',').replace(' ;', ';'))


class AuthorItem(Item):
    slug = Field()
    name = Field()
    name_for_WS = Field()
    family_parsed_for_WS = Field()
    names_parsed_for_WS = Field()
    image_url = Field()
    desc = Field()
    live_time = Field()
    town = Field()
    litarea = Field()

    works = Field()


class AuthorLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, spaces, dashes)
    default_output_processor = TakeFirst()

    # title_out = TakeFirst()
    # text_in = MapCompose(clear_outer_tag, clean_text, clear_outer_tag, tags2wiki)
    # text_out = TakeFirst()
    # filename_out = TakeFirst()
    # url_article_out = TakeFirst()
    # live_time_in = TakeFirst(), lambda x: x.replace('/', '.'), dashes, spaces
    # live_time_out = Compose(TakeFirst(), lambda x: x.replace('/', '.'))
    works_in = Identity()
    works_out = Identity()



class Text(Item):

    tid = Field()
    # slug = Field()
    # categories = Field()
    html = Field()
    content = Field()
    wiki = Field()
    wikified = Field()
