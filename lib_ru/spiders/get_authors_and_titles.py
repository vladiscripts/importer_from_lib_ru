#!/usr/bin/env python3
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from itemloaders.processors import TakeFirst, MapCompose, Join, Compose
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from lib_ru.items import *
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import requests
import json
import time
import vladi_helpers
import requests
# from vladi_helpers import vladi_helpers
from vladi_helpers.vladi_helpers import url_params_str_to_dict, url_params_str_to_list, \
    cookies_string_from_Chrome_to_list
from vladi_helpers.file_helpers import json_save_to_file, json_load_from_file, file_savetext, file_readtext
from vladi_helpers.vladi_helpers import url_params_str_to_dict, url_params_str_to_list
import sqlite3
from lxml.html import fromstring  # import html5lib
import parsel
from w3lib.html import remove_tags
import pandas as pd
import os
from pathlib import Path
from typing import Union
import re
from bs4 import BeautifulSoup, Comment
import mwparserfromhell as mwp

from db import *

def selector_from_html5(response):
    response = response.replace(encoding='utf-8',
                                # body=str(BeautifulSoup(response.body, 'html.parser', from_encoding='cp1251')))
                                body=str(BeautifulSoup(response.body, 'html5lib', from_encoding='cp1251')))
    return response


class AuthorsSpider(CrawlSpider):
    name = "get_authors_and_titles"
    allow_domains = ['az.lib.ru']
    start_urls = [
        # 'http://az.lib.ru/a/',
        # 'http://az.lib.ru/a/ashhacawa_s_m/',
        # 'http://az.lib.ru/h/hartulari_k_f/',
        # 'http://az.lib.ru/l/litoshenko_l_n/',
        # 'http://az.lib.ru/m/munshtejn_l_g/',
        # 'http://az.lib.ru/c/chukowskij_k_i',
        'http://az.lib.ru/c/chukowskij_k_i/text_1913_poezia_budushego.shtml',
    ]

    rules = (
        Rule(LinkExtractor(restrict_xpaths='//a[.="А"]/following-sibling::a|//a[.="Я"]/preceding-sibling::a',
                           unique=False), follow=True),
        Rule(LinkExtractor(restrict_xpaths='//dl/a|a[.="Связаться с программистом сайта"]/preceding::a'),
             callback='parse_item'),
    )

    def parse_start_url(self, response):
        """ to testing of scraping of a page """
        filepath = 'text.html'
        Path(filepath).write_text(response.text, encoding=response.encoding)
        html = Path(filepath).read_text(encoding=response.encoding)

        from scrapy.http import HtmlResponse
        r = HtmlResponse(url="my HTML string", body=html, encoding=response.encoding)
        r = selector_from_html5(response)

        yield item

    def parse_item(self, response):
        response = selector_from_html5(response)
        l = AuthorLoader(AuthorItem(), response)

        author_slug = urlsplit(response.url).path.rstrip('/')
        l.add_value('slug', author_slug)

        # l.add_css('author_name', 'h2::text', re=r':\s*(.*?)\s*:')
        author_name = response.css('h2::text').re_first(r':\s*(.*?)\s*:')
        # todo: в author_name и БД по паттерну r':\s*(.*?)\s*:' попали категории вроде "американская литература"
        #  http://az.lib.ru/a/amerikanskaja_literatura/
        #  на этих страницах также есть тексты анонимов http://az.lib.ru/a/amerikanskaja_literatura/text_1874_zhenskaya_voyna_oldorfo.shtml
        #  но не все, например, здесь есть автор http://az.lib.ru/f/francuzskij_epos/text_1968_pesn_o_rolande.shtml
        #  но в БД он не попал, поскольку использована insert_ignore по unique title_slug
        #  Надо перезагрузить тексты. Данные об авторе брать из шапок текстов, а не со страницы авторов.
        l.add_value('name', author_name)
        family, _, names = author_name.partition(' ')
        # l.add_value('name_parsed_for_WS', f'{names} {family}')
        l.add_value('family_parsed_for_WS', family)
        l.add_value('names_parsed_for_WS', names)

        # import w3lib.html
        # w3lib.html.remove_tags('a')
        # l.add_xpath('names_parsed_for_WS', '//li//*[contains(.,"Даты жизни:")]/ancestor::li', lambda x: w3lib.html.remove_tags(x, which_ones=('b')))
        # table_desc = l.nested_xpath('//li//*[contains(.,"Даты жизни:")]/ancestor::li')
        # table_desc = l.nested_xpath('//dd/table')
        table_desc = l.nested_xpath('//table//li/..')
        # table_desc = table_desc.nested_xpath('//li//*[contains(.,"Даты жизни:")]/ancestor::td')
        # table_desc.add_css('live_time', 'li ::text', MapCompose(lambda x: re.sub(r'\s', '', x), dashes))
        # h =table_desc.selector.xpath('li[contains(.,"Даты жизни:")]//following-sibling::text()').get()
        # h = table_desc.selector.xpath(x % 'Где жил(а):').get()
        x = 'li[contains(.,"%s")]//following-sibling::text()'

        # table_desc.add_xpath('live_time', x % 'Даты жизни:', TakeFirst(), lambda x: x.replace('/', '.'), dashes, spaces)
        table_desc.add_xpath('live_time', x % 'Даты жизни:', TakeFirst(), lambda x: x.replace('/', '.'))
        table_desc.add_xpath('town', x % 'Где жил(а):', TakeFirst(),
                             lambda x: '; '.join(s.strip(' ,') for s in x.strip().split(';') if s != ''))
        table_desc.add_xpath('litarea', 'li[contains(.,"%s")]//a/text()' % 'Принадлежность:')

        # todo

        # works = l.nested_xpath('//dt/li/a[starts-with(@href, "text_")]/ancestor::li')
        # works = response.css('h2::text').re_first(r':\s*(.*?)\s*:')
        # works = [dict(
        #     slug=w.css('a ::attr(href)').get(),  # 'a[href^="text_"] ::attr(href)'
        #     title=w.css('a ::text').get(),
        #     desc=w.css('dd ::text').get(),
        #     oo=bool(w.xpath('b[contains(.,"Ѣ")]').get()),
        #     size=w.css('b ::text').re_first(r'(\d)k'),
        #     year=w.css('small ::text').re_first(r'\[(.*?)\]')
        # ) for w in response.xpath('//dt/li/a[starts-with(@href, "text_")]/ancestor::li')]

        # yield from self.get_texts_metadata(response, author_slug)
        l.add_value('works', self.get_texts_metadata(response))

        # work.selector[0].css('h2::text').re_first(r':\s*(.*?)\s*:')
        # works.selector[0].css('a ::text').get()
        # l.add_css('slug', 'a ::attr(href)')  # 'a[href^="text_"] ::attr(href)'
        # l.add_css('title', 'a ::text')
        # l.add_css('desc', 'dd ::text')
        # l.add_xpath('oo', 'b[contains(.,"Ѣ")]', bool)
        # l.add_css('size', 'b ::text', TakeFirst(), re=r'\dk')
        # l.add_css('year', 'small ::text', TakeFirst(), re=r'\[(.*?)\]')
        # l.add_value('works', [d[0].__dict__ for d in works])
        # l.add_value('works', tuple(d.__dict__ for d in works))
        # l.add_value('works', works)

        yield l.load_item()

        yield response.follow(response.url + 'about.shtml', callback=self.parse_about_page,
                              cb_kwargs={'author_slug': author_slug})

    def get_texts_metadata(self, response):
        t = [dict(
            slug=w.css('a ::attr(href)').get(),  # 'a[href^="text_"] ::attr(href)'
            title=w.css('a ::text').get(),
            desc=''.join(w.css('dd').getall()),
            oo=bool(w.xpath('b[contains(.,"Ѣ")]').get()),
            size=w.css('b ::text').re_first(r'(\d+)k'),
            year=w.css('small ::text').re_first(r'\[(.*?)\]')
        ) for w in response.xpath('//dt/li/a[starts-with(@href, "text_")]/ancestor::li')]
        return t

    def parse_about_page(self, response, author_slug):
        response = selector_from_html5(response)
        g = AuthorAboutLoader(AuthorAboutItem(), response)
        g.add_value('author_slug', author_slug)
        main_block = g.nested_xpath('//noindex[1]')
        main_block.add_css('image_url',
                           'img ::attr(src)')  # todo: записываются в базу сторонние ссылки, проверить на /a/adamowich_j_a
        main_block.add_css('desc', 'dd', re=r'^(?:\s|--)*(.*?)\s*$')
        yield g.load_item()

    # def parse_json(self, response):
    #	i = Item()
    #	j = json.loads(response.text)
    #	u = response.url
    # i['id'] = place.get('id')

    # сбор атрибутов с html - названий полей
    # response.css('div.clinic div').css('::attr(class)').extract()

    # создание списка полей
    # keys = j[0]['Physicians'][0].keys()
    # for k in keys:
    #     print(f'{k} = Field()')
    # for k in keys:
    #     print(f"i['{k}'] = d['{k}']")


# class AncienthomeItem(PortiaItem):
#     text = scrapy.Field(
#         input_processor=Identity(),
#         output_processor=Join(),
#     )
#     title = scrapy.Field(
#         input_processor=Text(),
#         output_processor=Join(),
#     )
#     dict_label = scrapy.Field(
#         input_processor=Text(),
#         output_processor=Join(),
#     )


def strip_strs(item: Union[scrapy.Item, dict]):
    """Strip all strings in dict/item. Usage: i = strip_strs(i) """
    for k, v in item.items():
        if isinstance(v, str):
            v = v.strip()
            v = re.sub(r'  +', ' ', v)
            if v == '': v = None
            item[k] = v
    return item
