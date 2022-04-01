#!/usr/bin/env python3
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from scrapy.loader.processors import Join, MapCompose, Identity
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urljoin, urlencode, quote_plus
import requests
import json
import time
# import sqlite3
# from lxml.html import fromstring  # import html5lib
# import parsel
# from w3lib.html import remove_tags
# import pandas as pd
import os
from pathlib import Path
from typing import Union
import re
import dataset
from threading import RLock
from bs4 import BeautifulSoup, Comment

import db
from lib_ru.items import WorksItem as Item, Text


def selector_from_html5(response):
    response = response.replace(
        encoding='utf-8',
        # body=str(BeautifulSoup(response.body, 'html.parser', from_encoding='cp1251')))
        body=str(BeautifulSoup(response.body, 'html5lib', from_encoding='cp1251')))
    return response


class TextImagesSpider(CrawlSpider):
    name = "get_all_texts_images"
    # allow_domains = ['az.lib.ru']
    start_urls = [
        # 'http://az.lib.ru/a/',
        # 'http://az.lib.ru/a/ashhacawa_s_m/',
        'http://az.lib.ru/d/defo_d/text_0100oldorfo.shtml',
    ]

    # rules = (
    #     # Rule(LinkExtractor(allow=r'/text_', restrict_css="li a", allow_domains='az.lib.ru'), callback='parse_item'),
    #     Rule(LinkExtractor(restrict_xpaths='//a[.="А"]/following-sibling::a|//a[.="Я"]/preceding-sibling::a',
    #                        unique=False), follow=True, callback='parse_item'),
    #     Rule(LinkExtractor(restrict_xpaths='//dl/a|a[.="Связаться с программистом сайта"]/preceding::a'), follow=True),
    # )

    def start_requests(self):
        # t = {'url': 'http://az.lib.ru/d/dikkens_c/text_0110oldorfo.shtml', 'id': 29468}
        # yield scrapy.Request(t['url'], callback=self.save_html_to_db, cb_kwargs={'tid': t['id']})

        for r in db.all_tables.all():
            # col = db.all_tables.table.c.html
            # for t in db.all_tables.find(col.is_(None), author_id=a['id']):
            yield scrapy.Request(r['text_url'], callback=self.save_html_to_db,
                                 cb_kwargs={'tid': t['tid'], 'tid': t['tid']})

        for r in db.all_tables.all():
            # col = db.all_tables.table.c.html
            # for t in db.all_tables.find(col.is_(None), author_id=a['id']):
            yield scrapy.Request(r['text_url'], callback=self.save_html_to_db,
                                 cb_kwargs={'tid': t['tid'], 'tid': t['tid']})

    # def parse_start_url(self, response):
    def parse_item(self, response, slug):

        # filepath = 'text.html'
        # Path(filepath).write_text(response.text, encoding=response.encoding)
        # html = Path(filepath).read_text(encoding=response.encoding)
        # from scrapy.http import HtmlResponse
        # r = HtmlResponse(url="my HTML string", body=html, encoding=response.encoding)

        r = selector_from_html5(response)

        # soup = BeautifulSoup(html, from_encoding='cp1251')
        # desc = soup.select('table li i')[0].text

        desc_block = r.xpath(
            '//table//comment()[contains(.,"Блок описания произведения (слева вверху")]/ancestor::table//li')
        categories_ = desc_block.xpath('.//a[starts-with(@href,"/type/")]/ancestor::li//a').css('a[href^="/"]')
        categories = [(z.css('::attr(href)').get(), z.css('::text').get()) for z in categories_]

        content = r.xpath('//noindex//comment()[contains(.,"Собственно произведение")]/parent::noindex').get()

        i = Text()

        slug = 'l'
        i['slug'] = slug
        # i['categories'] = categories
        # i['categories'] = []
        # i['html'] = content
        # i['html'] = response.text

        db.htmls.update({'slug': slug, 'html': response.text}, ['slug'], ensure=True)

        yield i

    # def parse_start_url(self, response):
    def save_html_to_db(self, response, tid):

        # filepath = 'text.html'
        # Path(filepath).write_text(response.text, encoding=response.encoding)
        # html = Path(filepath).read_text(encoding=response.encoding)
        # from scrapy.http import HtmlResponse
        # r = HtmlResponse(url="my HTML string", body=html, encoding=response.encoding)

        # r = selector_from_html5(response)

        # soup = BeautifulSoup(html, from_encoding='cp1251')
        # desc = soup.select('table li i')[0].text

        # desc_block = r.xpath(
        #     '//table//comment()[contains(.,"Блок описания произведения (слева вверху")]/ancestor::table//li')
        # categories_ = desc_block.xpath('.//a[starts-with(@href,"/type/")]/ancestor::li//a').css('a[href^="/"]')
        # categories = [(z.css('::attr(href)').get(), z.css('::text').get()) for z in categories_]
        #
        # content = r.xpath('//noindex//comment()[contains(.,"Собственно произведение")]/parent::noindex').get()

        i = Text()

        i['tid'] = tid
        # i['slug'] = t_slug
        # i['categories'] = categories
        # i['categories'] = []
        # i['html'] = content
        # i['html'] = response.text

        db.htmls.upsert({'tid': tid, 'html': response.text}, ['tid'])
        # db.htmls.insert({'tid': tid, 'html': response.text}, ensure=True)

        yield i

    def parse_report_page(self, r):
        i = r.meta.get('item')
        yield i

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
