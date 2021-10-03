#!/usr/bin/env python3
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from lib_ru.items import ScrapyItem as Item
from scrapy.loader.processors import Join, MapCompose, Identity
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
import dataset
from threading import RLock

uniques_p = set()
uniques_e = set()


class ScrapySpider(CrawlSpider):
    name = "getfiles"
    # allowed_domains = ['tolstoy.ru']
    start_urls = [
        'http://tolstoy.ru/creativity/90-volume-collection-of-the-works/',
    ]
    rules = (
        Rule(LinkExtractor(allow=r'90-volume-collection-of-the-works/\d+/', restrict_css="section.content",
                           # deny='#map', restrict_xpaths="//section[@id='paginadoListado']
                           ), callback='parse_item'),
        Rule(LinkExtractor(restrict_css="div.pagination", unique=False),
             process_links='filter_url_dubles_cat', follow=True),
        Rule(LinkExtractor(restrict_css='a[data-name="ViewLink"]'),
             process_links='filter_url_dubles_e', callback='parse_item'),
    )


def filter_url_dubles_e(self, items):
    items_new = []
    for l in items:
        eid = l.url.split('/')[-1]
        if not eid in uniques_e:
            uniques_e.add(eid)
            if not self.db_table.find_one(url_id=eid):
                items_new.append(l)
    return items_new


def filter_url_dubles_cat(self, items):
    items_new = []
    for l in items:
        p = int(parse_qs(l.url)['p'][0])
        if not p in uniques_p:
            uniques_p.add(p)
            items_new.append(l)
    return items_new

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     spider.db = dataset.connect('sqlite:///db.sqlite', engine_kwargs=dict(echo=False))
    #     spider.db_input = spider.db['open_pubs']
    #     spider.db_table = spider.db['signalchecker']
    #     spider.db_lock = RLock()

    # def start_requests(self):
    # 	urls = ['',]
    # 	for url in urls:
    # 		yield scrapy.Request(url, callback=self.parse)


def start_requests(self):
    db_numbers = [(r['id'], r['postcode']) for r in self.db_input.find()]
    # db_downloaded_numbers = [int(r['pid']) for r in self.db_table.find()]
    # numbers = [(pid, code.replace(' ', '')) for pid, code in db_numbers if pid not in db_downloaded_numbers]
    numbers = db_numbers
    # for pid, code in [['4623', 'NR17 1AY']]:
    for pid, code in numbers:
        # if n != '01543721': continue
        yield scrapy.Request(f'{self.url}?postcode={code}', callback=self.parse_item,
                             # errback=self.err,
                             meta={'pid': pid, 'code': code,
                                   # 'proxy': 'http://127.0.0.1:8118'
                                   },
                             dont_filter=True)

    def parse_item(self, response):
        i = Item()

        i['url'] = response.url
        content = response.css('section.content')
        i['vol_label'] = content.css('h1 ::text').extract_first()
        i['vol_title'] = ' / '.join(content.css('p.tit ::text').extract())
        urlfiles_list = [urljoin(response.url, url) for url in content.css('ul.download_c2 a::attr(href)').extract()]
        for link in urlfiles_list:
            i['pdf'] = link if 'pdf' in link else ''
            i['epub'] = link if 'epub' in link else ''
            i['fb2'] = link if 'fb2' in link else ''
            i['mobi'] = link if 'mobi' in link else ''

    # yield scrapy.Request(self.url_base + url_recent_report, meta={'item': i}, callback=self.parse_report_page)

    i = strip_strs(i)

    with spider.db_lock:
        spider.db_table.insert_ignore(item, ['pid', 'provider'], ensure=True, keys=['pid'])

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

class AncienthomeItem(PortiaItem):
    text = scrapy.Field(
        input_processor=Identity(),
        output_processor=Join(),
    )
    title = scrapy.Field(
        input_processor=Text(),
        output_processor=Join(),
    )
    dict_label = scrapy.Field(
        input_processor=Text(),
        output_processor=Join(),
    )


def strip_strs(item: Union[scrapy.Item, dict]):
    """Strip all strings in dict/item. Usage: i = strip_strs(i) """
    for k, v in item.items():
        if isinstance(v, str):
            v = v.strip()
            v = re.sub(r'  +', ' ', v)
            if v == '': v = None
            item[k] = v
    return item
