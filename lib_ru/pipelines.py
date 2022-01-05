# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import requests

import db
from lib_ru.items import AuthorItem, WorksItem, Text


class LibRuPipeline(object):
    cache_works = {}
    cache_text_categories = {}

    def process_item(self, item, spider):
        # r = requests.get('http://tools.wmflabs.org/vltools/WDBquery_transcludes_template/', params={'item': 6})
        # r = requests.post('http://tools.wmflabs.org/ruwikisource/text2do/toDOraw.php', data=item)
        # e = r.status_code, r.reason
        # r.encoding = 'utf-8'
        # t = r.text

        # db.authors = spider.crawler.db_authors
        # db.htmls = spider.crawler.db_htmls
        # db.titles = spider.crawler.db_titles
        # db.texts_categories = spider.crawler.db_texts_categories
        # db_texts_categories_names = spider.crawler.texts_categories_names

        if isinstance(item, Text):  # if 'html_' in item:
            pass
            # with spider.crawler.db_lock:
            #
            #     db.titles.update({'slug': item['slug'], 'html': item['html']}, ['slug'], ensure=True)
            #     t = db.titles.find_one(slug=item['slug'])
            #
            #     for slug, name in item['categories']:
            #         if not name in self.cache_text_categories:
            #             db.texts_categories_names.insert({'slug': slug, 'name': name}, ensure=True)
            #             c = db.texts_categories_names.find_one(name=name)
            #             self.cache_text_categories[name] = dict(c)
            #         try:
            #             c = self.cache_text_categories[name]
            #         except:
            #             print()
            #
            #         try:
            #             db.texts_categories.insert({'cat_id': c['id'], 'text_id': t['id']}, ensure=True)
            #         except:
            #             print()

        elif isinstance(item, AuthorItem):  # elif 'name' in item:
            with spider.crawler.db_lock:
                a = db.authors.find_one(slug=item['slug'])

                if not a:
                    works = item['works']
                    del item['works']
                    db.authors.upsert(item, ['slug'], ensure=True)

                    a = authors.find_one(slug=item['slug'])
                    for w in works:  w.update({'author_id': a['id']})
                    db.titles.insert_many(works, ensure=True)

        elif isinstance(item, WorksItem):  # elif 'author_slug' in item:
            with spider.crawler.db_lock:
                # for parse_about_page()
                item['slug'] = item['author_slug']
                del item['author_slug']
                db.authors.upsert(item, ['slug'], ensure=True)

        return item
