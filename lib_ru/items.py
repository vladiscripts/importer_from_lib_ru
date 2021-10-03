# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Join, MapCompose
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


class UrlsWordlists(Item):
	url = Field()
	# url_article = Field()


# class ArticleLoader(ItemLoader):
# 	title_out = TakeFirst()
# 	text_in = Join()
# 	text_out = TakeFirst()
# 	filename_out = TakeFirst()
# 	url_article_out = TakeFirst()
# 	pass

class Works(Item):
	title = Field()
	text = Field(
		input_processor=MapCompose(clear_outer_tag, clean_text, clear_outer_tag, tags2wiki),
		# output_processor = TakeFirst(),
	)  # lambda s: s.replace('­', ''))
	# pagenum = Field()
	# filename = Field()
	slug = Field()
	categories = Field()	# [[slug, title], ]
	categories_on_author_page = Field()	# [[slug, title], ]
	date = Field()
	annotation = Field()
	author_slug = Field()
	author_name = Field()



