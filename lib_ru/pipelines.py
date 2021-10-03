# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import requests


class LubkerPipeline(object):
	def process_item(self, item, spider):
		# r = requests.get('http://tools.wmflabs.org/vltools/WDBquery_transcludes_template/', params={'item': 6})
		# r = requests.post('http://tools.wmflabs.org/ruwikisource/text2do/toDOraw.php', data=item)
		# e = r.status_code, r.reason
		# r.encoding = 'utf-8'
		# t = r.text
		return item
