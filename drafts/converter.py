#!/usr/bin/env python3
from dataclasses import dataclass
import time
import re
import threading, queue
import pypandoc
import html as html_
import mwparserfromhell as mwp

import db
from converter_html_to_wiki.get_parsed_html import get_html
from converter_html_to_wiki.html2wiki import LibRu
from converter_html_to_wiki.parser_html_to_wiki import *
import make_work_wikipages

# convert_pages_to_db_with_pandoc_on_several_threads()
# wikify_all_into_db()
make_work_wikipages.make_wikipages_to_db()
