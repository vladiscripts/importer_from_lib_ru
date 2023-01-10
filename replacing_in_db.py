import re
from pathlib import Path
import sqlalchemy as sa

import db.schema as db

text = """
ДОНЦЫ, УРАЛЬЦЫ, КУБАНЦЫ, ТЕРЦЫ</center>

=== Очерки изъ Исторіи стародавняго казацкаго быта въ общедоступномъ изложеніи, для чтенія въ войскахъ, семьѣ и школѣ. ===
<center>2-е изданіе, исправленное и дополненное

СОСТАВИЛЪ<br>
''К. К. Абаза''

''С.-Петербургъ. Колокольная, собствен. домъ, No 14.<br>
''1890.</center>

<center>ОГЛАВЛЕНЕ.</center>

=== Донцы. ===
"""

text_new = re.sub(r"([^'])''([-—.\" ]+)''([^'])", r'\1\2\3', text)
text_new = re.sub(r"^(''+) +", r'\1', text_new, flags=re.MULTILINE)

stmt = sa.select(db.Htmls).where(db.Htmls.wiki.regexp_match(r"([^'])''([-—\s.\"]+)''([^'])"))
for r in db.db_.conn.execute(stmt).all():
    text = r['wiki']
    text = re.sub(r"([^'])''([-—\s.\"]+)''([^'])", r'\1\2\3', text)
    rp = db.htmls.update({'id': r['id'], 'wiki': text, 'wiki_converted': 1}, ['id'])

stmt = sa.select(db.Htmls).where(db.Htmls.wiki.like('%{{right%'))
for r in db.db_.conn.execute(stmt).all():
    text = r['wiki']
    text = re.sub(r'([^\n])({{right\|)\s*', r'\1\n\2', text, flags=re.DOTALL)
    rp = db.htmls.update({'id': r['id'], 'wiki': text, 'wiki_converted': 1}, ['id'])

stmt = sa.select(db.Wiki).where(db.Wiki.text.regexp_match(r"([^'])''([-—\s.\"]+)''([^'])"))
for r in db.db_.conn.execute(stmt).all():
    text = r['text']
    text = re.sub(r"([^'])''([-—\s.\"]+)''([^'])", r'\1\2\3', text)
    rp = db.wiki.update({'id': r['id'], 'text': text}, ['id'])

stmt = sa.select(db.Wiki).where(db.Wiki.text.like('%{{right%'))
for r in db.db_.conn.execute(stmt).all():
    text = r['text']
    text = re.sub(r'([^\n])({{right\|)\s*', r'\1\n\2', text, flags=re.DOTALL)
    rp = db.wiki.update({'id': r['id'], 'text': text}, ['id'])

# переименование страниц: создание списка для pywikibot
pages = []
tt = db.Titles
stmt = sa.select(tt.title_ws_as_uploaded, tt.title_ws_proposed).where(
    tt.title != tt.title_old, tt.uploaded == 1, tt.renamed_manually == 0, tt.title_ws_as_uploaded != tt.title_ws_proposed
) # .limit(10)
res = db.db_.s.execute(stmt).fetchall()
for r in res:
    pages.append(tt.title_ws_as_uploaded)
    pages.append(tt.title_ws_proposed)
txt = '\n'.join(pages)
f = Path('to_rename.lst').write_text(txt, encoding='utf-8')


# переименование страниц в {{отексте}} для pywikibot
pages = []
tt = db.Titles
stmt = sa.select(tt.title_ws_as_uploaded, tt.title_old, tt.title).where(
    tt.title != tt.title_old, tt.uploaded == 1, tt.renamed_manually == 0, # tt.title_ws_as_uploaded_2 != tt.title_ws_proposed
) # .limit(10)
res = db.db_.s.execute(stmt).fetchall()
for r in res:
    s = 'python3 $PWBPATH/pwb.py replace -family:wikisource -lang:ru -page:"%s" ' \
        '"НАЗВАНИЕ              = %s\\n" ' \
        '"НАЗВАНИЕ              = %s\\n"' \
        % (r.title_ws_as_uploaded.replace('"', '\\"'), r.title_old.replace('"', r'\"'), r.title.replace('"', r'\"'))
    pages.append(s)
txt = '\n'.join(pages)
f = Path('to_replace.sh').write_text(txt, encoding='utf-8')