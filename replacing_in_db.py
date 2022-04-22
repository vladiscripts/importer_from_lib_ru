import re
import sqlalchemy

import db_schema as db

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

stmt = sqlalchemy.select(db.Htmls).where(db.Htmls.wiki.regexp_match(r"([^'])''([-—\s.\"]+)''([^'])"))
for r in db.db_.conn.execute(stmt).all():
    text = r['wiki']
    text = re.sub(r"([^'])''([-—\s.\"]+)''([^'])", r'\1\2\3', text)
    rp = db.htmls.update({'id': r['id'], 'wiki': text, 'wiki2_converted': 1}, ['id'])

stmt = sqlalchemy.select(db.Htmls).where(db.Htmls.wiki.like('%{{right%'))
for r in db.db_.conn.execute(stmt).all():
    text = r['wiki']
    text = re.sub(r'([^\n])({{right\|)\s*', r'\1\n\2', text, flags=re.DOTALL)
    rp = db.htmls.update({'id': r['id'], 'wiki': text, 'wiki2_converted': 1}, ['id'])

stmt = sqlalchemy.select(db.Wiki).where(db.Wiki.text.regexp_match(r"([^'])''([-—\s.\"]+)''([^'])"))
for r in db.db_.conn.execute(stmt).all():
    text = r['text']
    text = re.sub(r"([^'])''([-—\s.\"]+)''([^'])", r'\1\2\3', text)
    rp = db.wiki.update({'id': r['id'], 'text': text}, ['id'])

stmt = sqlalchemy.select(db.Wiki).where(db.Wiki.text.like('%{{right%'))
for r in db.db_.conn.execute(stmt).all():
    text = r['text']
    text = re.sub(r'([^\n])({{right\|)\s*', r'\1\n\2', text, flags=re.DOTALL)
    rp = db.wiki.update({'id': r['id'], 'text': text}, ['id'])
