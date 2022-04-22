#!/usr/bin/env python3

from pathlib import Path
from sqlalchemy.orm import aliased

import db_schema as db


def make_filename(url) -> str:
    p = Path(url)
    try:
        p.parts[-2]
    except IndexError as e:
        raise e
    name_ws = f'{p.parts[-3]}_{p.parts[-2]}_{p.name}'
    return name_ws


offset = 0
limit = 300
# select * from images as i1 join images as i2 on i1.name_ws = i2.name_ws where i1.urn != i2.urn

# images_doubles
# i1 = aliased(db.Images)
# i2 = aliased(db.Images)
# stmt = db.db_.s.query(i1).join(i2, i1.name_ws == i2.name_ws).filter(i1.tid != i2.tid)
stmt = db.db_.s.query(db.Images).filter(db.Images.downloaded == 0)
res = stmt.all()
for r in res:
    r.downloaded = 0
    r.uploaded = 0
    r.name_ws = make_filename(r.urn)
db.db_.s.commit()
