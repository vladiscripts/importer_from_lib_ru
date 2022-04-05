#!/usr/bin/env python3
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.sql import or_
from dataclasses import dataclass, InitVar, asdict
import db_schema as db


@dataclass
class TitleRow:
    id: int
    title: InitVar[str]
    family_parsed: InitVar[str]
    oo: bool
    title_ws_proposed: str = None
    is_already_this_title_in_ws: bool = False


    def __post_init__(self, title, family_parsed):
        """ set title_ws """
        def make_title_oo(title:str, oo:bool):
            return f'{title}/ДО' if oo else title

        t = db.titles
        tw = db.wikisource_listpages

        title_base = f'{title} ({family_parsed})'
        title_proposed = make_title_oo(title_base, self.oo)
        i = 1
        while True:
            is_already_this_title_in_ws = bool(tw.find_one(pagename=title_proposed))
            if is_already_this_title_in_ws or t.find_one(title_ws_proposed=title_proposed):
                i += 1
                title_base = f"{title} ({family_parsed})/Версия {i}"
                title_proposed = make_title_oo(title_base, self.oo)
            else:
                break

        self.is_already_this_title_in_ws = is_already_this_title_in_ws
        self.title_ws_proposed = title_proposed


def db_set_titles_ws():
    t = db.titles
    col = t.table.c
    t.update({'title_ws_proposed': None}, [])  # сбросить все перед запуском

    # stmt = sa.select(db._titles).select_from(sqlalchemy.join(db._titles, db._wiki, db._titles.c.id==db._wiki.c.tid)).limit(1)
    for ra in db.authors.find():  # is_author=True
        # print(ra['id'])
        rows_titles = t.find(col.title.is_not(None), author_id=ra['id']
                             # , updated_as_named_proposed=0
                             )  # cols.title_ws.is_(None),  ,id=87481
        for rt in rows_titles:
            db_row = TitleRow(id=rt['id'], title=rt['title'], family_parsed=ra['family_parsed'], oo=rt['oo'])
            print(f"{db_row.id=} {rt['title']=} {db_row.title_ws_proposed=}")
            t.update({
                'id': db_row.id,
                'is_already_this_title_in_ws': db_row.is_already_this_title_in_ws,
                'title_ws_proposed': db_row.title_ws_proposed}, ['id'])


if __name__ == '__main__':
    db_set_titles_ws()
