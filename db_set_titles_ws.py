#!/usr/bin/env python3
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
    title_ws_guess: str = None
    title_ws: str = None
    is_same_title_in_ws_already: bool = False


    def __post_init__(self, title, family_parsed):
        """ set title_ws """
        t = db.titles
        tw = db.wikisource_listpages

        title_ws = f'{title} ({family_parsed})'
        self.title_ws_guess = title_ws if not self.oo else f'{title_ws}/ДО'
        i = 1
        while True:
            has_title_ws_already = bool(tw.find_one(pagename=title_ws))
            if has_title_ws_already or t.find_one(title_ws=title_ws):
                i += 1
                title_ws = f"{title} ({family_parsed})/Версия {i}"
                self.is_same_title_in_ws_already = has_title_ws_already
            else:
                break

        self.title_ws = title_ws


def db_set_titles_ws():
    t = db.titles
    col = t.table.c
    t.update({'title_ws': None}, [])  # сбросить все перед запуском

    for ra in db.authors.find():  # is_author=True
        # print(ra['id'])
        rows_titles = t.find(col.title.is_not(None), author_id=ra['id'])  # cols.title_ws.is_(None),  ,id=87481
        for rt in rows_titles:
            db_row = TitleRow(id=rt['id'], title=rt['title'], family_parsed=ra['family_parsed'], oo=rt['oo'])
            print(f"{db_row.id=} {rt['title']=} {db_row.title_ws=} {db_row.title_ws_guess=}")
            t.update({
                'id': db_row.id,
                'is_same_title_in_ws_already': db_row.is_same_title_in_ws_already,
                'title_ws': db_row.title_ws,
                'title_ws_guess': db_row.title_ws_guess}, ['id'])


if __name__ == '__main__':
    db_set_titles_ws()
