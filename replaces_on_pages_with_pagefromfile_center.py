#!/usr/bin/env python3
import os
import shlex, subprocess
import time
from datetime import datetime
from math import floor
import re
from pathlib import Path
from typing import Optional, Union, Tuple, List
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from dataclasses import dataclass
import html as html_
import mwparserfromhell as mwp


import src.replace as pwb_replace

summary = "<center> [импорт с lib.ru]"
def make_args():
    family = 'wikisource'
    args_base = [f'-site:{family}:ru', '-pt:1']
    args = [
        # '-simulate',
        # '-file:%s' % filename_wikilists,
        # '-begin:"%s"' % marker_page_start, '-end:"%s"' % marker_page_end, '-notitle',
        f'-summary:"{summary}"',
        # '-force',
        '-search:"incategory:Импорт/lib.ru insource:/center>( *==)?/"',
        # '-cat:"Импорт/lib.ru/Указаны тождественнные автор и переводчик"',
        # '-cat:"Импорт/lib.ru"',
        # "-page:'Крошка Доррит (Диккенс; Энгельгардт)/Книга 1'",
        # "-page:'Паж цесаревны (Чарская)'",
        # """-page:'"Религиозные искания" и народ (Блок)'""",
        # '-page:"Аббат Жюль (Мирбо)/ДО"',
        '""', '""',
    ]
    args.extend(args_base)
    args = shlex.split(' '.join(args))
    return args


def run_console_command(command: str):
    # if not os.path.isfile(command):
    #     raise Exception(f'No file "{command}"')
    # os.system(command)
    code = subprocess.Popen(shlex.split(command)).wait()
    # ToDo код завершения всегда == 0, ошибка не проверятся.
    # ToDo Хотя это может это только если сам pwb не закрылся по Exception? Сейчас нет времени на тесты.
    # ToDo Если же == 0 в любом случае, то надо открывать вопрос на Phabricator
    assert code == 0, f'posting subprocess exited with status {code}'


def run_replace_bot_via_console(args):
    python_and_path = 'python %s/pwb.py' % os.getenv('PWBPATH')
    pwb_script = 'replace'
    # if do_post_simulate: args.append('-simulate')  # "-simulate" параметр для тестирования записи pwb
    command = f'%s %s %s' % (python_and_path, pwb_script, ' '.join(args))
    run_console_command(command)


@dataclass
class ReplaceOpt:
    replace_translator: bool
    re_center = re.compile(r"<center>([^\n=]+?)(?<!</center>)(\n+)")
    wordnumbers = r'ПЕРВОЕ|ВТОРОЕ|ТРЕТІЕ|ЧЕТВЕРТОЕ|ПЯТОЕ|ПОСЛѢДНЕЕ'

class ReplaceRobot_mwp(pwb_replace.ReplaceRobot):
    opt: ReplaceOpt

    def center_tag_replaces(self, original_text, applied, page=None):
        text = original_text

        for i in range(20):
            text = self.opt.re_center.sub(r'<center>\1</center>\2<center>', text)

            text = re.sub(r'<br></center>', r'</center>', text)
            text = re.sub(r'</center><br>', r'</center>', text)

        text = re.sub(r'<center></center>', '', text)

        text = re.sub(r'<center>(==+[^\n]+?==+)</center>', r'\1', text)
        text = re.sub(r'<center>(==+[^\n]+?)(?!==+)</center>\n+<center> *(==+) *</center>', r'\1 \2', text)

        text = re.sub(r"(<center>)([^'\n]+?)(''+)(</center>)", r"\1\3\2\4", text)
        text = re.sub(r"(<center>)(''+)([^'\n]+?)(</center>)", r"\1\2\3\2\4", text)

        text = re.sub(r"(?i)<center>[ ']*(ЯВЛЕНІЕ) +([ІVXLI\d]+\.?)[ ']*</center>", r'===== \1 \2 =====', text)
        text = re.sub(r"(?i)<center>[ ']*(СЦЕНА|Глава) +([ІVXLI\d]+(?:-я)?\.?)[ ']*</center>", r'==== \1 \2 ====', text)        
        text = re.sub(r"(?i)<center>[ ']*((?:ДѢЙСТВУЮЩІЕ|[ІVXLI]+|\d{,3})\.?)[ ']*</center>", r'==== \1 ====', text)

        text = re.sub(r"(?i)<center>[ ']*(ПРИМЕЧАНИЯ\.?|ДѢЙСТВІЕ (?:[ІVXLI\d]+|%s)\.?|ДѢЙСТВУЮЩІЯ ЛИЦА[:.]|ПРОЛОГЪ\.?)[ ']*</center>" % self.opt.wordnumbers, r'=== \1 ===', text)
        text = re.sub(r"(?i)<center>[ ']*((?:Часть [ІVXLI\d]+)\.?)[ ']*</center>", r'== \1 ==', text)


        text = re.sub(r'({{right\|.+?)(\n+)({{\*\*\*}})}}', r'\1}}\2\3', text)
        text = re.sub(r'<center>{{\*\*\*}}</center>', r'{{***}}', text)

        text = re.sub(r'<center>(\[\[Файл:.+?px)(.*?\]\])(?:<br>)?</center>', r'\1|center\2', text)
        text = re.sub(r'(\[\[Файл:.+?\|\d+)x\d+(px.*?\]\])', r'\1\2', text)
        text = re.sub(r'(\[\[Файл:.+?)0x01 graphic(.*?\]\])', r'\1\2', text)

        return text



def run():
    # pwb_replace.ReplaceRobot = ReplaceRobot_mwp
    # pwb_replace.ReplaceRobot.apply_replacements = ReplaceRobot_mwp.center_tag_replaces
    pwb_replace.ReplaceRobot.apply_replacements = ReplaceRobot_mwp.reference_notes
    pwb_replace.ReplaceRobot.opt = ReplaceOpt(replace_translator=True)
    args = make_args()
    # run_replace_bot_via_console(args)
    pwb_replace.main(*args)


if __name__ == '__main__':
    run()
