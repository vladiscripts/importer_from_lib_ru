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


def make_args():
    family = 'wikisource'
    args_base = [f'-site:{family}:ru', '-pt:1']
    args = [
        '-simulate',
        # '-file:%s' % filename_wikilists,
        # '-begin:"%s"' % marker_page_start, '-end:"%s"' % marker_page_end, '-notitle',
        '-summary:"regexps {{отексте}}. импорт с lib.ru"',
        # '-force',
        # '-cat:"Импорт/lib.ru/Указаны тождественнные автор и переводчик"',
        "-page:'Тарзан и его звери (Берроуз; Бродерсен)'",
        # "-page:'М(арк) Т(вен) (Горький)'",
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


def tpl_param_fill_or_add(tpl, pname, new_value, replace_value: bool = False, prepend_value: bool = False):
    if new_value:
        new_value = new_value.strip()
        if tpl.has(pname):
            tpl_value = tpl.get(pname).value.strip()
            if tpl_value == '' or replace_value:
                tpl.get(pname).value = f' {new_value}\n'
            elif prepend_value:
                tpl.get(pname).value = f' {new_value} {tpl_value}\n'
        else:
            tpl.add(pname, new_value)
    return tpl

@dataclass
class ReplaceOpt:
    replace_translator:bool


class ReplaceRobot_mwp(pwb_replace.ReplaceRobot):
    replace_opt:ReplaceOpt

    def apply_replacements(self, original_text, applied, page=None):
        replace_translator=self.replace_opt.replace_translator
        remove_text_first_pubished = False
        remove_text_source = False
        text = original_text

        re_text_first_pubished = re.compile(r"^'*(?:Впервые опубликовано|По\sизданию): (.+)[;]?", flags=re.MULTILINE)
        re_text_source = re.compile(r"^'*Источник текста: (.+?)'*$", flags=re.MULTILINE)

        wc = mwp.parse(text)

        # {{Отексте}}
        tpl: mwp.nodes.Template = [t for t in wc.ifilter(forcetype=mwp.nodes.Template) if t.name.matches('отексте')][0]
        tpl_otexte_original = str(tpl)
        for p in tpl.params:
            if p.value.endswith('\n\n'):
                p.value = p.value.removesuffix('\n')
        if tpl.has('ДРУГОЕ'):
            drugoe = tpl.get('ДРУГОЕ').value
            re_ = re.compile(r'[Пп]еревод(?: с \w+)?\s+'
                             r'(?P<translator>\[\[[А-Я][^]]+?]]|[А-Я][^]]+?)'
                             r'\s*,?\s*\(?(?P<translate_date>[\d{4}]|)\)?\.?')
            if translator_m := re_.search(str(drugoe)):
                tpl_param_fill_or_add(tpl, 'ПЕРЕВОДЧИК', translator_m['translator'], replace_value=replace_translator)
                tpl_param_fill_or_add(tpl, 'ДАТАПУБЛИКАЦИИ', translator_m['translate_date'])
                tpl.get('ДРУГОЕ').value = re_.sub('', str(drugoe))

            re_ = re.compile(r'[Тт]екст издания:\s*(?:[Жж]урнал)?\s*(?P<source>.+?)$')
            if source_m := re_.search(str(drugoe)):
                if source_m and source_m != '':
                    tpl_param_fill_or_add(tpl, 'ИСТОЧНИК', source_m['source'])
                    tpl.get('ДРУГОЕ').value = re_.sub('', str(drugoe))
            if source_text_m := re_text_first_pubished.search(text):
                tpl_param_fill_or_add(tpl, 'ДАТАПУБЛИКАЦИИ', source_text_m.group(1), replace_value=True)
                remove_text_first_pubished=True

            if source_text_m := re_text_source.search(text):
                tpl_param_fill_or_add(tpl, 'ИСТОЧНИК', source_text_m.group(1), prepend_value=True)
                remove_text_source=True

        """
        "ДРУГОЕ                = <i>
Перевод [[Софья Александровна Боборыкина|Софьи Боборыкиной]].</i><br>Текст издания: журнал «Русское Богатство», №№ 3-6, 1905."

"""


        # # удалить <span id=""> на которые нет ссылок
        # links_ids = [l.title.lstrip('#') for l in wc.filter_wikilinks() if l.title.startswith('#')]
        # spans = [t for t in wc.filter_tags() if t.tag == 'span'
        #          for a in t.attributes if a.name == 'id' and a.value not in links_ids]
        # for span in spans: wc.remove(span)
        # # for span in spans: wc.remove(span)
        # # out2 = re.sub('<span class="footnote"></span>', '', out2)
        #
        # # <span "class=underline"> → '<u>'. Такие теги делает pandoc.
        # for t in wc.filter_tags():
        #     if t.tag == 'span':
        #         for a in t.attributes:
        #             if a.name == 'class' and a.value == 'underline':
        #                 t.tag = 'u'
        #                 t.attributes.remove(a)

        # strip параметр в {{right|}}
        # for t in wc.filter_templates(matches=lambda x: x.name == 'right'):
        #     t.params[0].value = t.params[0].value.strip()
        # for t in wc.filter_templates(matches=lambda x: x.name == 'right' and x.params[0].value == ''):
        #     wc.remove(t)

        # for t in [t for t in wc.filter_tags(matches=lambda x: x.tag == 'div')]:
        #     t.params[0].value = t.params[0].value.strip()
        #
        # for t in wc.filter_tags(matches=lambda x: x.tag == 'div'):
        #     if t.tag == 'div' and t.get('align').value== 'center':
        #         t.attributes = []

        # g = [t for t in wc.filter_tags(matches=lambda x: x.tag == 'div' and x.get('align').value== 'right')]
        # [t for t in wc.filter_templates(matches=lambda x: x.name == 'right')]

        # mwp.parse('{{right|}}').nodes[0]

        text.replace(tpl_otexte_original, str(tpl))
        if remove_text_first_pubished:
            text = re_text_first_pubished.sub('', text)
        if remove_text_source:
            text = re_text_source.sub('', text)

        # строковые чистки
        re_lines = [r"^\s*'*OCR .+?$", r"^\s*'*Оригинал (?:\[\S )здесь.*?$"]
        for re_line in re_lines:
            re_ = re.compile(re_line, flags=re.MULTILINE)
            text = re_.sub('', text)

        return text


def run():
    # pwb_replace.ReplaceRobot = ReplaceRobot_mwp
    pwb_replace.ReplaceRobot.apply_replacements = ReplaceRobot_mwp.apply_replacements
    pwb_replace.ReplaceRobot.replace_opt = ReplaceOpt(replace_translator=True)
    args = make_args()
    # run_replace_bot_via_console(args)
    pwb_replace.main(*args)


if __name__ == '__main__':
    run()
