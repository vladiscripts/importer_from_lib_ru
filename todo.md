
# правки текста
* В html искать "�С" как на "Тайна двух океанов (Адамов)/Версия 2". Это был символ градуса Цельсия.

* uploaded=1 and title_ws_as_uploaded != title_ws_as_uploaded_2   - возможные дубли заливки, вроде ".../Версия 6" и ".../Версия 12". сверить страницы на наличие одинаковых ссылок на lib.ru

* удалить лишние images из ВТ
* "----" в текстах меняется на подписи бота. 
    https://ru.wikisource.org/w/index.php?title=%D0%92%D0%BE%D1%81%D0%BF%D0%BE%D0%BC%D0%B8%D0%BD%D0%B0%D0%BD%D0%B8%D1%8F._1._%D0%92_%D1%8E%D0%BD%D1%8B%D0%B5_%D0%B3%D0%BE%D0%B4%D1%8B_(%D0%92%D0%B5%D1%80%D0%B5%D1%81%D0%B0%D0%B5%D0%B2)&diff=prev&oldid=4398946

* Символы с битыми кодами, '\x07'. Потом категоризовать страницы в html которых есть `re.findall(r"[\x00-\x19]", text)). 
    https://ru.wikisource.org/w/index.php?title=О_состоянии_Российского_флота_(Головнин)&diff=prev&oldid=4398988

* Удалены данные из таблиц. Возможно причиной <h> теги в ячейках таблиц, или кривые таблицы. УточнитьЮ, категоризировать, возможно написать багрепорт на pandoc,  в прошлой версии 2.5 такого небыло. 
    https://ru.wikisource.org/w/index.php?title=%D0%92%D0%BE%D1%81%D1%82%D0%BE%D1%87%D0%BD%D0%B0%D1%8F_%D0%B2%D0%BE%D0%B9%D0%BD%D0%B0_1853-1856_%D0%B3%D0%BE%D0%B4%D0%BE%D0%B2._%D0%A2%D0%BE%D0%BC_%D0%B2%D1%82%D0%BE%D1%80%D0%BE%D0%B9_(%D0%91%D0%BE%D0%B3%D0%B4%D0%B0%D0%BD%D0%BE%D0%B2%D0%B8%D1%87)&diff=prev&oldid=4399004
    https://ru.wikisource.org/w/index.php?title=%D0%92%D0%BE%D1%81%D1%82%D0%BE%D1%87%D0%BD%D0%B0%D1%8F_%D0%B2%D0%BE%D0%B9%D0%BD%D0%B0_1853-1856_%D0%B3%D0%BE%D0%B4%D0%BE%D0%B2._%D0%A2%D0%BE%D0%BC_%D0%BF%D0%B5%D1%80%D0%B2%D1%8B%D0%B9_(%D0%91%D0%BE%D0%B3%D0%B4%D0%B0%D0%BD%D0%BE%D0%B2%D0%B8%D1%87)&diff=prev&oldid=4399003

* Заменить ('[А-Яа-я][23;]+[а-я]', 'ѣѢ').
* ... ` --\n` на ` —\n`

* Переименовать страницы с нераспознанными названиями. Улучшить парсер поиска названий, переименовать с установкой флага в БД, переименовать в ВТ.
    https://ru.wikisource.org/wiki/New_(%D0%A1%D0%B5%D0%B3%D1%8E%D1%80)
* Переименовать кавычки в названиях страниц
* Исправить загловки "== ==" содержащие `<br>\n`. `r'\n==+([^\n]<br />)\n([^\n] ==+)\n'`
    https://ru.wikisource.org/wiki/%D0%A2%D1%80%D0%B8_%D1%87%D0%B5%D0%BB%D0%BE%D0%B1%D0%B8%D1%82%D0%BD%D1%8B%D1%85_%D0%A1%D0%BF%D1%80%D0%B0%D0%B2%D1%89%D0%B8%D0%BA%D0%B0_%D0%A1%D0%B0%D0%B2%D0%B2%D0%B0%D1%82%D0%B8%D1%8F,_%D0%A1%D0%B0%D0%B2%D0%B2%D1%8B_%D0%A0%D0%BE%D0%BC%D0%B0%D0%BD%D0%BE%D0%B2%D0%B0_%D0%B8_%D0%BC%D0%BE%D0%BD%D0%B0%D1%85%D0%BE%D0%B2_%D0%A1%D0%BE%D0%BB%D0%BE%D0%B2%D0%B5%D1%86%D0%BA%D0%BE%D0%B3%D0%BE_%D0%BC%D0%BE%D0%BD%D0%B0%D1%81%D1%82%D1%8B%D1%80%D1%8F_(%D0%A1%D0%B0%D0%B2%D0%B2%D0%B0%D1%82%D0%B8%D0%B9)/%D0%94%D0%9E
* `python3 $PWBPATH/pwb.py replace -family:wikisource -lang:ru -cat:"Импорт/lib.ru" -regex '\u0301ѣ' 'ѣ'  '\u0301Ѣ' 'Ѣ'`