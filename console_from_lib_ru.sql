-- we don't know how to generate root <with-no-name> (class Root) :(
# create table authors
# (
# 	id INTEGER not null auto_increment
# 		primary key,
# 	slug VARCHAR(255) not null
# 		unique,
# 	name VARCHAR(255) not null,
# 	family_parsed_for_WS VARCHAR(255),
# 	names_parsed_for_WS VARCHAR(255),
# 	live_time varchar(255),
# 	town varchar(255),
# 	litarea varchar(255),
# 	image_url varchar(255),
# 	`desc` varchar(255)
# );
#
# create table htmls
# (
# 	id INTEGER not null auto_increment
# 		primary key,
# 	tid INTEGER
# 		unique,
# 	html MEDIUMTEXT,
# 	content MEDIUMTEXT,
# 	constraint htmls_ibfk_1
#         foreign key (tid) references titles (id)
#             on update cascade on delete cascade
# );
#
# create table texts_categories
# (
# 	id INTEGER not null
# 		primary key
# );
#
# create table texts_categories_names
# (
# 	id INTEGER not null auto_increment
# 		primary key
# );
#
# create table titles
# (
# 	id INTEGER not null auto_increment
# 		primary key,
# 	slug VARCHAR(255)	unique,
# 	author_id INTEGER,
# 	year SMALLINT,
# 	size TINYINT,
# 	title VARCHAR(255),
# 	`desc` VARCHAR(255),
# 	oo BOOLEAN,
# 	constraint titles_ibfk_1
#         foreign key (author_id) references authors (id)
#             on update cascade on delete cascade
# );
# create table wiki
# (
# 	id INTEGER not null auto_increment
# 		primary key,
# 	tid INTEGER
# 		unique
# );


# CREATE VIEW all_tables as
# select 'http://az.lib.ru' || authors.slug || '/' || titles.slug as text_url, authors.*, titles.*, htmls.*
# from authors
#         join titles  on authors.id = titles.author_id
#         join htmls  on titles.id = htmls.tid;


create table lib_ru.desc
(
    id             int auto_increment primary key,
    tid            int      not null,
    author         text     null,
    translator     text     null,
    year           text     null,
    `desc`         text     null,
    author_tag     text     null,
    year_tag       text     null,
    annotation_tag text     null,
    constraint tid
        unique (tid),
    constraint desc_titles_id_fk
        foreign key (tid) references lib_ru.titles (id)
            on update cascade on delete cascade
);


CREATE OR REPLACE VIEW all_tables as
select
#        'http://az.lib.ru' || a.slug || '/' || t.slug as text_url,
       t.slug as slug_text,
       t.text_url,
       t.id as tid,
       t.year,
       t.size,
       t.text_length,
       t.title,
       t.title_ws,
       t.desc as text_desc_raw,
       d.desc as text_desc,
       w.desc as text_desc_wikified,
       t.oo,
       t.uploaded as uploaded_text,
       t.do_upload,
       t.is_same_title_in_ws_already,
       a.slug as slug_author,
       a.id as author_id,
       a.name,
       a.family_parsed,
       a.names_parsed,
       a.name_WS,
       a.live_time,
       a.town,
       a.litarea,
       d.translator,
       a.image_url_filename,
       a.image_filename_wiki,
       a.desc as author_desc,
       a.is_author,
       a.uploaded as uploaded_author,
       a.year_dead,
       h.html,
       h.wiki,
       w.text as wikified,
       d.tid as desc_tid,
       d.author_tag,
       ac.name_ws as author_cat,
       ac.text_lang_by_author as lang
#        h.wikified
from authors a
         left join titles t on a.id = t.author_id
         left join htmls h on t.id = h.tid
         left join desc_ d on t.id = d.tid
         left join wikified w on t.id = w.tid
         left join authors_categories ac on a.litarea = ac.name_site;



CREATE VIEW authors_with_cat as
select
       a.slug as slug_author,
       a.id as author_id,
       a.name,
       a.family_parsed,
       a.names_parsed,
       a.name_WS,
       a.live_time,
       a.town,
       a.litarea,
       a.image_url_filename,
       a.image_filename_wiki,
       a.desc as author_desc,
       a.is_author,
       a.uploaded as uploaded_author,
       ac.name_ws as author_cat
#        h.wikified
from authors a
         left join authors_categories ac on a.litarea = ac.name_site;

update authors set year_dead = REGEXP_SUBSTR(live_time, '(?!<\.)([0-9]+$)');

update titles set do_upload = 1;

update titles set title_ws = null;

update titles t
    left join htmls h on t.id = h.tid
set banned = 2
where  -- do_upload = 1
#   and title_ws like '%Библиографическая справка%'
  html like '%: Энциклопедический словарь Брокгауза%'
  and html like '%{{right|\'\'Энциклопедический словарь Брокгауза%'
  and html like '%/brokgauz/index.html \'\'Энциклопедический словарь Брокгауза%'
  and html like '% \'\'Энциклопедический словарь Брокгауза%'
  and html like '%html Энциклопедический словарь Брокгауза%'
  and html like '%Текст издания: Энциклопедический словарь Брокгауза%'
  and html like '%center>\'\'Энциклопедический словарь Брокгауза%'
  and html like '%vehi.net/brokgauz/%'
  and html like '%feb-web.ru/feb/kle%'
  and html like '%feb-web.ru/feb/slt%'
  and html like '%feb-web.ru/feb/litenc%'
  and html like '%Литературная энциклопедия:%'
  and html like '%/ ЭНИ "Словарь псевдонимов%'
;
#                                       author_id=11273

update authors set name_WS = concat_ws(' ', names_parsed, family_parsed);

SELECT  *
FROM   authors
join wikisource_listpages on name_WS = wikisource_listpages.pagename;

UPDATE htmls SET wiki = NULL where wiki like '%[[file:%';

UPDATE desc_ SET translator = NULL where translator = 'Без указания переводчика';

UPDATE htmls
SET wiki           = NULL,
    author         = NULL,
    `year`         = NULL,
    `desc`         = NULL,
    author_tag     = NULL,
    year_tag       = NULL,
    annotation_tag = NULL,
    translator     = NULL;
delete from images;
delete from texts_categories_names;

-- поиск дубликатов в колонке
select tid, urn,
       name_ws,
       COUNT(`name_ws`) AS `count`
from images
group by urn
HAVING `count` > 1;


SET foreign_key_checks = 0;
drop index tid on images;
SET foreign_key_checks = 1;

insert into authors_categories (name_site)
SELECT DISTINCT litarea FROM authors where litarea is not null;



select * from htmls
left join images i on htmls.tid = i.tid
where urn like 'file:%';

UPDATE htmls
left join images i on htmls.tid = i.tid
SET wiki           = NULL
where urn like 'file:%';



SELECT * FROM all_tables as a LEFT JOIN texts_categories as c ON a.tid=c.tid WHERE c.tid IS NULL;

update all_tables set do_upload=0 where desc_tid is null;


CREATE VIEW all_cat_join as
SELECT slug_text,text_url,a.tid as tid,year,size,title,title_ws,text_desc,oo,uploaded_text,do_upload,slug_author,author_id,name,family_parsed,names_parsed,name_WS,live_time,town,litarea,image_url_filename,image_filename_wiki,author_desc,is_author,uploaded_author,html,wiki,desc_tid,author_cat
FROM all_tables as a
         LEFT JOIN texts_categories as c ON a.tid = c.tid
WHERE c.tid IS NULL
#   AND texts_categories.tid IS NOT NULL
  AND a.title IS NOT NULL;
# LIMIT 40 OFFSET 0;



SELECT titles.id as tid,wiki,desc_.desc,wikified.text FROM titles LEFT JOIN htmls ON titles.id=htmls.tid LEFT JOIN wikified ON titles.id=wikified.tid LEFT JOIN desc_ ON titles.id=desc_.tid WHERE titles.do_upload IS TRUE AND htmls.wiki IS NOT NULL AND wikified.text IS NULL LIMIT 20 OFFSET 0;

select title_ws from all_tables where do_upload=0 and uploaded_text=1;


select length(wikified) from all_tables where title_ws = 'На рубеже двух столетий (Белый)';
update all_tables set text_length=length(wikified);
