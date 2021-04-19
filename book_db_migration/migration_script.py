#!/usr/bin/env python3
from time import time
import datetime
start = time()
value = datetime.datetime.fromtimestamp(start)
print(value.strftime('%Y-%m-%d %H:%M:%S'))

import mysql.connector
from mysql.connector import (connection)
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import pymysql

from book_db_migration.migration_functions import *

# The id number for English in the languages table
ENGLISH = 17

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="isfdb",
)
cur = conn.cursor()

alchemyEngine = create_engine('mysql+pymysql://root:@127.0.0.1/isfdb')
alch_conn = alchemyEngine.connect()

psg_conn = psycopg2.connect("user=postgres")
psg_conn.autocommit = True
psg_cur = psg_conn.cursor()

success = attempt_delete_rec_system_db(psg_cur)
if not success:
    raise SystemError
psg_cur.close()
psg_conn.close()

psg_conn = psycopg2.connect(db_conn_string)
psg_cur = psg_conn.cursor()
prepare_books_tables(psg_cur)


# Get all the novels, novellas, collections, and omnibuses 
# where an English version exists, 
# and which aren't non-genre, graphic novels, or by an excluded author
print("main title table query...")
cur.execute("""
    SELECT title_id, title_title, title_synopsis, note_id, series_id, 
        title_seriesnum, YEAR(title_copyright) as year, title_ttype, title_parent, 
        title_rating, title_seriesnum_2, title_jvn
    FROM titles  
    WHERE ( 
        (title_ttype = 'SHORTFICTION' AND title_storylen = 'novella') 
        OR title_ttype IN ('ANTHOLOGY', 'COLLECTION', 'NOVEL', 'OMNIBUS')
    ) 
    AND title_language = %s
    AND title_non_genre != 'Yes' 
    AND title_graphic != 'Yes'
    AND title_id NOT IN (
        SELECT title_id 
        FROM  canonical_author 
        WHERE author_id in (4853, 2857, 319407)
    )
    LIMIT 1000
    ; """, (ENGLISH,))
titles = cur.fetchall()
books = []


print("main title loop...")
i = 0
from math import ceil
two_percent_increment = ceil(len(titles) / 50)
print("hash mark for each {} titles processed".format(two_percent_increment))
print("1%[" + "    ."*10 + "]100%")
print("  [", end='', flush=True)

debug_id = 10
# for title_id, title, synopsis_id, note_id, series_id, seriesnum, \
#     year, ttype, parent_id, rating, seriesnum_2 in [titles[debug_id - 1]]:
for title_id, title, synopsis_id, note_id, series_id, seriesnum, \
    year, ttype, parent_id, rating, seriesnum_2, title_jvn in titles:
    
    try:
        i += 1
        # print('.', end='', flush=True)
        if i % two_percent_increment == 0:
            print("#", end='', flush=True)

        if parent_id != 0:
            # This title may be a translation. 

            original_fields = get_original_fields(
                title_id, parent_id, cur, psg_cur)

            if not original_fields:
                # This is either just a variant title of an English 
                # work, or it is an English translation but not the most
                # recent. Skip it an wait to process the better title.
                continue
            psg_conn.commit()
            
            original_lang, original_title, original_year = original_fields
            root_id = parent_id
        else:
            root_id = title_id
            original_lang = "English"
            original_title = original_year = None

        if ttype == "SHORTFICTION":
            ttype = "NOVELLA"
        elif ttype in ['COLLECTION', 'ANTHOLOGY', 'OMNIBUS']:
            populate_book_contents_table(title_id, cur, psg_cur)
            psg_conn.commit()

        pub_fields = get_pub_fields(
            title_id, root_id, ttype, alch_conn, psg_cur)
        if not pub_fields:
            # this title isn't available in book form
            continue

        psg_conn.commit()
        editions, pages, cover_image, isbn = pub_fields

        if year == 0:
            year = None

        if title_jvn == 'Yes':
            juvenile = True
        else:
            juvenile = False

        # Unless this is a translation, check for alternate English titles
        if (original_title == ENGLISH):
            alt_titles = get_alternate_titles(title_id, title, cur)
        else:
            alt_titles = None

        authors = get_authors(title_id, cur)

        wikipedia = get_wikipedia_link(title_id, cur)

        won_award = get_won_award(title_id, cur)

        if synopsis_id:
            synopsis = get_synopsis(synopsis_id, cur)
        else:
            synopsis = None


        if note_id:
            note = get_note(note_id, cur)
        else:
            note = None


        if series_id:
            series_str_1, series_str_2 = get_series_strings(
                series_id, seriesnum, seriesnum_2, cur)
        else:
            series_str_1 = series_str_2 = None

        psg_cur.execute("""
            INSERT INTO books 
            (title_id, title, year, authors, book_type, isbn, pages, editions, 
            alt_titles, series_str_1, series_str_2, original_lang, 
            original_title, original_year, isfdb_rating, won_award, juvenile, 
            cover_image, wikipedia, synopsis, note) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (title_id, title, year, authors, ttype, isbn, pages, editions,
            alt_titles, series_str_1, series_str_2, original_lang, 
            original_title, original_year, rating, won_award, juvenile, 
            cover_image, wikipedia, synopsis, note))
        psg_conn.commit()

    except Exception as e:
        print("\nERROR: {}\t{}".format(title_id, title))
        print(e)
        psg_cur.close()
        psg_conn.close()
        psg_conn = psycopg2.connect(db_conn_string)
        psg_cur = psg_conn.cursor()


cur.close()
conn.close()
alch_conn.close()

# constrain_and_index_book_tables(psg_cur)
psg_conn.commit()

psg_cur.close()
psg_conn.close()

end = time()
total_time = (end - start) / 60
print("#]")
print("total time: {} minutes".format(total_time))
