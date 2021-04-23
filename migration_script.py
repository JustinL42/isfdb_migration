#!/usr/bin/env python3

from multiprocessing import Pool
from datetime import datetime
import logging
from math import ceil
import sys

import mysql.connector
import pymysql
from sqlalchemy import create_engine
import psycopg2

from migration_functions import *

PROGRESS_BAR = True
N_THREADS = 3
LIMIT = 1000
# The id number for English in the languages table
my_lang = 17

source_db_params = dict(
    host="localhost",
    user="root",
    password="",
    database="isfdb"
)

dest_db_name = "rec_system"
dest_db_conn_string = "dbname={} user=postgres".format(dest_db_name)

source_db_alchemy_conn_string ='mysql+pymysql://root:@127.0.0.1/isfdb'


def process_title(title_data):

    title_id, title, synopsis_id, note_id, series_id, seriesnum, \
    year, ttype, parent_id, rating, seriesnum_2, title_jvn = title_data

    if parent_id != 0:
        # This title may be a translation. 

        original_fields = get_original_fields(
            title_id, parent_id, source_cur, language_dict, dest_cur)

        if not original_fields:
            # This is either just a variant title of an English 
            # work, or it is an English translation but not the most
            # recent. Skip it an wait to process the better title.
            return False
        # dest_conn.commit()
        
        root_id = parent_id
        original_lang, original_title, \
            original_year, translations = original_fields
    else:
        root_id = title_id
        original_lang = "English"
        original_title = original_year = translations = None

    if ttype == "SHORTFICTION":
        ttype = "NOVELLA"
    elif ttype in ['COLLECTION', 'ANTHOLOGY', 'OMNIBUS']:
        contents = populate_contents_table(title_id, source_cur)
        # dest_conn.commit()

    pub_fields = get_pub_fields(
        title_id, root_id, ttype, source_alch_conn)
    if not pub_fields:
        # this title isn't available in book form
        return False

    # dest_conn.commit()
    stand_alone, editions, pages, \
        cover_image, isbn, all_isbns, more_images = pub_fields

    if year == 0 or year == 8888:
        year = None

    if title_jvn == 'Yes':
        juvenile = True
    else:
        juvenile = False

    # Unless this is a translation, check for alternate English titles
    if (original_title == my_lang):
        alt_titles = get_alternate_titles(title_id, title, source_cur)
    else:
        alt_titles = None

    authors = get_authors(title_id, source_cur)

    wikipedia = get_wikipedia_link(title_id, source_cur)

    award_winner = get_award_winner(title_id, source_cur)

    if synopsis_id:
        synopsis = get_synopsis(synopsis_id, source_cur)
    else:
        synopsis = None

    if note_id:
        note = get_note(note_id, source_cur)
    else:
        note = None

    if series_id:
        series_str_1, series_str_2 = get_series_strings(
            series_id, seriesnum, seriesnum_2, source_cur)
    else:
        series_str_1 = series_str_2 = None

    dest_cur.execute("""
        INSERT INTO books 
        (title_id, title, year, authors, book_type, isbn, pages, editions, 
        alt_titles, series_str_1, series_str_2, original_lang, 
        original_title, original_year, isfdb_rating, award_winner, 
        juvenile, stand_alone, cover_image, wikipedia, synopsis, note) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (title_id, title, year, authors, ttype, isbn, pages, editions,
        alt_titles, series_str_1, series_str_2, original_lang, 
        original_title, original_year, rating, award_winner, juvenile, 
        stand_alone, cover_image, wikipedia, synopsis, note))
    dest_conn.commit()
    return True


if __name__ == '__main__':

    start = datetime.now()
    log_path = "/tmp/" + str(start).split('.')[0] + ".log"
    logging.basicConfig(filename=log_path, level=logging.INFO)
    # logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__name__)
    logger.info("{}\tStarting Logging".format(str(start)))

    # The code for English in the languages table
    # TODO: make this configurable script parameter
    my_lang = 17

    # Try to delete and the table (with user interaction) 
    # and create them again from scratch
    dest_conn = psycopg2.connect(dest_db_conn_string)
    dest_conn.autocommit = True
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                success = safe_drop_tables(['books', 'isbns', 'contents', 
                    'translations', 'more_images', 'ttype'], dest_cur)
                if not success:
                    sys.exit(1)
                prepare_books_tables(dest_cur)
    except:
        print("Problem deleting or creating the tables.")
        print("Exiting migration script.")
        sys.exit(1)
    finally:
        dest_conn.close()
        print("Closed connection")

    # Get dictionary of language codes and all the titles specified by 
    # the query in get_all_titles.
    source_conn = mysql.connector.connect(**source_db_params)
    try:
        source_cur = source_conn.cursor()
        language_dict = get_language_dict(source_cur)
        print("Main ISFDB title table query...")
        titles = get_all_titles(source_cur, limit=LIMIT)
    finally:
        source_conn.close()

    alchemyEngine = create_engine(source_db_alchemy_conn_string)
    source_alch_conn = alchemyEngine.connect()

    titles_added = 0
    titles_skipped = 0
    titles_errored = 0

    print("\nMain title loop...")
    print("Processing {} titles".format(len(titles)))
    print("Start time: {}".format(datetime.now()))
    if PROGRESS_BAR:
        two_percent_increment = ceil(len(titles) / 50)
        print("\n# = {} titles processed".format(two_percent_increment))
        print("1%[" + "    ."*10 + "]100%")
        print("  [", end='', flush=True)

    i = 0
    for title_data in titles:
        i += 1
        if PROGRESS_BAR and  i % two_percent_increment == 0:
            print("#", end='', flush=True)
        try:
            added_to_db = process_title(title_data)
            if added_to_db:
                titles_added += 1
            else:
                titles_skipped += 1
        except Exception:
            logger.exception(
                "\ntitle_id: {}\ntitle: {}".format(
                    title_data[0], title_data[1]))
            titles_errored += 1
            dest_cur.close()
            dest_conn.close()
            dest_conn = psycopg2.connect(dest_db_conn_string)
            dest_cur = dest_conn.cursor()


    source_cur.close()
    source_conn.close()
    source_alch_conn.close()

    dest_conn = psycopg2.connect(dest_db_conn_string)
    dest_cur = dest_conn.cursor()
    index_book_tables(dest_cur)
    dest_conn.commit()


    end = datetime.now()
    total_time = (end - start)
    if PROGRESS_BAR:
        print("]")
    print("\nTitles added: {}".format(titles_added))
    print("Titles skipped: {}".format(titles_skipped))
    print("Titles errored: {}".format(titles_errored))
    print("Total time: {}".format(total_time))

    constrain_book_tables(dest_cur)
    dest_cur.close()
    dest_conn.close()
    logger.info("Migration script completed")