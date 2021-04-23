#!/usr/bin/env python3

from multiprocessing import Pool, cpu_count, Value, get_logger
from multiprocessing_logging import install_mp_handler
import logging
from datetime import datetime
from math import ceil
import sys

import mysql.connector
import pymysql
from sqlalchemy import create_engine
import psycopg2

from migration_functions import *

PROGRESS_BAR = True
N_PROC = -1
LIMIT = None
DEBUG = False
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

    if PROGRESS_BAR:
        with i.get_lock():
            i.value += 1
            if i.value % two_percent_increment == 0:
                print("#", end='', flush=True)

    title_id, title, synopsis_id, note_id, series_id, seriesnum, \
    year, ttype, parent_id, rating, seriesnum_2, title_jvn = title_data

    source_conn = mysql.connector.connect(**source_db_params)
    try:
        source_cur = source_conn.cursor()


        #       ORIGINAL DATA
        # For titles translated into my_lang, get bibliographic data about 
        # the original. Get data about existing translations into my_lang.
        if parent_id != 0:
            # This title may be a translation. 

            original_fields = get_original_fields(
                title_id, parent_id, source_cur, language_dict)

            if not original_fields:
                # This is either just a variant title of an English 
                # work, or it is an English translation but not the most
                # recent. Skip it an wait to process the better title.
                logger.info("\n{}\t{}\tSkipped: Not preferred title". \
                    format(title_id, title))
                with titles_skipped.get_lock():
                    titles_skipped.value += 1
                return
                # return False
            
            root_id = parent_id
            original_lang, original_title, \
                original_year, translations = original_fields
        else:
            root_id = title_id
            original_lang = "English"
            original_title = original_year = None
            translations = []


        # The only shortfiction being processed are Novellas
        if ttype == "SHORTFICTION":
            ttype = "NOVELLA"


        # if ttype != 'NOVELLA':
        #     contents = get_contents(title_id, source_cur)


        #       PUBLICATION DATA
        # Choose representative publications to get a cover, page number, etc.
        # Get all covers and isbns to link to this title in their own tables.
        with alchemyEngine.connect() as source_alch_conn:
            pub_fields = get_pub_fields(
                title_id, root_id, ttype, source_alch_conn)
        source_alch_conn.close()

        if not pub_fields:
            # this title isn't available in book form
            logger.info("\n{}\t{}\tSkipped: Not available as a book" \
                .format(title_id, title))
            with titles_skipped.get_lock():
                    titles_skipped.value += 1
            return
            # return False

        stand_alone, editions, pages, \
            cover_image, isbn, all_isbns, more_images = pub_fields


        #       ETC
        authors = get_authors(title_id, source_cur)
        wikipedia = get_wikipedia_link(title_id, source_cur)
        award_winner = get_award_winner(title_id, source_cur)


        # Unless this is a translation, check for alternate English titles
        if (original_lang == my_lang):
            alt_titles = get_alternate_titles(title_id, title, source_cur)
        else:
            alt_titles = None

        if series_id:
            series_str_1, series_str_2 = get_series_strings(
                series_id, seriesnum, seriesnum_2, source_cur)
        else:
            series_str_1 = series_str_2 = None

        if synopsis_id:
            synopsis = get_synopsis(synopsis_id, source_cur)
        else:
            synopsis = None

        if note_id:
            note = get_note(note_id, source_cur)
        else:
            note = None

        if year == 0 or year == 8888:
            year = None

        if title_jvn == 'Yes':
            juvenile = True
        else:
            juvenile = False

    except:
        logger.exception("\n{}\t{}\tSource db error".format(
                    title_data[0], title_data[1]))
        with titles_errored.get_lock():
            titles_errored.value += 1
        return
    finally:
        source_conn.close()


    dest_conn = psycopg2.connect(dest_db_conn_string)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:


                dest_cur.execute("""
                    INSERT INTO books 
                    (title_id, title, year, authors, book_type, 
                    isbn, pages, editions, alt_titles, 
                    series_str_1, series_str_2, 
                    original_lang, original_title, original_year, 
                    isfdb_rating, award_winner, juvenile, stand_alone, 
                    cover_image, wikipedia, synopsis, note) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    title_id, title, year, authors, ttype, 
                    isbn, pages, editions, alt_titles, 
                    series_str_1, series_str_2, 
                    original_lang, original_title, original_year, 
                    rating, award_winner, juvenile, stand_alone, 
                    cover_image, wikipedia, synopsis, note )
                )


                for book_isbn in all_isbns:
                    dest_cur.execute("""
                        INSERT INTO isbns 
                        (isbn, title_id) 
                        VALUES (%s, %s);
                        """, (book_isbn, title_id)
                    )


                for translation_id, translation_title, \
                    tr_year, note in translations:

                    dest_cur.execute("""
                        INSERT INTO translations 
                        (title_id, newest_title_id, title, year, note) 
                        VALUES (%s, %s, %s, %s, %s);
                        """, (translation_id, title_id, translation_title, 
                                tr_year, note))


                for image in more_images:
                    dest_cur.execute("""
                        INSERT INTO more_images 
                        (title_id, image) 
                        VALUES (%s, %s);
                    """, (title_id, image)
                    )

                # for content_id in content_ids:
                #     dest_cur.execute("""
                #         INSERT INTO contents 
                #         (book_title_id, content_title_id) 
                #         VALUES (%s, %s);
                #         """, (title_id, content_id)
                #     )
    except psycopg2.errors.UniqueViolation:
        # Hande isbn duplicates here
        pass
    except:
        logger.exception("\n{}\t{}\tDestination db error".format(
                    title_data[0], title_data[1]))
        with titles_errored.get_lock():
            titles_errored.value += 1
        return
    finally:
            dest_conn.close()

    with titles_added.get_lock():
        titles_added.value += 1
    return


if __name__ == '__main__':

    start = datetime.now()
    if DEBUG:
        logging.basicConfig(level=logging.WARNING)
    else:
        log_path = "/tmp/" + str(start).split('.')[0] + ".log"
        logging.basicConfig(filename=log_path, level=logging.INFO)
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
                success = safe_drop_tables(['isbns', 'contents', 
                    'translations', 'more_images', 'books', 'ttype'], dest_cur)
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

    if N_PROC > 0:
        pool_size = N_PROC
    elif N_PROC < -1:
        pool_size = cpu_count() + N_PROC + 1
    else:
        pool_size = cpu_count()

    titles_added = Value('i', 0)
    titles_skipped = Value('i', 0)
    titles_errored = Value('i', 0)
    semaphore = Value('i', 0)

    print("\nMain title loop...")
    print("Processing {} titles".format(len(titles)))
    print("Start time: {}".format(datetime.now()))
    if PROGRESS_BAR:
        i = Value('i', 0)
        two_percent_increment = ceil(len(titles) / 50)
        print("\n# = {} titles processed".format(two_percent_increment))
        print("1%[" + "    ."*10 + "]100%")
        print("  [", end='', flush=True)


    with Pool(pool_size) as p:
        p.map(process_title, titles)

        
    if PROGRESS_BAR:
        print("]")

    dest_conn = psycopg2.connect(dest_db_conn_string)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                index_book_tables(dest_cur)
    except:
        error_str = "Problem indexing or constraining the tables."
        print(error_str)
        logger.exception(error_str)
    finally:
        dest_conn.close()


    end = datetime.now()
    total_time = (end - start)

    print("\nTitles added: {}".format(titles_added.value))
    print("Titles skipped: {}".format(titles_skipped.value))
    print("Titles errored: {}".format(titles_errored.value))
    print("Total time: {}".format(total_time))

    logger.info("Migration script completed")