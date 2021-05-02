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
PROGRESS_BAR = False
N_PROC = -2
LIMIT = None
DEBUG = True
# The id number for English in the languages table
my_lang = 17
dest_db_name = "rec_system"
dest_db_conn_string = "dbname={} user=postgres".format(dest_db_name)

# This title_id originally belonged to a title that was deleted in the 
# isfdb and is a convenient place holder to represent isbns that have 
# been deleted due to being re-used for completely different books. 
AMBI_ISBN_VIRTUAL_TITLE_ID = 73


def insert_virtual_books(dest_cur):
    ambigous_isbn_note = "You have arrived at this page because the " + \
    "ISBN you entered is associated with two or more books with " + \
    "significantly different contents. The book you are looking for is " + \
    "probably included on this site. Search by title instead."

    dest_cur.execute("""
        INSERT INTO books
        (title_id, title, book_type, virtual, note)
        VALUES(%s, 'Ambiguous ISBN', 'NOVEL', True, %s)
        ON CONFLICT DO NOTHING;
        """, (AMBI_ISBN_VIRTUAL_TITLE_ID, ambigous_isbn_note))

def delete_isbn(isbn, dest_cur):

    dest_cur.execute("""
        INSERT INTO isbns
        (isbn, title_id, book_type)
        VALUES(%s, %s, 'NOVEL');
        """, (isbn, AMBI_ISBN_VIRTUAL_TITLE_ID)
    )

    dest_cur.execute("""
        DELETE 
        FROM isbns
        WHERE title_id != %s
        AND isbn = %s;
        """, (AMBI_ISBN_VIRTUAL_TITLE_ID, isbn)
    )

    dest_cur.execute("""
        UPDATE books
        SET isbn = NULL
        WHERE isbn = %s;
        """, (isbn,)
    )
            

def winner_takes_all(isbn_claimants, dest_cur):
    # The winner inherits all the losers' contents and 
    #containers, isbns, translations, and images. Conflicts 
    # based on the winner # already having the item are 
    # expected and can be ignored.

    # b.title_id, b.title, b.year, b.pages, b.alt_titles, b.cover_image, i.book_type, i.foreign_lang

    winner_id = isbn_claimants[0][0]

    for claimant in isbn_claimants[1:]:

        claimant_id, claimant_title, _, _, _, \
            claimant_alt_titles, claimant_cover_image, _, _ = claimant                    

        dest_cur.execute(
            """
            INSERT INTO contents (book_title_id, content_title_id) 
            SELECT %s, content_title_id
            FROM contents
            WHERE (
                book_title_id = %s
                AND content_title_id != %s
            )
            ON CONFLICT DO NOTHING;
            """, (winner_id, claimant_id, winner_id)
        )
        logger.info("{} had {} inserts for content_title_id" \
            .format(winner_id, dest_cur.rowcount))

        dest_cur.execute(
            """
            INSERT INTO contents (book_title_id, content_title_id) 
            SELECT book_title_id, %s
            FROM contents
            WHERE (
                content_title_id = %s
                AND book_title_id != %s
            )
            ON CONFLICT DO NOTHING;
            """, (winner_id, claimant_id, winner_id)
        )
        logger.info("{} had {} inserts for book_title_id" \
            .format(winner_id, dest_cur.rowcount))

        dest_cur.execute(
            """
            INSERT INTO isbns (isbn, title_id, book_type) 
            SELECT isbn, %s, book_type
            FROM isbns
            WHERE title_id = %s
            ON CONFLICT DO NOTHING;
            """, (winner_id, claimant_id)
        )
        logger.info("{} had {} inserts for isbns" \
            .format(winner_id, dest_cur.rowcount))

        dest_cur.execute(
            """
            UPDATE translations
            SET newest_title_id = %s
            WHERE newest_title_id = %s;
            """, (winner_id, claimant_id)
        )
        logger.info("{} had {} inserts for translations" \
            .format(winner_id, dest_cur.rowcount))

        dest_cur.execute(
            """
            INSERT INTO more_images (title_id, image) 
            SELECT %s, image
            FROM more_images
            WHERE title_id = %s
            ON CONFLICT DO NOTHING;
            """, (winner_id, claimant_id)
        )
        logger.info("{} had {} inserts for more_images" \
            .format(winner_id, dest_cur.rowcount))

        if claimant_cover_image:
            dest_cur.execute(
                """
                INSERT INTO more_images (title_id, image) 
                SELECT %s, %s
                FROM more_images
                WHERE title_id = %s
                ON CONFLICT DO NOTHING;
                """, (winner_id, claimant_cover_image, claimant_id)
            )
            logger.info("{} had {} inserts for cover image" \
                .format(winner_id, dest_cur.rowcount))

        # Delete the losing book. 
        # Deletes will cascade to linked tables.
        dest_cur.execute(
            """
            DELETE FROM books
            WHERE title_id = %s;
            """, (claimant_id,)
        )
        logger.info("{} deleted {}. {} rows affected" \
            .format(winner_id, claimant_id, dest_cur.rowcount))

    #TODO add losers' titles as alternate titles
    dest_cur.execute("""
        UPDATE books
        SET inconsistent = TRUE
        WHERE title_id = %s
        """, (winner_id, )
    )


def simplify_title(title):
    title = title.lower()
    for stop_word in [' a ', ' an ', ' the ']:
        title = title.replace(stop_word, ' ')

    for stop_word in ['a', 'an', 'the']:        
        title = title[len(stop_word):] if title.startswith(stop_word) \
            else title

    for char in ' ,.;\'"()|\\?![]':
        title = title.replace(char, '')

    return title



def deduplicate_isbn(duplicate_isbn):
    logger.info(str(duplicate_isbn))

    if PROGRESS_BAR:
        with i.get_lock():
            i.value += 1
            if i.value % two_percent_increment == 0:
                print("#", end='', flush=True)


    dest_conn = psycopg2.connect(dest_db_conn_string)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                dest_cur.execute("""
                    SELECT title_id
                    FROM books
                    WHERE isbn = %s
                    FOR UPDATE;
                    """, duplicate_isbn
                )

                dest_cur.execute("""
                    SELECT b.title_id, b.title, b.authors, b.year, b.pages, 
                        b.alt_titles, b.cover_image, 
                        i.book_type, i.foreign_lang
                    FROM books AS b
                    JOIN isbns AS i
                    ON b.title_id = i.title_id
                    WHERE i.isbn = %s
                    ORDER BY
                        CASE
                            WHEN i.book_type = 'NOVEL' THEN 1
                            WHEN i.book_type = 'NOVELLA' THEN 2
                            WHEN i.book_type = 'OMNIBUS' THEN 3
                            WHEN i.book_type = 'ANTHOLOGY' THEN 4
                            WHEN i.book_type = 'COLLECTION' THEN 5
                        END,
                        b.year DESC, b.pages DESC, b.title_id DESC
                    FOR UPDATE;
                    """, duplicate_isbn
                )

                isbn_claimants = dest_cur.fetchall()


                #TODO lock contents tables


                if len(isbn_claimants) == 0:
                    e_str = str(duplicate_isbn[0]) + ": all records " + \
                        "for this ISBN were already deleted"
                elif len(isbn_claimants) == 1:
                    e_str = str(duplicate_isbn[0]) + \
                        ": all but one record " + \
                        "for this ISBN were already deleted"
                else:
                    e_str = None

                if e_str:
                    raise Exception(e_str)

                _, a_title, a_authors, _, _, _, _, \
                    a_book_type, a_foreign_lang = isbn_claimants[0]

                _, b_title, b_authors, _, _, _, _, \
                    b_book_type, b_foreign_lang = isbn_claimants[1]  

                a_b_book_types =   (a_book_type, b_book_type)

                if (len(isbn_claimants) > 2 or 
                    a_foreign_lang or b_foreign_lang or
                    a_b_book_types in \
                        [('NOVELLA', 'OMNIBUS'), ('NOVEL', 'NOVELLA')]):

                    delete_isbn(duplicate_isbn[0], dest_cur)

                else:


                    if a_b_book_types in [('NOVEL', 'ANTHOLOGY'), \
                        ('NOVEL', 'COLLECTION'), ('NOVEL', 'OMNIBUS'), \
                        ('NOVELLA', 'ANTHOLOGY'), ('NOVELLA', 'COLLECTION')]:

                        titles_match = (a_title.lower() == b_title.lower())

                    else:

                        a_simple_title = simplify_title(a_title)
                        b_simple_title = simplify_title(b_title)
                        titles_match = a_simple_title in b_simple_title or \
                            b_simple_title in a_simple_title

                    if not titles_match:
                        delete_isbn(duplicate_isbn[0], dest_cur)
                    else:
                        a_author_set = set(a_authors.lower().split(', '))
                        b_author_set = set(b_authors.lower().split(', '))
                        authors_in_common = \
                            a_author_set.intersection(b_author_set)
                        if not authors_in_common:
                            delete_isbn(duplicate_isbn[0], dest_cur)
                        else:
                            winner_takes_all(isbn_claimants, dest_cur)

    except:
        logger.exception("\n{}\tFailed to dedulpicate".format(
                    duplicate_isbn[0]))
        with isbns_errored.get_lock():
            isbns_errored.value += 1
        return
    finally:
            dest_conn.close()

    with isbns_deduped.get_lock():
        isbns_deduped.value += 1
    return


if __name__ == '__main__':

    start = datetime.now()
    if DEBUG:
        logging.basicConfig(level=logging.WARN)
    else:
        log_path = "/tmp/" + str(start).split('.')[0] + ".log"
        logging.basicConfig(filename=log_path, level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("{}\tStarting Logging".format(str(start)))


    dest_conn = psycopg2.connect(dest_db_conn_string)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                print("Main isbn table query...")
                dest_cur.execute("""
                    SELECT isbn 
                    FROM isbns 
                    GROUP BY isbn 
                    HAVING count(*) > 1
                    LIMIT %s;
                    """, (LIMIT,)
                )
                duplicate_isbns = dest_cur.fetchall()
                insert_virtual_books(dest_cur)
    finally:
        dest_conn.close()

    if N_PROC > 0:
        pool_size = N_PROC
    elif N_PROC < -1:
        pool_size = cpu_count() + N_PROC + 1
    else:
        pool_size = cpu_count()

    isbns_deduped = Value('i', 0)
    isbns_errored = Value('i', 0)


    #       MAIN TITLE PROCESSING LOOP
    print("\nMain isbn loop...")
    print("Processing {} isbns".format(len(duplicate_isbns)))
    print("Start time: {}".format(datetime.now()))
    if PROGRESS_BAR:
        i = Value('i', 0)
        two_percent_increment = ceil(len(duplicate_isbns) / 50)
        print("\n# = {} isbns processed".format(two_percent_increment))
        print("1%[" + "    ."*10 + "]100%")
        print("  [", end='', flush=True)

    # Process isbns in parallel
    with Pool(pool_size) as p:
        p.map(deduplicate_isbn, duplicate_isbns)
 
    if PROGRESS_BAR:
        print("]")

    end = datetime.now()
    total_time = (end - start)
    print("\nisbns depulicated: {}".format(isbns_deduped.value))
    print("isbns errored: {}".format(isbns_errored.value))
    print("Total time: {}\n".format(total_time))

    #      CONSTRAIN ISBN TABLE
    dest_conn = psycopg2.connect(dest_db_conn_string)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                print("Constraining ISBN table")
                dest_cur.execute("""
                    ALTER TABLE isbns 
                    ADD CONSTRAINT injective_isbn_to_title_id UNIQUE (isbn);
                    """
                )
    finally:
        dest_conn.close()

    logger.info("dedupe script completed")