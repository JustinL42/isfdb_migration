#!/usr/bin/env python3

import logging
import subprocess
import sys
from datetime import datetime
from html import unescape
from math import ceil
from multiprocessing import Pool, Value, cpu_count

import mysql.connector
import psycopg2
from sqlalchemy import create_engine

import setup_configuration as cfg
from cold_start import cold_start_ranks
from isbn_deduplication_functions import (
    delete_isbn,
    get_all_isbn_tuples,
    get_duplicate_isbns,
    insert_virtual_books,
    isbn10_to_13,
    isbn13_to_10,
    simplify_title,
    winner_takes_all,
)
from migration_functions import (
    constrain_vacuum_analyze,
    create_custom_text_search_config,
    create_ttype_enum,
    get_all_titles,
    get_alternate_titles,
    get_authors,
    get_award_winner,
    get_contents,
    get_language_dict,
    get_note,
    get_original_fields,
    get_pub_fields,
    get_series_strings,
    get_synopsis,
    get_wikipedia_link,
    index_book_tables,
    populate_search_columns,
    prepare_books_tables,
    safe_drop_tables,
    setup_custom_stop_words,
)


def process_title(title_data):

    if cfg.PROGRESS_BAR:
        with i.get_lock():
            i.value += 1
            if i.value % two_percent_increment == 0:
                print("#", end="", flush=True)

    (
        title_id,
        title,
        synopsis_id,
        note_id,
        series_id,
        seriesnum,
        year,
        ttype,
        parent_id,
        rating,
        seriesnum_2,
        title_jvn,
    ) = title_data

    if year == 8888:
        # this indicates the title was never published
        with titles_skipped.get_lock():
            titles_skipped.value += 1
        return
    if year == 0:
        year = None

    rank = cold_start_ranks.get(title_id, None)

    source_conn = mysql.connector.connect(**cfg.SOURCE_DB_PARAMS)
    try:

        source_cur = source_conn.cursor()

        #       ORIGINAL DATA
        # For titles translated into my_lang, get bibliographic data about
        # the original. Get data about existing translations into my_lang.
        if parent_id != 0:
            # This title may be a translation.

            original_fields = get_original_fields(
                title_id, parent_id, source_cur, language_dict
            )

            if not original_fields:
                # This is either just a variant title of an English
                # work, or it is an English translation but not the most
                # recent. Skip it an wait to process the better title.
                logger.info(
                    f"\n{title_id}\t{title}\tSkipped: Not preferred title"
                )
                with titles_skipped.get_lock():
                    titles_skipped.value += 1
                return

            root_id = parent_id
            (
                original_lang,
                original_title,
                original_year,
                translations,
                lowest_title_id,
            ) = original_fields
        else:
            root_id = title_id
            original_lang = "English"
            original_title = original_year = None
            translations = []

        # The only shortfiction being processed are Novellas
        if ttype == "SHORTFICTION":
            ttype = "NOVELLA"

        #       PUBLICATION DATA
        # Choose representative publications to get a cover, page number, etc.
        # Get all covers and isbns to link to this title in their own tables.
        with alchemyEngine.connect() as source_alch_conn:
            pub_fields = get_pub_fields(
                title_id, root_id, ttype, source_alch_conn
            )
        source_alch_conn.close()

        if not pub_fields:
            # this title isn't available in book form
            logger.info(
                f"\n{title_id}\t{title}\tSkipped: Not available as a book"
            )
            with titles_skipped.get_lock():
                titles_skipped.value += 1
            return
            # return False

        (
            stand_alone,
            editions,
            pages,
            cover_image,
            isbn,
            all_isbns,
            more_images,
        ) = pub_fields

        #       ETC
        authors = get_authors(title_id, source_cur)
        wikipedia = get_wikipedia_link(title_id, source_cur)
        award_winner = get_award_winner(title_id, source_cur)

        # Unless this is a translation, check for alternate English titles
        if original_lang == language_dict[my_lang]:
            alt_titles = get_alternate_titles(title_id, title, source_cur)
        else:
            alt_titles = None

        series_str_1, series_str_2 = get_series_strings(
            series_id, seriesnum, seriesnum_2, parent_id, source_cur
        )

        if synopsis_id:
            synopsis = get_synopsis(synopsis_id, source_cur)
        else:
            synopsis = None

        if note_id:
            note = get_note(note_id, source_cur)
        else:
            note = None

        juvenile = bool(title_jvn == "Yes")

        # For translated works, use data from the most recent
        # translation, but use the lowest title_id of all the
        # translations. This improves the stability of the title_id in
        # the destination database, since it won't change if a new
        # translation is added to the isfdb in the future, but the data
        # will be updated to reflect the newer translation.
        if original_lang != "English":
            title_id = lowest_title_id

    except:
        logger.exception(
            f"\n{title_data[0]}\t{title_data[1]}\tSource db error"
        )
        with titles_errored.get_lock():
            titles_errored.value += 1
        return
    finally:
        source_conn.close()

    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:

                dest_cur.execute(
                    """
                    INSERT INTO books
                    (title_id, title, year, authors, book_type,
                    isbn, pages, editions, alt_titles,
                    series_str_1, series_str_2,
                    original_lang, original_title, original_year,
                    isfdb_rating, cold_start_rank, award_winner,
                    juvenile, stand_alone, cover_image, wikipedia,
                    synopsis, note)
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s);
                """,
                    (
                        title_id,
                        unescape(title),
                        year,
                        authors,
                        ttype,
                        isbn,
                        pages,
                        editions,
                        alt_titles,
                        series_str_1,
                        series_str_2,
                        original_lang,
                        original_title,
                        original_year,
                        rating,
                        rank,
                        award_winner,
                        juvenile,
                        stand_alone,
                        cover_image,
                        wikipedia,
                        synopsis,
                        note,
                    ),
                )

                for book_isbn, book_ttype, foreign_lang in all_isbns:
                    dest_cur.execute(
                        """
                        INSERT INTO isbns
                        (isbn, title_id, book_type, foreign_lang)
                        VALUES (%s, %s, %s, %s);
                        """,
                        (book_isbn, title_id, book_ttype, foreign_lang),
                    )

                for (
                    translation_id,
                    translation_title,
                    tr_year,
                    note,
                ) in translations:

                    dest_cur.execute(
                        """
                        INSERT INTO translations
                        (title_id, lowest_title_id, title, year, note)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (
                            translation_id,
                            title_id,
                            translation_title,
                            tr_year,
                            note,
                        ),
                    )

                for image in more_images:
                    dest_cur.execute(
                        """
                        INSERT INTO more_images
                        (title_id, image)
                        VALUES (%s, %s)
                        ON CONFLICT
                        ON CONSTRAINT more_images_title_id_image_key
                        DO NOTHING;
                    """,
                        (title_id, image),
                    )

    except:
        logger.exception(
            f"\n{title_data[0]}\t{title_data[1]}\tDestination db error"
        )
        with titles_errored.get_lock():
            titles_errored.value += 1
        return
    finally:
        dest_conn.close()

    with titles_added.get_lock():
        titles_added.value += 1
    return


def get_books_for_contents():
    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                dest_cur.execute(
                    """
                    SELECT title_id, book_type
                    FROM books
                    WHERE book_type != 'NOVELLA';
                    """
                )
                volumes = dest_cur.fetchall()
    except:
        logger.exception("Destination db error in get_books_for_contents")
        return False
    finally:
        dest_conn.close()
    return volumes


def populate_contents_for_volume(volume):
    title_id, ttype = volume
    source_conn = mysql.connector.connect(**cfg.SOURCE_DB_PARAMS)
    try:
        source_cur = source_conn.cursor()
        contents = get_contents(title_id, ttype, source_cur)
    except:
        logger.exception(
            f"\n{title_id}\tSource db error in populate_contents_for_volume"
        )
        return
    finally:
        source_conn.close()

    for content in contents:
        dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
        try:
            with dest_conn:
                with dest_conn.cursor() as dest_cur:
                    dest_cur.execute(
                        """
                        INSERT INTO contents
                        (book_title_id, content_title_id)
                        VALUES (%s, %s);
                        """,
                        (title_id, content),
                    )
        except psycopg2.errors.ForeignKeyViolation:
            # Attempts to insert contents not tracked in the books table
            # will fail. These can be safely ignored.
            pass

        except:
            logger.exception(
                f"\n{title_id}\tDestination db error in "
                "populate_contents_for_volume"
            )
        finally:
            dest_conn.close()
    return


def create_isbn_10_and_13(isbn_tuple):
    try:
        isbn, title_id, book_type, foreign_lang = isbn_tuple

        if cfg.PROGRESS_BAR:
            with i.get_lock():
                i.value += 1
                if i.value % two_percent_increment == 0:
                    print("#", end="", flush=True)

        if len(isbn) == 13:
            if isbn[:3] != "978":
                # newer isbn 13s don't have an isbn 10 equivilent
                new_isbn = None
            else:
                new_isbn = isbn13_to_10(isbn)
                if len(new_isbn) != 10:
                    raise Exception(f"Invalid ISBN conversion for: {isbn}")
        elif len(isbn) == 10:
            new_isbn = isbn10_to_13(isbn)
            if len(new_isbn) != 13:
                raise Exception(f"Invalid ISBN conversion for: {isbn}")
        else:
            raise Exception(
                f"ISBN for {title_id} has incorrect number of characters: "
                f"{isbn}"
            )

        if new_isbn:
            dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
            with dest_conn:
                with dest_conn.cursor() as dest_cur:
                    dest_cur.execute(
                        """
                        INSERT INTO isbns
                        (isbn, title_id, book_type, foreign_lang)
                        VALUES(%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (new_isbn, title_id, book_type, foreign_lang),
                    )

        with isbns_processed.get_lock():
            isbns_processed.value += 1
    except:
        logger.exception(f"\n{isbn_tuple[0]}\tFailed to process")
        with isbns_errored.get_lock():
            isbns_errored.value += 1
        return
    finally:
        try:
            dest_conn.close()
        except (NameError, psycopg2.InterfaceError):
            pass


def deduplicate_isbn(duplicate_isbn):
    logger.info(str(duplicate_isbn))

    if cfg.PROGRESS_BAR:
        with i.get_lock():
            i.value += 1
            if i.value % two_percent_increment == 0:
                print("#", end="", flush=True)

    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                dest_cur.execute(
                    """
                    SELECT title_id
                    FROM books
                    WHERE isbn = %s
                    FOR UPDATE;
                    """,
                    duplicate_isbn,
                )

                # Lock the book and isbn enties in question so that
                # concurrent processes don't try to work on the same data
                dest_cur.execute(
                    """
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
                    """,
                    duplicate_isbn,
                )
                isbn_claimants = dest_cur.fetchall()

                # TODO lock contents tables

                if len(isbn_claimants) == 0:
                    e_str = (
                        str(duplicate_isbn[0])
                        + ": all records "
                        + "for this ISBN were already deleted"
                    )
                elif len(isbn_claimants) == 1:
                    # This title was already deduped
                    return
                else:
                    e_str = None

                if e_str:
                    raise Exception(e_str)

                # There are two ways to resolve duplicate ISBNs:
                # delete the ISBN from both titles (most common), or
                # "winner takes all", which is reserved for cases where
                # the book is more or less just a new edition with extra
                # material and the book entries can be merged. The rest
                # of the logic in this method tries to determine this.
                (
                    _,
                    a_title,
                    a_authors,
                    _,
                    _,
                    _,
                    _,
                    a_book_type,
                    a_foreign_lang,
                ) = isbn_claimants[0]

                (
                    _,
                    b_title,
                    b_authors,
                    _,
                    _,
                    _,
                    _,
                    b_book_type,
                    b_foreign_lang,
                ) = isbn_claimants[1]

                # The tuple of book types is a key variable in determining
                # which case this is, since certain changes
                # (novel to collection) are typical of extra material
                # being added to the new edition of a book, while others
                # (novella to omnibus) aren't possible without making it
                # a substantially different book.
                a_b_book_types = (a_book_type, b_book_type)

                if (
                    len(isbn_claimants) > 2
                    or a_foreign_lang
                    or b_foreign_lang
                    or a_b_book_types
                    in [("NOVELLA", "OMNIBUS"), ("NOVEL", "NOVELLA")]
                ):
                    # Cases of more than two titles sharing an ISBN are
                    # pactically always multi-volume series that are
                    # sold under a common ISBN, which than is not useful
                    # for identifying the book. It should be deleted.
                    # Likewise, any case of foreign isbns causing
                    # duplication are too likely to cause errors with
                    # winner-take-all, and cases of book type
                    # transformation considered impossible are also deleted

                    delete_isbn(duplicate_isbn[0], dest_cur)

                else:
                    if a_b_book_types in [
                        ("NOVEL", "ANTHOLOGY"),
                        ("NOVEL", "COLLECTION"),
                        ("NOVEL", "OMNIBUS"),
                        ("NOVELLA", "ANTHOLOGY"),
                        ("NOVELLA", "COLLECTION"),
                    ]:
                        # Book transformations that are technically possible
                        # by adding or removing small amount of material
                        # are considered for winner-takes-all only on an
                        # exact title match

                        titles_match = a_title.lower() == b_title.lower()

                    else:
                        # The most likely cases of book transformation,
                        # included those were a and b are the same type,
                        # have a less strict name match requirement. This
                        # is allow cases like "Ghost Stories", and
                        # "Ghost Stories: now with two bonus stories".

                        a_simple_title = simplify_title(a_title)
                        b_simple_title = simplify_title(b_title)
                        titles_match = (
                            a_simple_title in b_simple_title
                            or b_simple_title in a_simple_title
                        )

                    if not titles_match:
                        delete_isbn(duplicate_isbn[0], dest_cur)
                    else:
                        # The final requirement for winner-takes-all is
                        # for the books to have the same author(s), or
                        # to have some authors in common to account for
                        # cases of stories being added or removed from
                        # some editions.
                        a_author_set = set(a_authors.lower().split(", "))
                        b_author_set = set(b_authors.lower().split(", "))
                        authors_in_common = a_author_set.intersection(
                            b_author_set
                        )
                        if not authors_in_common:
                            delete_isbn(duplicate_isbn[0], dest_cur)
                        else:
                            winner_takes_all(isbn_claimants, dest_cur)

    except:
        logger.exception(f"\n{duplicate_isbn[0]}\tFailed to dedulpicate")
        with isbns_errored.get_lock():
            isbns_errored.value += 1
        return
    finally:
        dest_conn.close()

    with isbns_deduped.get_lock():
        isbns_deduped.value += 1
    return


if __name__ == "__main__":

    start = datetime.now()
    if cfg.DEBUG:
        logging.basicConfig(level=logging.WARNING)
    else:
        log_path = f"/tmp/{str(start).split('.')[0]}.log"
        logging.basicConfig(filename=log_path, level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info(f"{start}\tStarting Logging")
    # TODO: make this configurable script parameter
    my_lang = cfg.ENGLISH
    source_conn = mysql.connector.connect(**cfg.SOURCE_DB_PARAMS)
    try:
        source_cur = source_conn.cursor()
        language_dict = get_language_dict(source_cur)
    finally:
        source_conn.close()

    # Try to delete and the table (with user interaction)
    # and create them again from scratch
    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    dest_conn.autocommit = True
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                if cfg.CREATE_SEARCH_INDEXES:
                    success = setup_custom_stop_words()
                    if not success:
                        sys.exit(1)
                try:
                    success = safe_drop_tables(
                        [
                            "isbns",
                            "contents",
                            "translations",
                            "more_images",
                            "books",
                            "words",
                            "isfdb_title_tsc",
                            "isfdb_title_dict",
                            "ttype",
                        ],
                        dest_cur,
                    )
                except psycopg2.errors.DependentObjectsStillExist:
                    pass
                finally:
                    if not success:
                        sys.exit(1)
                if cfg.CREATE_SEARCH_INDEXES:
                    language_used = create_custom_text_search_config(
                        language_dict[my_lang], dest_cur
                    )
                    if language_used == "simple":
                        warning_str = (
                            "The specified language isn't doesn't have a "
                            "snowball stemmer. Using simple configuration "
                            "instead. Stemming isn't available for title "
                            "search vectors"
                        )
                        print(warning_str)
                        logger.warning(warning_str)
                try:
                    create_ttype_enum(dest_cur)
                except psycopg2.errors.DuplicateObject:
                    pass
                prepare_books_tables(dest_cur)
    except:
        print(
            "Problem with stop word file, "
            + " or with deleting or creating the tables."
        )
        print("Exiting migration script.")
        logger.exception("exception")
        sys.exit(1)
    finally:
        dest_conn.close()

    source_conn = mysql.connector.connect(**cfg.SOURCE_DB_PARAMS)
    try:
        source_cur = source_conn.cursor()
        language_dict = get_language_dict(source_cur)
        print("Main ISFDB title table query...")
        titles = get_all_titles(source_cur, limit=cfg.LIMIT)
    finally:
        source_conn.close()

    alchemyEngine = create_engine(cfg.SOURCE_DB_ALCHEMY_CONN_STRING)

    if cfg.N_PROC > 0:
        pool_size = cfg.N_PROC
    elif cfg.N_PROC < -1:
        pool_size = cpu_count() + cfg.N_PROC + 1
    else:
        pool_size = cpu_count()

    titles_added = Value("i", 0)
    titles_skipped = Value("i", 0)
    titles_errored = Value("i", 0)

    #       MAIN TITLE PROCESSING LOOP
    print("\nMain title loop...")
    print(f"Processing {len(titles)} titles")
    print(f"Start time: {datetime.now()}")
    if cfg.PROGRESS_BAR:
        i = Value("i", 0)
        two_percent_increment = ceil(len(titles) / 50)
        print(f"\n# = {two_percent_increment} titles processed")
        print("1%[" + "    ." * 10 + "]100%")
        print("  [", end="", flush=True)

    # Process titles in parallel
    with Pool(pool_size) as p:
        p.map(process_title, titles)

    if cfg.PROGRESS_BAR:
        print("]")

    end = datetime.now()
    total_time = end - start
    print(f"\nTitles added: {titles_added.value}")
    print(f"Titles skipped: {titles_skipped.value}")
    print(f"Titles errored: {titles_errored.value}")
    print(f"Total time: {total_time}\n")

    #       POPULATE CONTENTS TABLE
    start = datetime.now()
    print("Populating contents table...")
    print(f"Start time: {start}")
    volumes = get_books_for_contents()

    # populate books contents in parallel
    with Pool(pool_size) as p:
        p.map(populate_contents_for_volume, volumes)

    end = datetime.now()
    total_time = end - start
    print(f"Total time: {total_time}\n")

    #       INDEX TABLES
    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                if cfg.CREATE_SEARCH_INDEXES:
                    start = datetime.now()
                    print("Populating search vector columns...")
                    populate_search_columns(dest_cur)
                    end = datetime.now()
                    total_time = end - start
                    print(f"Total time: {total_time}\n")
                print("Creating Indexes...")
                index_book_tables(dest_cur)

                print("\nAdding ISBN 10 and 13 entries where they are missing")
                start = datetime.now()
                print(f"Start time: {start}")
                isbn_tuples = get_all_isbn_tuples(dest_cur)
    except:
        error_str = "Problem indexing or getting ISBNs"
        print(error_str)
        logger.exception(error_str)
    finally:
        dest_conn.close()

    #       MAIN ISBN 10 - 13 LOOP
    isbns_processed = Value("i", 0)
    isbns_errored = Value("i", 0)
    print(f"Processing {len(isbn_tuples)} isbns")
    if cfg.PROGRESS_BAR:
        i = Value("i", 0)
        two_percent_increment = ceil(len(isbn_tuples) / 50)
        print(f"\n# = {two_percent_increment} isbns processed")
        print("1%[" + "    ." * 10 + "]100%")
        print("  [", end="", flush=True)

    # Process isbns in parallel
    with Pool(pool_size) as p:
        p.map(create_isbn_10_and_13, isbn_tuples)

    if cfg.PROGRESS_BAR:
        print("]")

    end = datetime.now()
    total_time = end - start
    print(f"\nisbns processed: {isbns_processed.value}")
    print(f"isbns errored: {isbns_errored.value}")
    print(f"Total time: {total_time}\n")

    #       ISBN DEDUPLICATION
    print("Depulicating ISBNs...")
    start = datetime.now()
    print(f"Start time: {start}")

    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    try:
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                insert_virtual_books(dest_cur)
                duplicate_isbns = get_duplicate_isbns(dest_cur)
    except:
        error_str = "Problem getting duplicate ISBNs"
        print(error_str)
        logger.exception(error_str)
    finally:
        dest_conn.close()

    isbns_deduped = Value("i", 0)
    isbns_errored = Value("i", 0)

    print("\nMain isbn deduplication loop...")
    print(f"Processing {len(duplicate_isbns)} isbns")
    if cfg.PROGRESS_BAR:
        i = Value("i", 0)
        two_percent_increment = ceil(len(duplicate_isbns) / 50)
        print(f"\n# = {two_percent_increment} isbns processed")
        print("1%[" + "    ." * 10 + "]100%")
        print("  [", end="", flush=True)

    # Process isbns in parallel
    with Pool(pool_size) as p:
        p.map(deduplicate_isbn, duplicate_isbns)

    if cfg.PROGRESS_BAR:
        print("]")

    end = datetime.now()
    total_time = end - start
    print(f"\nisbns depulicated: {isbns_deduped.value}")
    print(f"isbns errored: {isbns_errored.value}")
    print(f"Total time: {total_time}\n")

    #      FINAL DATABASE OPERATIONS
    dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
    dest_conn.autocommit = True
    try:
        dest_cur = dest_conn.cursor()
        print("Constraining, Vacuuming, and Analyzing the database...")
        constrain_vacuum_analyze(dest_cur)
    except:
        error_str = "Problem constraining ISBN table, Vacuuming or Analyzing."
        print(error_str)
        logger.exception(error_str)
    finally:
        dest_conn.close()

    logger.info(f"Migration complete. Exporting to /tmp/{cfg.DEST_DB_NAME}...")
    sp = subprocess.Popen(["rm", "-rf", f"/tmp/{cfg.DEST_DB_NAME}"])
    sp.wait()
    pg_dump_cmd = [
        "pg_dump",
        f"{cfg.DEST_DB_NAME}",
        f"--file=/tmp/{cfg.DEST_DB_NAME}",
        "--format=directory",
        f"--jobs={pool_size}",
        f"--port={cfg.DEST_DB_PORT}",
        f"--username={cfg.DEST_DB_USER}",
    ]
    sp = subprocess.Popen(pg_dump_cmd)
    return_code = sp.wait()
    if return_code != 0:
        print(f"There was an error dumping {cfg.DEST_DB_NAME}")
    else:
        print("SUCCESS")
