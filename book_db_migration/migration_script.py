#!/usr/bin/env python3

import mysql.connector
from mysql.connector import (connection)
from sqlalchemy import create_engine
# import psycopg2
import pandas as pd
import pymysql

# The id number for English in the languages table
ENGLISH = 17

# which publications to prefere to get an example page number
# title or the most recent English translation. 

# Within each group, favor newer publications
def pages_sort(pub_column):
    if pub_column.name == 'title_id':
        # Favor the curent title_id above all else, 
        # which is either the parent title 
        # or the most recent English translation
        favor_title_id = lambda id : 1 if id == title_id else 2
        return pub_column.map(favor_title_id)
    elif pub_column.name == 'pub_ptype':
        #Favor hardcovers, then paperback, then trade paperback, 
        # then ebook, then anything else. 
        favor_hard_cover = lambda ptype : { 'hc' : 1, 'pb' : 2, 
            'tp' : 3, 'ebook' : 4}.get(ptype, 5)
        return pub_column.map(favor_hard_cover)
    elif pub_column.name == 'p_year' or pub_column.name == 'title_id':
        # make it negative to affect a descending sort on year or id
        return -pub_column
    else:
        print("Invalid column passed to pages_sort")
        return pub_column


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="isfdb",
)

cur = conn.cursor()

# Get all the novels, novellas, collections, and omnibuses 
# where an English version exists, 
# and which aren't non-genre, graphic novels, or by an excluded author
cur.execute("""
    SELECT title_id, title_title, title_synopsis, note_id, series_id, 
        title_seriesnum, YEAR(title_copyright) as year, title_ttype, title_parent, 
        title_rating, title_seriesnum_2
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
    LIMIT 10000
    ; """, (ENGLISH,))


titles = cur.fetchall()

alchemyEngine = create_engine('mysql+pymysql://root:@127.0.0.1/isfdb')
alch_conn = alchemyEngine.connect()

i = 0
debug_id = 10
# for title_id, title, synopsis_id, note_id, series_id, seriesnum, \
#     year, ttype, parent_id, rating, seriesnum_2 in [titles[debug_id - 1]]:
for title_id, title, synopsis_id, note_id, series_id, seriesnum, \
    year, ttype, parent_id, rating, seriesnum_2 in titles:
    
    i += 1
    print('.', end='', flush=True)
    if i > 10000:
        break

    if parent_id != 0:
        # This title may be a translation. 

        cur.execute("""
            SELECT original.title_title, 
                YEAR(original.title_copyright) as original_year, 
                original.title_language
            FROM titles as original
            JOIN titles as translation
            ON translation.title_parent = original.title_id
            WHERE original.title_id = %s
            LIMIT 1
            """, (parent_id,))
        original_title, original_year, original_language = cur.fetchone()


        # If the original language is also English, this is just a variant title.
        # Process this title when the loop is the parent title instead.
        if original_language == ENGLISH:
            continue

        # This is an English translation of a foreign language work
        # Get all the English translations
        cur.execute("""
            SELECT title_id, title_title as translation_title, 
                YEAR(title_copyright) as translation_year, note_id 
            FROM titles 
            WHERE title_language = %s 
            AND title_parent = %s 
            ORDER BY translation_year DESC, title_id DESC ;
            """, (ENGLISH, parent_id))
        translations = cur.fetchall()

        # Wait to process this work if we aren't on the most recent translation
        if title_id != translations[0][0]:
            continue

        #TODO
        # insert the translations into the translation table
        # translation_title_id, translation_title, newest_title_id, year

        root_id = parent_id
    else:
        root_id = title_id

    ###############################
    # Publictions-based information
    ##############################
    # Useful test cases
    # 1608: fellowship of ring
    # 
    # 2157884: Mira's last dance
    # 1475: Nueromancer
    # 41897: call of cthulhlu
    # 2369460: roadside picnic

    if ttype == "SHORTFICTION":
        ttype = "NOVELLA"

    # editions, pages, cover_image, isbn = get_pub_fields(title_id, root_id, ttype)

    # TODO: remove fields when I no longer need them for debugging:
    # title
    all_pubs = pd.read_sql("""
        SELECT t.title_id, t.title_language, p.pub_id, 
            YEAR(p.pub_year) as p_year, p.pub_pages, p.pub_ptype, 
            p.pub_ctype, p.pub_isbn, p.pub_frontimage
        FROM pubs as p
        JOIN pub_content as c
        ON c.pub_id = p.pub_id
            JOIN titles as t
            ON t.title_id = c.title_id
            WHERE t.title_id = %s
            OR t.title_parent = %s;""", 
        alch_conn, params=[root_id, root_id])


    # editions
    if ttype == "NOVEL" or ttype == "NOVELLA":
        books = all_pubs[( (all_pubs.pub_ctype == "NOVEL") | 
                        (all_pubs.pub_ctype == "CHAPBOOK") |
                        (all_pubs.pub_ctype == "ANTHOLOGY") |
                        (all_pubs.pub_ctype == "COLLECTION") |
                        (all_pubs.pub_ctype == "OMNIBUS") )]
    else:
        # ttype is ANTHOLOGY, COLLECTION or OMNIBUS and must match 
        # the publication type exactly
        books = all_pubs[( (all_pubs.pub_ctype == ttype) )]

    editions = books[books.title_language == ENGLISH].shape[0]

    # If the title was never published in book form, it probably isn't a
    # good book club recommendation, since it is probably hard to find 
    # and not very good
    if editions == 0:
        continue

    # For pages and pictures, further filter Novels and Novellas
    if ttype == "NOVEL" or ttype == "NOVELLA":
        books = books[(books.pub_ctype == "NOVEL") | 
                        (books.pub_ctype == "CHAPBOOK")]

    # map all remaining ISBNs to this title_id in the isbn table
    all_book_isbns = books[( (books.pub_isbn.notnull()) & 
                            (books.pub_isbn != '') )]\
                            .pub_isbn.drop_duplicates().to_list()
    #TODO add isbns to table
    # for book_isbn in all_book_isbns:
    #     print(book_isbn)

    # Only deal with English editions from this point on
    books = books[ books.title_language == ENGLISH]

    # A key method to pass to sort_values to choose the best source for
    # page number and cover images. Which fields have higher priority, 
    # or are actually used are determined by the sort_values'by=' keyword
    def preferred_pubs(pub_column):
        if pub_column.name == 'title_id':
            # Favor the curent title_id above all else, 
            # which is either the parent title 
            # or the most recent English translation
            favor_title_id = lambda id : 1 if id == title_id else 2
            return pub_column.map(favor_title_id)
        elif pub_column.name == 'pub_ptype':
            #Favor hardcovers, then paperback, then trade paperback, 
            # then ebook, then anything else. 
            favor_hard_cover = lambda ptype : { 'hc' : 1, 'pb' : 2, 
                'tp' : 3, 'ebook' : 4}.get(ptype, 5)
            return pub_column.map(favor_hard_cover)
        elif pub_column.name == 'p_year' or pub_column.name == 'pub_id':
            # within each group, favor newer publications
            # make it negative to affect a descending sort on year or id
            return -pub_column
        else:
            print("Invalid column passed to preferred_pubs")
            return pub_column

    pages = None
    books = books.sort_values(
        by=['title_id', 'pub_ptype', 'p_year', 'pub_id'], key=preferred_pubs)
    for page_str in books.pub_pages:
        # Might be in a format like: "vii+125+[10]" or "125+[10]"
        # try the second and then the first positions before giving up
        for ii in (1,0):
            try:
                pages = int(page_str.split("+")[ii])
                break
            except AttributeError:
                pass
            except IndexError:
                pass
            except ValueError:
                pass
        if pages:
            break


    # Don't consider binding type cover image for now
    # TODO: consider removing audio book or ebook covers 
    # if they cause a quality problem
    books = books.sort_values(
        by=['title_id', 'p_year', 'pub_id'], key=preferred_pubs)
    # only keep external image links if they are from amazon or isfdb
    allowed_covers = books[(books.pub_frontimage.notnull() ) &
                        ( ('amazon.com' in str(books.pub_frontimage) ) |
                        ('amazon.ca' in str(books.pub_frontimage) ) |
                        ('isfdb.org' in str(books.pub_frontimage) ) )]\
                        .pub_frontimage.drop_duplicates().to_list()

    if allowed_covers:
        cover_image = allowed_covers[0]
        #TODO: insert additional covers into their own table
        for image in allowed_covers[1:]:
            pass
    else:
        cover_image = None

    preferred_isbns = books[((books.pub_isbn.notnull()) &
                            (books.pub_isbn != '') &
                            (books.pub_ptype.str.contains('audio') == False))]\
                            .pub_isbn.to_list()

    if preferred_isbns:
        isbn = preferred_isbns[0]
    else:
        isbn = None


    cur.execute("""
        SELECT author_canonical
        FROM authors
        WHERE author_id IN (
            SELECT author_id
            FROM canonical_author
            WHERE title_id = %s
        );""", (title_id,))
    authors_results = cur.fetchall()
    if authors_results:
        authors = [a for result in authors_results for a in result]
        author_str = ", ".join(authors)


    # If the title has exactly one link to wikipedia, 
    # assume that is the general wikipedia link for the title
    cur.execute("""
        SELECT url
        FROM webpages
        WHERE title_id = %s 
        AND url like '%en.wikipedia.org%'
        """, (title_id,))
    wiki_links = cur.fetchall()
    if len(wiki_links) == 1:
        wiki = wiki_links[0][0]


    cur.execute("""
        SELECT award_id
        FROM awards
        WHERE award_id IN (
            SELECT award_id
            FROM title_awards
            WHERE title_id = %s
        )
        AND award_level = 1
        LIMIT 1;
        """, (title_id,))
    award_result = cur.fetchone()
    if award_result:
        won_awards = True
    else:
        won_awards = False


    # If this title has child entries that aren't translations,
    # gather possible alternate titles
    cur.execute("""
    SELECT title_title
    FROM titles
    WHERE title_id IN (
        SELECT title_id
        FROM titles
        WHERE title_parent = %s
        AND title_language = %s
    );
    """, (title_id, ENGLISH))
    title_set = set([r[0] for r in cur.fetchall()]) - set(title)
    
    # TODO: filter out alternate titles like: "part x of y"
    if title_set:
        alt_titles_str = "; ".join([title for title in title_set])

        # if alt_titles_str is too long, it is probably an 
        # injudicious application of alternate titles and shouldn't be used
        if len(alt_titles_str) > 750:
            alt_titles_str = None

    if year == 0:
        year = None



    #TODO: cleanup html tags in synopsis
    if synopsis_id:
        cur.execute("""
            SELECT note_note
            FROM notes
            WHERE note_id = %s
            """, (synopsis_id,))
        synopsis = cur.fetchone()[0]

    if note_id:
        cur.execute("""
            SELECT note_note
            FROM notes
            WHERE note_id = %s
            """, (note_id,))
        note = cur.fetchone()[0]

    if series_id:
        cur.execute("""
            SELECT series_title, series_parent
            FROM series
            WHERE series_id = %s
            """, (series_id,))
        series_title, series_parent = cur.fetchone()

        parent_series = []
        while series_parent:
            cur.execute("""
                SELECT series_title, series_parent
                FROM series
                WHERE series_id = %s
                """, (series_parent,))
            parent_title, series_parent = cur.fetchone()
            parent_series.append(parent_title)


        series_str = "Part"
        if seriesnum:
            series_str += (" " + str(seriesnum))
        if seriesnum_2:
            series_str += ("." + str(seriesnum_2))
        series_str += (" of the " + series_title + " series.")

        if len(parent_series) == 1:
            series_str += " Also part of the " \
                + parent_series[0] + " series."

        if len(parent_series) > 1:
            series_str += " Also part of the "
            for series_title in parent_series[:-1]:
                series_str += (series_title + ", ")
            series_str += ("and " + parent_series[-1] + " series.")


#TODO: After main loop through titles:
#    * Populate included_titles table for Omnibuses
#   


cur.close()
conn.close()