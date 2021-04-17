#!/usr/bin/env python3

import mysql.connector
from mysql.connector import (connection)
import psycopg2

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="isfdb",
)

cur = conn.cursor()

# Get all the novels, novellas, collections, and omnibuses 
# where an English (code # 17) version exists, 
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
    AND title_language = 17
    AND title_non_genre != 'Yes' 
    AND title_graphic != 'Yes'
    AND title_id NOT IN (
        SELECT title_id 
        FROM  canonical_author 
        WHERE author_id in (4853, 2857, 319407)
    )
    LIMIT 10000
    ; """)


titles = cur.fetchall()

i = 0
for title_id, title, synopsis_id, note_id, series_id, seriesnum, \
    year, ttype, parent_id, rating, seriesnum_2 in titles:
    
    i += 1
    if i > 10000:
        break

    if parent_id != 0:
        # This title may be a translation. 

        cur.execute("""
            SELECT lang_name
            FROM languages
            WHERE lang_id in (
                SELECT title_language
                FROM titles
                WHERE title_id = %s
            )
            """, (parent_id,))
        original_language = cur.fetchone()[0]

        # If the original language is also English, this is just a variant title.
        # Process this title when the loop is the parent title instead.
        if original_language == "English":
            continue

        # This is an English translation of a foreign language work
        # Get all the English translations
        cur.execute("""
            SELECT title_id, title_title, YEAR(title_copyright) as year, note_id 
            FROM titles 
            WHERE title_language = 17 
            AND title_parent = %s 
            ORDER BY year DESC, title_id DESC ;
            """, (parent_id,))
        translations = cur.fetchall()

        # Wait to process this work if we aren't on the most recent translation
        if title_id != translations[0][0]:
            continue

        #TODO
        # insert the translations into the translation table
        # translation_title_id, translation_title, newest_title_id, year


    cur.execute("""
        SELECT author_canonical
        FROM authors
        WHERE author_id IN (
            SELECT author_id
            FROM canonical_author
            WHERE title_id = %s
        );
        """, (title_id,))
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
        AND title_language = 17
    );
    """, (title_id,))
    title_set = set([r[0] for r in cur.fetchall()]) - set(title)
    if title_set:
        alt_titles_str = "; ".join([title for title in title_set])

        # if alt_titles_str is too long, it is probably an 
        # injudicious application of alternate titles and shouldn't be used
        if len(alt_titles_str) > 750:
            alt_titles_str = None

    if year == 0:
        year = None

    if ttype == "SHORTFICTION":
        ttype = "NOVELLA"

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

    ################
    # Useful test cases
    # 1608: fellowship of ring
    # 2157884: Mira's last dance
    # 1475: Nueromancer
    # 41897: call of cthulhlu

    # page count
    if ttype in ('NOVEL', 'NOVELLA'):
        allowed_ctypes = "('NOVEL', 'CHAPBOOK') "
    else:
        allowed_ctypes = "('" + ttype + "') "

    # Try to get page count of a recent edition. Favor hardcovers, 
    # then paperback, then trade paperback, then ebook, then anything else
    sql="""
    SELECT pub_pages
    FROM pubs 
    WHERE pub_id in (
        SELECT pub_id 
        FROM pub_content 
        WHERE title_id = %s
    ) 
    AND pub_ctype IN """ \
    + allowed_ctypes \
    + """
    AND pub_pages IS NOT NULL 
    AND pub_pages != '' 
    ORDER BY 
        CASE pub_ptype 
            WHEN 'hc' THEN 1 
            WHEN 'pb' THEN 2 
            WHEN 'tp' THEN 3 
            when 'ebook' THEN 4
            ELSE 5
        END, 
        pub_year DESC, 
        pub_id DESC;
    """

    cur.execute(sql, (title_id,))
    page_counts = cur.fetchall()

    pages = None
    for page_count in page_counts:
        count_str = page_count[0]
        try:
            pages = int(count_str)
            break
        except ValueError:
            pass

        # Might be in a format like: "vii+125+[10]" or "125+[10]"
        for i in (1,0):
            try:
                pages = int(count_str.split("+")[i])
                break
            except IndexError:
                pass
            except ValueError:
                pass
        if pages:
            break

    #TODO: There could be novellas that were never published as 
    # stand-alone works, but appear in omnibuses. In some cases, it 
    # could be possible to calculate their page numbers from the omnibus
    # contents, but it would be a lot of work for a few cases


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



cur.close()
conn.close()