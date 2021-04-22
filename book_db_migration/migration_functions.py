import pandas as pd

# Code from languages table
# Default is 17 for English
my_lang = 17
excluded_authurs = [4853, 2857, 319407]

def safe_drop_tables(tables, dest_cur):
    print("Are you sure you want to drop these tables/types " + \
        "and permanently lose any data they may contain?\n")
    for table in tables:
        print(table)
    print()
    value = input("If yes, type DROP\n")
    if value != 'DROP':
        print("Not dropping tables")
        return False
    print("Dropping tables...")
    for table in tables:
        dest_cur.execute("DROP table IF EXISTS {};".format(table))
    for sql_type in tables:
        dest_cur.execute("DROP type IF EXISTS {};".format(table))
    return True


def prepare_books_tables(dest_cur):

    print("Creating tables...")
    dest_cur.execute("""

        CREATE TYPE ttype as ENUM (
            'NOVEL', 'NOVELLA', 'ANTHOLOGY', 'COLLECTION', 'OMNIBUS');


        CREATE TABLE books (
            title_id            integer NOT NULL,
            title               text NOT NULL CHECK (title <> ''),
            year                integer default NULL,
            authors             text default NULL,
            book_type           ttype NOT NULL,
            isbn                varchar(13) default NULL CHECK (isbn <> ''),
            pages               int default NULL,
            editions            int default NULL,
            alt_titles          text default NULL,
            series_str_1        text default NULL,
            series_str_2        text default NULL,
            original_lang       text default NULL,
            original_title      text default NULL,
            original_year       text default NULL,
            isfdb_rating        real default NULL, 
            award_winner        boolean default FALSE,
            juvenile            boolean default FALSE,
            stand_alone         boolean default FALSE,
            cover_image         text default NULL CHECK (cover_image <> ''),
            wikipedia           text default NULL CHECK (wikipedia <> ''),
            synopsis            text default NULL,
            note                text default NULL,
            -- UNIQUE(isbn),
            PRIMARY KEY (title_id)
        );

        CREATE TABLE isbns (
            id          serial PRIMARY KEY,
            isbn        varchar(13) NOT NULL CHECK (isbn <> ''),
            title_id    integer NOT NULL
        );

        CREATE TABLE translations (
            title_id            integer NOT NULL,
            newest_title_id     integer NOT NULL,
            title               text NOT NULL CHECK (title <> ''),
            year                integer default NULL,
            note                text default NULL,
            PRIMARY KEY (title_id)
        );

        CREATE TABLE contents (
            id                  serial PRIMARY KEY,
            book_title_id       integer NOT NULL,
            content_title_id    integer NOT NULL
        );

        CREATE TABLE more_images (
            id          serial PRIMARY KEY,
            title_id    integer NOT NULL,
            image       text default NULL CHECK (image <> '')
        )
        """
    )

def constrain_book_tables(dest_cur):
    print("Index tables...")
    dest_cur.execute("""
        ALTER TABLE isbns 
        ADD FOREIGN KEY (title_id) 
        REFERENCES books(title_id);

        ALTER TABLE translations 
        ADD FOREIGN KEY (newest_title_id) 
        REFERENCES books(title_id);

        ALTER TABLE contents 
        ADD FOREIGN KEY (book_title_id) 
        REFERENCES books(title_id);

        ALTER TABLE contents 
        ADD FOREIGN KEY (content_title_id) 
        REFERENCES books(title_id);
        """
    )

def index_book_tables(dest_cur):
    print("Index tables...")
    dest_cur.execute("""
        CREATE INDEX ON books(book_type);
        CREATE INDEX ON books(isbn);
        CREATE INDEX ON books(original_lang);
        CREATE INDEX ON books(isfdb_rating);
        CREATE INDEX ON books(award_winner);
        CREATE INDEX ON books(juvenile);

        CREATE INDEX ON translations(newest_title_id);

        CREATE INDEX ON contents(book_title_id);
        CREATE INDEX ON contents(content_title_id);

        CREATE INDEX ON more_images(title_id);
        """
    )


def get_language_dict(cur):
    cur.execute("""
        SELECT lang_id, lang_name 
        FROM languages;
        """
    )
    return dict(cur.fetchall())



# Get all the novels, novellas, collections, and omnibuses 
# where a my_lang version exists, 
# and which aren't non-genre, graphic novels, or by an excluded author
def get_all_titles(cur, limit=None):
    print("main title table query...")
    sql = """
        SELECT title_id, title_title, title_synopsis, note_id, series_id, 
            title_seriesnum, YEAR(title_copyright) as year, title_ttype, 
            title_parent, title_rating, title_seriesnum_2, title_jvn
        FROM titles  
        WHERE ( 
            (title_ttype = 'SHORTFICTION' AND title_storylen = 'novella') 
            OR title_ttype IN ('ANTHOLOGY', 'COLLECTION', 'NOVEL', 'OMNIBUS')
        ) 
        AND title_language = {}
        AND title_non_genre != 'Yes' 
        AND title_graphic != 'Yes'
        AND title_id NOT IN (
            SELECT title_id 
            FROM  canonical_author 
            WHERE author_id in ({})
        )""".format(my_lang, ",". join([str(a) for a in excluded_authurs]))

    if limit:
        sql += "\nLIMIT " + str(limit)

    cur.execute(sql)
    return cur.fetchall()


def get_original_fields(title_id, parent_id, cur, language_dict, dest_cur):
    cur.execute("""
        SELECT original.title_title, 
            YEAR(original.title_copyright) as original_year, 
            original.title_language
        FROM titles as original
        JOIN titles as translation
        ON translation.title_parent = original.title_id
        WHERE original.title_id = %s
        LIMIT 1;
        """, (parent_id,))
    original_title, original_year, original_lang = cur.fetchone()


    # If the original language is also my_lang, this is just a variant title.
    # Process this title when the loop is the parent title instead.
    if original_lang == my_lang:
        return False

    # The title is a translation of a foreign language work into my_lang.
    # Get all the my_lang translations
    cur.execute("""
        SELECT title_id, title_title as translation_title, 
            YEAR(title_copyright) as translation_year, note_id 
        FROM titles 
        WHERE title_language = %s 
        AND title_parent = %s 
        ORDER BY translation_year DESC, title_id DESC ;
        """, (my_lang, parent_id))
    translations = cur.fetchall()

    # Wait to process this work if we aren't on the most recent translation
    if title_id != translations[0][0]:
        return False

    for translation_id, translation_title, tr_year, note_id in translations:

        if note_id:
            note = get_note(note_id, cur)
        else:
            note = None

        if tr_year == 0 or tr_year == 8888:
            tr_year = None

        dest_cur.execute("""
            INSERT INTO translations 
            (title_id, newest_title_id, title, year, note) 
            VALUES (%s, %s, %s, %s, %s);
            """, (translation_id, title_id, translation_title, tr_year, note))

    original_lang = language_dict[original_lang]
    if original_year == 0 or original_year == 8888:
        original_year = None

    return original_lang, original_title, original_year


def get_pub_fields(title_id, root_id, ttype, alch_conn, dest_cur):
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


    en_books = all_pubs[( (all_pubs.pub_ctype == "NOVEL") | 
                    (all_pubs.pub_ctype == "CHAPBOOK") |
                    (all_pubs.pub_ctype == "ANTHOLOGY") |
                    (all_pubs.pub_ctype == "COLLECTION") |
                    (all_pubs.pub_ctype == "OMNIBUS") ) &
                    (all_pubs.title_language == my_lang )]

    if en_books.shape[0] == 0:
        # if not available as book in my_lang, don't include this title
        return False

    if ttype == "NOVELLA":
        en_editions = en_books[ (en_books.pub_ctype == "CHAPBOOK") ]
        all_editions = all_pubs[ (all_pubs.pub_ctype == "CHAPBOOK") ]
    else:
        en_editions = en_books[( (en_books.pub_ctype == ttype) )]
        all_editions = all_pubs[( (all_pubs.pub_ctype == ttype) )]

    editions = en_editions.shape[0]
    if editions:
        stand_alone = True
    else:
        # This title (probably a novella) was never published on its own.
        # Don't look for publication related information
        stand_alone = False
        pages = cover_image = isbn = None
        return stand_alone, editions, pages, cover_image, isbn



    # map all remaining ISBNs to this title_id in the isbn table
    all_book_isbns = all_editions[( (all_editions.pub_isbn.notnull()) & 
                            (all_editions.pub_isbn != '') )]\
                            .pub_isbn.drop_duplicates().to_list()

    # Insert all isbns into the isbn table 
    # so we can map from isbn to title_id
    for book_isbn in all_book_isbns:
        dest_cur.execute("""
            INSERT INTO isbns 
            (isbn, title_id) 
            VALUES (%s, %s);
            """, (book_isbn, title_id)
        )

    # A key method to pass to sort_values to choose the best source for
    # page number and cover images. Which fields have higher priority, 
    # or are actually used are determined by the sort_values'by=' keyword
    def preferred_pubs(pub_column):
        if pub_column.name == 'title_id':
            # Favor the curent title_id above all else, which is either 
            #the parent title or the most recent my_lang translation
            favor_title_id = lambda id : 1 if id == title_id else 2
            return pub_column.map(favor_title_id)
        elif pub_column.name == 'pub_ptype':
            #Favor hardcovers, then trade paperback, then paperback, 
            # then ebook, then anything else. 
            favor_hard_cover = lambda ptype : { 'hc' : 1, 'tp' : 2, 
                'pb' : 3, 'ebook' : 4}.get(ptype, 5)
            return pub_column.map(favor_hard_cover)
        elif pub_column.name == 'p_year' or pub_column.name == 'pub_id':
            # Favor newer publications.
            # Make it negative to affect a descending sort on year or id
            return -pub_column
        else:
            print("Invalid column passed to preferred_pubs")
            return pub_column

    pages = None
    all_editions = all_editions.sort_values(
        by=['title_id', 'pub_ptype', 'p_year', 'pub_id'], key=preferred_pubs)
    for page_str in all_editions.pub_pages:
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
    all_editions = all_editions.sort_values(
        by=['title_id', 'p_year', 'pub_id'], key=preferred_pubs)
    # only keep external image links if they are from amazon or isfdb
    preferred_covers = all_editions[(all_editions.pub_frontimage.notnull() ) &
                            (all_editions.pub_frontimage != '' ) &
                        ( ('amazon.com' in str(all_editions.pub_frontimage) ) |
                        ('amazon.ca' in str(all_editions.pub_frontimage) ) |
                        ('isfdb.org' in str(all_editions.pub_frontimage) ) )]\
                        .pub_frontimage.drop_duplicates().to_list()

    if preferred_covers:
        cover_image = preferred_covers[0]

        # insert additional covers into their own table
        for image in preferred_covers[1:]:
            dest_cur.execute("""
                INSERT INTO more_images 
                (title_id, image) 
                VALUES (%s, %s);
            """, (title_id, image)
            )
    else:
        cover_image = None

    preferred_isbns = all_editions[((all_editions.pub_isbn.notnull()) &
                    (all_editions.pub_isbn != '') &
                    (all_editions.pub_ptype.str.contains('audio') == False))]\
                    .pub_isbn.to_list()

    if preferred_isbns:
        isbn = preferred_isbns[0]
    else:
        isbn = None

    return stand_alone, editions, pages, cover_image, isbn


def get_alternate_titles(title_id, title, cur):
    cur.execute("""
    SELECT DISTINCT title_title
    FROM titles
    WHERE title_parent = %s
    AND title_language = %s
    AND title_title != %s
    AND title_title NOT REGEXP 'part [[:digit:]]+ of [[:digit:]]+|boxed set'
    """, (title_id, my_lang, title))
    title_set = set([r[0] for r in cur.fetchall()]) - set(title)
    
    if not title_set:
        return None
    else:
        alt_titles = "; ".join([title for title in title_set])

    # if alt_titles is too long, it is probably an 
    # injudicious application of alternate titles and shouldn't be used
    if len(alt_titles) > 500:
            alt_titles = None

    return alt_titles

def get_authors(title_id, cur):
    cur.execute("""
        SELECT author_canonical
        FROM authors
        WHERE author_id IN (
            SELECT author_id
            FROM canonical_author
            WHERE title_id = %s
        );""", (title_id,))
    authors_results = cur.fetchall()

    if not authors_results:
        return None
    authors_list = [a for result in authors_results for a in result]
    return", ".join(authors_list)


def get_wikipedia_link(title_id, cur):
    cur.execute("""
        SELECT url
        FROM webpages
        WHERE title_id = %s 
        AND url like '%en.wikipedia.org%'
        """, (title_id,))
    wiki_links = cur.fetchall()
    # If there isn't exactly one wikipedia link, 
    # we can't guess which is the general link
    if len(wiki_links) != 1:
        return None
    return wiki_links[0][0]


def get_award_winner(title_id, cur):
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
    return bool(award_result)


def get_synopsis(synopsis_id, cur):
    #TODO: cleanup html tags in synopsis        
    cur.execute("""
        SELECT note_note
        FROM notes
        WHERE note_id = %s
        LIMIT 1;
        """, (synopsis_id,))
    return cur.fetchone()[0]


def get_note(note_id, cur):
    cur.execute("""
        SELECT note_note
        FROM notes
        WHERE note_id = %s
        LIMIT 1;
        """, (note_id,))
    return cur.fetchone()[0]

def get_series_strings(series_id, seriesnum, seriesnum_2, cur):
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


    series_str_1 = "Part"
    if seriesnum:
        series_str_1 += (" " + str(seriesnum))
    if seriesnum_2:
        series_str_1 += ("." + str(seriesnum_2))
    series_str_1 += (" of the " + series_title + " series.")

    if len(parent_series) == 0:
        return series_str_1, None

    if len(parent_series) == 1:
        series_str_2 = " Also part of the " + parent_series[0] + " series."
    else:
        series_str_2 = " Also part of the "
        for series_title in parent_series[:-1]:
            series_str_2 += (series_title + ", ")
        series_str_2 += ("and " + parent_series[-1] + " series.")
    return series_str_1, series_str_2

def populate_contents_table(title_id, cur, dest_cur):
    # Use the pub_content table to find all novels or novellas in the book
    cur.execute("""
        SELECT distinct t.title_id
        FROM titles as t
        JOIN pub_content as c1
        ON t.title_id = c1.title_id
            JOIN pub_content as c2
            ON c1.pub_id = c2.pub_id
        WHERE c2.title_id = %s
        AND t.title_language = %s
        AND (
            t.title_ttype = 'NOVEL' 
            OR (
                t.title_ttype = 'SHORTFICTION'
                AND t.title_storylen = 'novella'
            )
        );""", (title_id, my_lang)
    )
    content_ids = cur.fetchall()
    for content_id in content_ids:
        dest_cur.execute("""
            INSERT INTO contents 
            (book_title_id, content_title_id) 
            VALUES (%s, %s);
            """, (title_id, content_id)
        )
