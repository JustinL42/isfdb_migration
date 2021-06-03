

# This title_id originally belonged to a title that was deleted in the 
# isfdb and is a convenient place holder to represent isbns that have 
# been deleted due to being re-used for completely different books. 
INCONSISTENT_ISBN_VIRTUAL_TITLE = 73

def get_all_isbn_tuples(dest_cur):
    dest_cur.execute("""
        SELECT isbn, title_id, book_type, foreign_lang
        FROM isbns
        """
    )
    return dest_cur.fetchall()

def get_duplicate_isbns(dest_cur):
    dest_cur.execute("""
        SELECT isbn 
        FROM isbns 
        GROUP BY isbn 
        HAVING count(*) > 1
        """
    )
    return dest_cur.fetchall()

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
        """, (INCONSISTENT_ISBN_VIRTUAL_TITLE, ambigous_isbn_note))


def isbn10_to_13(isbn10):
    cd = ( 10 - ( sum([38] + [int(x) * 3 for x in isbn10[:-1][0::2]] + \
        [int(x) for x in isbn10[:-1][1::2]]) % 10 ) ) % 10
    return '978' + isbn10[:-1]  + str(cd)


def isbn13_to_10(isbn13):
    cd = (11 - sum([
        int(digit) * weight \
        for digit, weight \
        in zip(isbn13[3:12], range(10,1,-1))
    ])) % 11
    if cd == 10:
        cd = 'X'
    return isbn13[3:12] + str(cd)


def delete_isbn(isbn, dest_cur):

    dest_cur.execute("""
        INSERT INTO isbns
        (isbn, title_id, book_type)
        VALUES(%s, %s, 'NOVEL');
        """, (isbn, INCONSISTENT_ISBN_VIRTUAL_TITLE)
    )

    dest_cur.execute("""
        DELETE 
        FROM isbns
        WHERE title_id != %s
        AND isbn = %s;
        """, (INCONSISTENT_ISBN_VIRTUAL_TITLE, isbn)
    )

    dest_cur.execute("""
        UPDATE books
        SET isbn = NULL
        WHERE isbn = %s;
        """, (isbn,)
    )
            

def winner_takes_all(isbn_claimants, dest_cur):
    # The winner inherits all the losers' contents and  containers, 
    # isbns, translations, and images. Conflicts based on the winner 
    # already having the item are expected and can be ignored.

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

        dest_cur.execute(
            """
            INSERT INTO isbns (isbn, title_id, book_type) 
            SELECT isbn, %s, book_type
            FROM isbns
            WHERE title_id = %s
            ON CONFLICT DO NOTHING;
            """, (winner_id, claimant_id)
        )

        dest_cur.execute(
            """
            UPDATE translations
            SET lowest_title_id = %s
            WHERE lowest_title_id = %s;
            """, (winner_id, claimant_id)
        )

        dest_cur.execute(
            """
            INSERT INTO more_images (title_id, image) 
            SELECT %s, image
            FROM more_images
            WHERE title_id = %s
            ON CONFLICT DO NOTHING;
            """, (winner_id, claimant_id)
        )

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

        # Delete the losing book. 
        # Deletes will cascade to linked tables.
        dest_cur.execute(
            """
            DELETE FROM books
            WHERE title_id = %s;
            """, (claimant_id,)
        )

    #TODO add losers' titles as alternate titles
    dest_cur.execute("""
        UPDATE books
        SET inconsistent = TRUE
        WHERE title_id = %s
        """, (winner_id, )
    )


def simplify_title(title):
    title = title.lower()

    # remove internal stopwords
    for stop_word in [' a ', ' an ', ' by', ' of ', ' the ', ' to ']:
        title = title.replace(stop_word, ' ')

    # remove initial stop words
    for stop_word in ['a', 'an', 'by', 'of', 'the', 'to']:        
        title = title[len(stop_word):] if title.startswith(stop_word) \
            else title

    # delete spaces and most common puncutation
    for char in ' ,.;\'"()|\\?![]':
        title = title.replace(char, '')

    return title

