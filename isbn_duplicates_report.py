import psycopg2

import setup_configuration as cfg


dest_conn = psycopg2.connect(cfg.DEST_DB_CONN_STRING)
try:
    with dest_conn:
        with dest_conn.cursor() as dest_cur:
            dest_cur.execute(
                """
                select isbn, count(*) as isbn_count
                from isbns
                group by isbn
                having count(*) > 1
                order by count(*) DESC;
                """
            )
            isbn_groups = dest_cur.fetchall()

            print(
                "Group\tCount\tISBN\tForeign\tType\t"
                + "Title ID\tYear\tPages\tAuthors\tTitle\tNote"
            )
            i = 0
            for isbn_group, isbn_count in isbn_groups:
                i += 1
                dest_cur.execute(
                    """
                    select i.isbn, i.foreign_lang, i.book_type, b.title_id,
                        b.year, b.pages, b.authors, b.title, b.note
                    from isbns as i
                    join books as b
                    on b.title_id = i.title_id
                    where i.isbn = %s
                    order by i.isbn;
                """,
                    (isbn_group,),
                )
                results = dest_cur.fetchall()
                for result in results:
                    note = result[-1]
                    if note:
                        note = note.replace("\n", " ").replace("\t", " ")
                    else:
                        note = ""
                    print(str(i) + "\t" + str(isbn_count), end="")
                    print(
                        "\t" + "\t".join([str(j) for j in result[:-1]]), end=""
                    )
                    print("\t" + note, end="")
                    print(
                        "\thttp://www.isfdb.org/cgi-bin/title.cgi?"
                        f"{result[1]}",
                        end="",
                    )
                    print("\n", end="")

except Exception as e:
    print("ERROR")
    raise e
finally:
    dest_conn.close()
