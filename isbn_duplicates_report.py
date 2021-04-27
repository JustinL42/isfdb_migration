import psycopg2
dest_db_name = "rec_system"
dest_db_conn_string = "dbname={} user=postgres".format(dest_db_name)


dest_conn = psycopg2.connect(dest_db_conn_string)
try:
    with dest_conn:
        with dest_conn.cursor() as dest_cur:
            dest_cur.execute("""
                select isbn
                from isbns
                group by isbn 
                having count(*) > 1;
                """
            )
            isbn_groups = dest_cur.fetchall()

            i = 0
            print("\t".join(['id', 'isbn', 'title_id', 'year', \
                'pages', 'authors', 'title']))
            for isbn_group in isbn_groups:
                i +=1
                dest_cur.execute("""
                    select i.isbn, b.title_id, 
                        b.year, b.pages, b.authors, b.title, b.note
                    from isbns as i
                    join books as b
                    on b.title_id = i.title_id
                    where i.isbn = %s
                    order by i.isbn;
                """, (isbn_group[0], )
                )
                results = dest_cur.fetchall()
                for result in results:
                    note = result[-1]
                    if note:
                        note = note.replace('\n', ' ').replace('\t', ' ')
                    else:
                        note = ''
                    print(str(i), end='')
                    print("\t" + "\t".join([str(j) for j in result[:-1]]), end='')
                    print("\t" + note, end = '')
                    print("\thttp://www.isfdb.org/cgi-bin/title.cgi?{}".format(result[1]), end='')
                    print('\n', end='')

except Exception as e:
    print("ERROR")
    raise e
finally:
    dest_conn.close()

