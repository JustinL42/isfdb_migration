import mysql.connector
import pickle

# Use the pickled copy if it exists. Otherwise, get it 
# from the db, turn it into a Set and pickle it for next time
def get_isbn_set():
	try:
		return pickle.load(open("isbnSet.pickle", "rb"))
	except FileNotFoundError: 
		mysql_conn = mysql.connector.connect(
			host="localhost",
			user="root",
			password="",
			database="isfdb"
		)

		mysql_cur = mysql_conn.cursor()

		# Get all the pulication ISBNs associated with titles 
		# that aren't graphic, juvenile, or non-genre
		mysql_cur.execute("""
		SELECT DISTINCT pub_isbn
		FROM pubs
		WHERE pub_id in (
			SELECT pub_id 
			FROM pub_content
			WHERE title_id in (
				SELECT title_id
				FROM titles
				WHERE title_non_genre = 'No'
				AND title_graphic = 'No'
				AND title_jvn = 'No'
				AND title_id NOT IN (
					SELECT title_id 
					FROM  canonical_author 
					WHERE author_id in (4853, 2857, 319407)
				)
			) 
		) 
		AND pub_isbn is not NULL
		AND pub_isbn != ''
		AND LENGTH(pub_isbn) <= 13;""")

		isbnSet = set([str(x[0]) for x in mysql_cur.fetchall()])
		mysql_cur.close()
		mysql_conn.close()
		pickle.dump(isbnSet, open("isbnSet.pickle", "wb"))
		return isbnSet