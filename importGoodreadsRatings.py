#!/usr/bin/python3
import pandas as pd
import psycopg2
import util
import sys

def main(args):
	if args:
		filename = args[0]
	else:
		print("Usage: provide goodreads books csv export as the first")
		print("argument. Optionally, provide an existing user's id.")
		print("Otherwise, the ratings will be added as a new user.")
		return

	global gr_df
	gr_df = pd.read_csv(filename, sep=",", quotechar='"')
	isbnSet = util.get_isbn_set()

	conn = psycopg2.connect("dbname=rec_system user=postgres")
	cur = conn.cursor()

	try:
		user_id = args[1]
	except IndexError:
		#todo
		user_id = "next user id"

	gr_df = gr_df[ gr_df['My Rating'] != 0]

	print("user id: {}".format(user_id))
	print("Speculative fiction ratings in file: {}.".format(
		gr_df.shape[0]))

	ratings_used = 0
	for i, row in gr_df.iterrows():
		isbn = row.ISBN.split('"')[1]
		if not isbn:
			isbn = row.ISBN13.split('"')[1]
		if not isbn:
			continue
		cur.execute(
			"""
			INSERT INTO ratings
			(id, original_isbn, original_rating, isbn, rating ) 
			VALUES (%s, %s, %s, %s, %s)""",
			(user_id, None, row['My Rating'], isbn, 2 * row['My Rating'])
		)
		ratings_used += 1
	conn.commit()

	print("Ratings inserted into database: {}".format(ratings_used))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))