#!/usr/bin/python3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import util
from math import nan

db_conn_string = "dbname=rec_system user=postgres"

#download and unzip the Book-Crossing Dataset, if not done already
import os
zipDownloaded = os.path.isfile("BX-CSV-Dump.zip")
filesUnzipped = os.path.isfile("BX-Books.csv") and \
				os.path.isfile("BX-Users.csv") and \
				os.path.isfile("BX-Book-Ratings.csv")

if not (zipDownloaded or filesUnzipped):
	from urllib import request
	dataURL = "http://www2.informatik.uni-freiburg.de/~cziegler/BX/BX-CSV-Dump.zip"
	print("fetching zip file...")

	# the download command is commented out to prevent unintentional 
	# traffic to the site. 
	# Uncomment the following line if actually fetching the data.
	# request.urlretrieve(dataURL, "BX-CSV-Dump.zip")

if not filesUnzipped:
	import zipfile
	try:
		zipObj = zipfile.ZipFile("BX-CSV-Dump.zip")
	except FileNotFoundError:
		errorMessage = (
		"ERROR: The data set zip file isn't in the current "
		"directory.\nTo use this script to download it, uncomment "
		"the 'request.urlretrieve' line, "
		"or download it manually from: \n"
		"http://www2.informatik.uni-freiburg.de/~cziegler/BX/")
		print(errorMessage)
		raise SystemExit
	zipObj.extractall()
	zipObj.close()


# convert csv to utf-8, remove problem characters:
os.system("""
	iconv -f ISO_8859-16 -t utf-8 BX-Books.csv > utf-books.csv""")
os.system("""
	iconv -f ISO_8859-16 -t utf-8 BX-Users.csv > utf-users.csv""")
os.system("""
	iconv -f ISO_8859-16 -t utf-8 BX-Book-Ratings.csv |
	tr -d '\\\"' > utf-ratings.csv""")

print("Getting set of speculative fiction ISBNs from ISFDB...")
isbnSet = util.get_isbn_set()


#create database
conn = psycopg2.connect("user=postgres")
conn.autocommit = True
cur = conn.cursor()

# the drop database command is commented out to prevent accidentally
# deleting irrecoverable data. It isn't needed during the initial ETL
# but can be uncommented if the ETL needs to be modified and re-run.
cur.execute("DROP DATABASE IF EXISTS rec_system;")

try:
	cur.execute("CREATE DATABASE rec_system;")
except psycopg2.errors.DuplicateDatabase:
	errorMessage = (
	"ERROR: The rec_system database already exists. If you really "
	"want to delete it and any new data that may have been added, "
	"uncomment the 'DROP DATABASE IF EXISTS' line in the script and "
	"rerun it. Alternately, switch to a different database name")
	print(errorMessage)
	raise SystemExit

if conn.notices:
	print(conn.notices)

cur.close()
conn.close()

print("creating tables...")
conn = psycopg2.connect(db_conn_string)
cur = conn.cursor()

cur.execute("""
	CREATE TABLE books 
	(ISBN varchar(13) NOT NULL default '', 
	title varchar(255) default NULL, 
	author varchar(255) default NULL, 
	year int default NULL, 
	publisher varchar(255) default NULL, 
	image_URL_S varchar(255) default NULL, 
	image_URL_M varchar(255) default NULL, 
	image_URL_L varchar(255) default NULL, 
	PRIMARY KEY  (ISBN));""")
cur.execute("""
	CREATE TABLE users 
	(id int NOT NULL default '0', 
	location varchar(250) default NULL, 
	age int default NULL, 
	PRIMARY KEY (id) );""")
cur.execute("""
	CREATE TABLE ratings 
	(id int NOT NULL, 
	original_ISBN varchar(13) NOT NULL, 
	original_rating real NOT NULL);""")
cur.execute("CREATE INDEX user_index ON ratings (id);")
conn.commit()
cur.close()
conn.close()

print("copying csv data into databases...")
conn = psycopg2.connect(db_conn_string)
conn.autocommit = True
cur = conn.cursor()

cur.copy_expert(r"""
	COPY books 
	FROM STDIN
	DELIMITER ';' 
	ESCAPE '\' 
	CSV HEADER 
	ENCODING 'UTF-8';""",
	open("utf-books.csv"))
cur.copy_expert(r"""
	COPY users 
	FROM STDIN
	DELIMITER ';' 
	NULL 'NULL' 
	ESCAPE '\' 
	CSV HEADER 
	ENCODING 'UTF-8';""",
	open("utf-users.csv"))
conn.commit()

print("filtering and formatting ratings...")
original_ratings = pd.read_csv("utf-ratings.csv", sep=";")

# remove ratings where:
#  * the isbn isn't in the set of speculative fiction books
#  * the rating is zero (representing an implicit rating)
filtered_ratings = original_ratings[(original_ratings.ISBN.isin(isbnSet)) & \
				(original_ratings['Book-Rating'] > 0)]

execute_values(
	cur=cur,
	sql="""
		INSERT INTO ratings
		(id, original_ISBN, original_rating)
		VALUES %s;
		""",
	argslist=filtered_ratings.to_dict(orient="records"),
	template="""(%(User-ID)s, %(ISBN)s, %(Book-Rating)s)"""
)
conn.commit()

# add columns for normalized values and 
# copy the originals as a starting point
cur = conn.cursor()

cur.execute("""
	ALTER TABLE ratings
	ADD COLUMN ISBN varchar(13),
	ADD COLUMN rating real;""")

cur.execute("""
	UPDATE ratings
	set ISBN = original_ISBN, 
		rating = original_rating;""")

conn.commit()
cur.close()

# for known duplicate books, deduplicate by pointed all related reviews
# to the same isbn
from collections import namedtuple

duplicate = namedtuple('duplicate', 
	['title', 'survivingID', 'oldIDs'])
deduplicateList = [
	duplicate("distraction", '0553576399', ['0553104845']),
	duplicate("neuromancer", '0441569579', ['04415l69587', '0441000681', 
		'0441569595', '0441007465']),
	duplicate("ringworld", '0345316754', 
		['0345333926', '0345293010', '0345306341', '0345275500', 
		'0345247957', '0345418409', '0030206561']),
	duplicate("children of men", '0571167411', ['0679418733', '0446679208', 
		'0446364622', '0375705783', '0679422102']),
	duplicate("curse of chalion", '0380979012', ['0380818604']),
	duplicate("cyberiad", '0156235501', ['0156027593']),
	duplicate("dying earth", '0671831526', ['0671441841']),
	duplicate("moon is a harsh mistress", '0425016013', ['0613262654', 
		'0312861761', '0441536999', '0312863551']),
	duplicate("shadow of the torturer", '0671540661', ['0671828258', 
		'0671463985', '0671253255']),
	duplicate("stars my destination", '0425043657', ['0871358816', 
		'0679767800']),
	duplicate("Hyperion", '0425043657', ['0871358816', 
		'0679767800']),
	duplicate("blue sword", '0425075052', ['0425101428', 
		'0425088405', '0688009387', '0441068804', '014130975X', 
		'0425063186','0141311886', ])
	]

cur = conn.cursor()
print("\nremoving duplicates...")
print("(title : duplicates removed)")

for item in deduplicateList:
	cur.execute("""
		update ratings
		set isbn = %s
		where isbn = any(%s)
		""", (item.survivingID, item.oldIDs))
	print("{}: {}".format(item.title, cur.rowcount))
	conn.commit()
	

# find users who now have the multiple ratings for the same book
cur.execute("""
	SELECT id, isbn, avg_rating
	FROM (
		SELECT id, isbn, COUNT(*) AS num, AVG(rating) AS avg_rating
		FROM ratings
		GROUP BY id, isbn
	) as review_counts
	where num > 1;""")
conn.commit()

# assign one of those ratings the user's average rating 
# and delete the rest
for record in cur.fetchall():
	(userID, isbn, avg_rating) = record
	cur2 = conn.cursor()
	cur2.execute("""
		DELETE
		FROM ratings
		WHERE id = %s
		AND isbn = %s""",
		(userID, isbn))
	cur2.execute("""
		INSERT INTO ratings 
		(id, original_isbn, original_rating, isbn, rating ) 
		VALUES (%s, %s, %s, %s, %s)""", 
		(userID, isbn, int(avg_rating), isbn, avg_rating))
	conn.commit()
	cur2.close()

# remove ratings from users who have only one rating in the table
cur.execute("""
	DELETE 
	FROM ratings 
	WHERE id in (
		SELECT id 
		FROM ratings
		GROUP BY id
		HAVING COUNT(*) = 1
	);""")
conn.commit()

# create norm_isbn_index
cur.execute("""
	ALTER TABLE ratings
	ALTER COLUMN original_isbn
	DROP NOT NULL;""")

cur.execute("""
	ALTER TABLE ratings
	ALTER COLUMN original_rating
	DROP NOT NULL;""")

cur.execute("""
	ALTER TABLE ratings
	ALTER COLUMN isbn
	SET NOT NULL;""")

cur.execute("""
	ALTER TABLE ratings
	ALTER COLUMN isbn
	SET NOT NULL;""")

cur.execute("""
	CREATE INDEX isbn_index 
	ON ratings (isbn);""")

conn.commit()
cur.close()
conn.close()

print("add book club users and and reviews")
conn = psycopg2.connect(db_conn_string)
conn.autocommit = True
cur = conn.cursor()

cur.copy_expert(r"""
	COPY users 
	FROM STDIN
	DELIMITER ',' 
	NULL 'NULL' 
	ESCAPE '\' 
	CSV HEADER 
	ENCODING 'UTF-8';""",
	open("book_club_users.csv"))
cur.copy_expert(r"""
	COPY ratings 
	FROM STDIN
	DELIMITER ','
	NULL ''
	CSV HEADER 
	ENCODING 'UTF-8';""",
	open("book_club_ratings.csv"))


book_club_df =  pd.read_csv("book_club_ratings.csv", 
	dtype={'id' : int, 'original_isbn' : str, 'original_rating' : float, 
	'ISBN' : str, 'rating' : float})
book_club_user_df = pd.DataFrame(book_club_df.groupby(by="ISBN", 
	as_index=False)['rating'].mean())
# means_df['id'] = '500000'
# means_df['original_isbn'] = nan
# means_df['original_rating'] = nan
# book_club_user_df =  means_df[['id', 'original_rating', 
	# 'original_isbn', 'ISBN', 'rating']]

execute_values(
	cur=cur,
	sql="""
		INSERT INTO ratings
		(id, original_ISBN, original_rating, ISBN, rating)
		VALUES %s;
		""",
	argslist=book_club_user_df.to_dict(orient="records"),
	template="""(500000, NULL, NULL, %(ISBN)s, %(rating)s)"""
)
conn.commit()



cur.close()
conn.close()
print("\nETL script complete")