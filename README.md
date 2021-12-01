ISFDB Migration Script
==========================

This script migrates book data from the public Internet Speculative Fiction Database (ISFDB) MySQL database to a Postgres database more suitable for use by a reader-centric website. I use it to create the book-related tables for [BookClub.Guide](https:\\www.bookclub.guide) (see the [rec_sys_app](https://github.com/JustinL42/rec_sys_app) repository).

Some of its goals include:

1. **Combine title and publication entities into a single book entity.**
The distinction between a title and the distinct publications of that title are important to collectors, but are less important to readers. Publication data like page count and cover image are taken from a recent, representative publication and combined with the title data to create 'book' entity in the destination database. This makes it simpler for a reader to see all the relevant information about the book on a single page, as opposed to the more complex setup of the isfb.org pages. 

2. **Create a search index for a general search on book title, authors, alternate titles, and series.**
This allows for a fast, user-friendly search on the book table. The search column generation features stemming (for languages where Postgres supports this), ranking of more important fields over others, and the removal of stop words. Since some book titles consist of a single English stop word ('IT', 'Them', 'ON', etc.), a small, custom stop word list must be used to avoid rendering these titles invisible to searches. The stop word list should be customized for languages if the script is run with languages other than English.
Intelligent handling of misspellings and typos in search queries is supported by creating a table of word frequencies that an application can you use to guess the user's most likely intended word ([demo](https://bookclub.guide/search/?search=teh+silmarrilion+tolkein))

3. **Map each ISBN to a single row in the book table.**
ISBNs are useful codes for identifying books between applications since they tend to be stable and are only associated with a single publication. The isbns table uniquely maps ISBNs in the source database to IDs in the book table. This is useful for importing a reader's ratings from the BookCrossings data set or a GoodReads export into BookClub.Guide, for instance. 
However, some publishers have re-used ISBNs, which means an extra deduplication step needs to be done to make this uniqueness guarantee. In some cases, the publisher has reused the ISBN for a new version of the book with only slightly different contents. In this case, the book entries are combined. In other cases, the ISBN is reused for a completely different book. These ISBNs are removed, since they can't reliably be used to identify books.

4. **Limit the migration to books available in a single language.**
The target language is English by default, but this can be changed just by changing the 'my_lang' variable to any of the other languages for which the ISFDB has data for. Books are included if they were originally written in a different language, but translated into the target language at least once. The translations table stores data about each each target language translation of that book. 

5. **Parallelize the process, wherever possible.**
The migration takes over two hours to run on my computer without multiprocessing. It takes about half that time using 4 cores. The N_PROC variable can be set to the number of threads to use. Set it to -1 to use a number threads equal to the number of the machines cores, -2 use all but one core, etc.

6. **Create a contents table, recording which book titles are contained in other books.**
This helps readers find out which anthologies contain a specific novella, or which novels are included in an omnibus volume, etc. This version of the script doesn't migrate short stories or any short fiction shorter than a novella.

The source database isn't in this repository. It is currently for available for download from isfdb.org under a Creative Commons license:
[http://www.isfdb.org/wiki/index.php/ISFDB_Downloads](http://www.isfdb.org/wiki/index.php/ISFDB_Downloads)

For instructions on mounting the source My database, see:
[http://www.isfdb.org/wiki/index.php/ISFDB:MySQL_Only_Setup](http://www.isfdb.org/wiki/index.php/ISFDB:MySQL_Only_Setup)

**Requirements:**
PostgreSQL 13 (older versions my require modification of newer index functions)
Python 3.8
psycopg2
pymysql
sqlalchemy
pandas

This script has only been tested on Ubuntu 18. It is likely to run on other unix-like systems without significant modification. There are known incompatibilities with Windows that would require modification.

**Usage:**

1. Download the ISFBD database and load it into MySQL using the above instructions.
2. In migration_script.py, set the script configuration variables, including the destination database name, and any required database passwords.
3. In the destination datbase (Postgres) create the database with the name you chose, e.g.:
	CREATE DATABSE recsysetl;
4. When the script is run for the first time, it will attempt to create the custom dictionary for indexing. This requires root access. While you can run the main script with root access, it is preferrable to run this command as root instead:

	sudo su
	python3 -c "from migration_functions import *; setup_custom_stop_words()" 

5) At this point, the main script can by run:

	python3 migration_script.py
