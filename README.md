ISFDB Migration Script
==========================

This script migrates book data from the public Internet Speculative Fiction Database (ISFDB) MySQL database to another database with a simpler schema. Some of its goals include:

1. Combine title and publication entities into a single book entity. 
The distinction between a title and the dinstinct publications of that title are important to collectors and librarians, but are less important to readers. Publication data like page count and cover image are taken from a recent, representative publication and combined the title data to create 'book' entity in the destination database. This makes it simpler for a reader to see all the relevant information about the book on a single page, as opposed to the more complex setup of the isfb.org page. 

2. Focus on books available in a single language.
The target langauge is English by default, but this can be changed just by changing the 'my_lang' variable to any of the other languages for which the isfdb has data for. For books that were originally written in a different language, but later translated into the target language, there is only on book entry. The translation table stores data about each translation of that book into the target language. 

3. Map each ISBN to a single row in the book table.
ISBNs are useful codes for identifying books between applications since they tend to be stable and are only associated with a single publication. The isbns table uniquely maps ISBNs in the source database to IDs in the book table. This is useful for importing a reader's ratings from a GoodReads data export into a new application, for instance. However, some publishers have re-used ISBNs, which means an extra deduplication step needs to be done to make this uniquens guarantee. In some cases, the publisher has reused the ISBN for a new version of the book with only slightly different contents. In this case, the book entries are combined. In other cases, the ISBN is reused for a completely different book. In this case, the ISBN is removed from the database. 

4. Prepare the data for use on a modern web page by converting the latin-encoded text with numeric character reference for unicode to full unicode, and convert all image and external links to https. 

5. Create a search index for a general search on book title, authors, alternate titles, and series. 
This allows for a fast, user-friendly search on the book table (see www.bookclub.guide for a demonstration). The search column generation features stemming (for languages where Postgres supports this), ranking of more important fields over others, and the removal of stop words. Since some book titles consist of a single English stop word ('IT', 'Them', 'ON', etc.), a small, custom stop word list must be used to avoid rendering these titles invisible to searches. The stop word list should also be customized for languages other than English. 

6. Parallelize the process, wherever possible. 
The migration takes over two hours to run on my computer without multiprocessing. It takes about half that time using 4 cores. It should be possible to significantly increase the speed on a machine with more cores, provided the connection to the database doesn't become a bottleneck. The N_PROC variable can be set to the number of threads to use. Set it to -1 to use a number threads equal to the number of the machines cores, -2 use all but one core, etc.


The source database isn't in this repository. It is currently for available for download from isfdb.org under a Creative Commons license:
http://www.isfdb.org/wiki/index.php/ISFDB_Downloads

For instructions on mounting the source MySQL database, see:
http://www.isfdb.org/wiki/index.php/ISFDB:MySQL_Only_Setup

Requirements:
PostgreSQL 13 (older versions my require modification of newer index functions)
Python 3.8
psycopg2
pymysql
sqlalchemy
pandas

This script has only been tested on Ubuntu 18. It is likely to run on other unix-like systems without significant modification. There are known incompatibilities with Windows that would require significant modification.


Required steps:
1) Download the ISFBD database and load it into MYSQL using the above instructions.
2) Set the script configuration variables, including the destination database name, and any required database passwords.
3) In the destination datbase (Postgres) create the database with the name you chose, e.g.:
CREATE DATABSE recsysetl;
4) When the script is run for the first time, it will attempt to create the custom dictionary for indexing. This requires root access. While you can run the main script with root access, it is preferrable to run this command as root instead:

sudo su
python3 -c "from migration_functions import *; setup_custom_stop_words()" 

5) At this point, the main migration_script.py can be run.


Copyright notice for the contents of this repository:
Copyright 2021 Justin Lavoie.
All rights reserved.