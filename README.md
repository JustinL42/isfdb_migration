ISFDB Migration Script
==========================

This script migrates the Internet Speculative Fiction Database (ISFDB) MySQL backups to a Postgres database. The schema is greatly simplified and denormalized in the process, and useful indexes are added. I developed it to source the book-related tables for my web application [BookClub.Guide](http://bookclub.guide) ([source code](https://github.com/JustinL42/rec_sys_app)). While the script was initially simple, it grew in complexity to support the features of the site, and to address problems in the source data as they were discovered. The source database isn't in this repository. It's currently available from the [ISFDB Downloads page](https://isfdb.org/wiki/index.php/ISFDB_Downloads#Database_Backups) under a Creative Commons license.

## Installation and Usage

See [INSTALLATION.md](INSTALLATION.md).

## Goals and Features

1. **Combine title and publication entities into a single book entity.** <br>
BookClub.Guide aims to help readers and book clubs decide what to read next. The distinction between a title and the unique publications of that title is important to collectors and bibliographers, but is less important to readers. Publication data like page count and cover image are taken from a recent, representative publication and combined with the title data to create a "book" entity in the destination database. This makes it simpler for a reader to see all the relevant information about the book on a single page, as opposed to the more complex setup of the ISFDB.org pages.

2. **Create a search index for a general search on book title, authors, alternate titles, and series.** <br>
This allows for a fast, user-friendly search on the `books` table. The search column generation features stemming (for languages where Postgres supports this), ranking of more important fields over others, and the removal of stop words. Intelligent handling of misspellings and typos in search queries is supported by creating a table of word frequencies that an application can use to guess the user's most likely intended word ([demo](https://bookclub.guide/search/?search=teh+silmarrilion+tolkein)).

3. **Map each ISBN to a single row in the `books` table.** <br>
ISBNs are useful for identifying books between applications since they tend to be stable and should only be associated with a single publication. The `isbns` table uniquely maps ISBNs in the source database to IDs in the `books` table. This is useful for importing a reader's ratings from the BookCrossings data set or a GoodReads export into BookClub.Guide, for instance. 
However, some publishers have reused ISBNs, which means an extra deduplication step is needed to make this uniqueness guarantee. Often, the ISBN has just been reused for a new version of the book with only slightly different contents. In this case, the book entries are combined. In other cases, the ISBN was reused for a completely different book. These ISBNs are removed, since they can't reliably be used to identify books.

4. **Limit the migration to books available in a single language.** <br>
The target language is English by default, but this can be changed by changing the 'my_lang' parameter to any of the other languages for which the ISFDB has data for. Books are included if they were originally written in a different language, but translated into the target language at least once. The `translations` table stores data about each each known translation of that book into the target language.

5. **Parallelize the process, wherever possible.** <br>
The migration takes over two hours to run on my computer without multiprocessing. It takes about half that time using 4 cpu cores. By default, the script uses all but one of the system's available cores. See the `n_proc` in [config/default.cfg](config/default.cfg) for more information.

6. **Create a `contents` table, recording which book titles are contained in other books.** <br>
This helps readers find out which anthologies contain a specific novella, or which novels are included in an omnibus volume, etc. This version of the script doesn't migrate short stories or other short fiction shorter than a novella.
