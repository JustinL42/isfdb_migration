ISFDB Migration Script
==========================

This script migrates book data from the public Internet Speculative Fiction Database (ISFDB) MySQL database to another database with a simpler schema. Some of its goals include:

1. Combine title and publication entities into a single book entity. 

2. Focus on books available in a single language.

3. Map each ISBN to a single row in the book table.

The source database isn't in this repository. It is currently for available for download from isfdb.org under a Creative Commons license:
http://www.isfdb.org/wiki/index.php/ISFDB_Downloads

For instructions on mounting the source MySQL database, see:
http://www.isfdb.org/wiki/index.php/ISFDB:MySQL_Only_Setup

Copyright notice for the contents of this repository:
Copyright 2021 Justin Lavoie.
All rights reserved.
