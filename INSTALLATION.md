Installation
==========================

These instructions have been tested on Fedora 37. Some details will be different on other platforms. In particular, `*-devel` packages may be named like `*-dev` on Debian-based distros.

1. **Add the MySQL Community repository.** <br>
The download urls can be found [here](https://dev.mysql.com/downloads/repo/yum/). It can then be installed with commands like:
   ~~~
   wget https://dev.mysql.com/get/mysql80-community-release-fc37-1.noarch.rpm
   sudo dnf install -y mysql80-community-release-fc37-1.noarch.rpm
   ~~~

2. **Install OS-level requirements.**
   ~~~
   sudo dnf install -y gcc git libpq-devel python-devel \
      postgresql-server postgresql-contrib \
      mysql-community-server mysql-community-devel
   ~~~
	Confirm your packages meet these minimum version requirements:
	Python 3.8
	MySQL 5.5
	PostgreSQL 13 (needed for newer text index functions)
	
3. **Set up MySQL.** <br>
   For the Fedora, the specific instructions to do this are [here](https://docs.fedoraproject.org/en-US/quick-docs/installing-mysql-mariadb/).
   The minimal steps needed from the above instructions are:
   ~~~
   sudo systemctl start mysqld
   sudo grep 'temporary password' /var/log/mysqld.log
   # Copy the password from the output and use it in the step below:
   sudo mysql_secure_installation
   ~~~
   If you are ok with connecting to the MySQL server as the existing root user, no further MySQL setup is needed. Just save the password you set in the secure installation for later. Otherwise, follow the instructions to create a MySQL user and password with privileges to read all databases.

4. **Clone the repo and install the required dependencies.**
   ~~~
   git clone https://github.com/JustinL42/isfdb_migration
   cd isfdb_migration
   pip install .
   ~~~
   Optionally, install the development dependencies:
   ~~~
   pip install .[dev]
   ~~~

5. **Setup PostgreSQL.**
   ~~~
   sudo postgresql-setup --initdb --unit postgresql
   sudo systemctl start postgresql
   ~~~
   The default Postgres settings make it difficult to access the database as the default `postgres` user unless you are running the script as the OS `postgres` user. It's best to setup a Postgres user for the OS user you plan to use to run the migration script (and who will run any downstream application that will use this database):
   ~~~
   sudo -u postgres psql
   CREATE USER example_name SUPERUSER;
   CREATE DATABASE example_name OWNER example_name;
   ~~~
   Also, create the destination database. It can have any name, but `recsysetl` is the default set in the configuration:
   ~~~
   CREATE DATABASE recsysetl;
   ~~~

6. **Add custom Stop Words.** <br>
If you plan to use the `create_search_indexes` feature, it is necessary to supply Postgres with a custom list of stop words (common word to be excluded from the full-text search index). This is because the default stop word  list for English includes words that are themselves titles of books in the database (e.g., "IT", "Them"), and removing these would make it impossible to lookup these books by title in the database. The customized list only includes those words known to not be any book's full title, namely: "a", "an", "and", "by", "of", "the", and "to". To do this, first determine where Postgres stores it's configuration files:
   ~~~
   pg_config --sharedir 
   # example output: /usr/share/pgsql
   # copy the stop word list from the repo to the above 
   # path's tsearch_data directory and make it accessible to all users:
   sudo cp config/isfdb_title.stop /usr/share/pgsql/tsearch_data/
   sudo chmod 775 /usr/share/pgsql/tsearch_data/isfdb_title.stop
   ~~~

7. **Download and restore the ISFDB MySQL backup.** <br>
   The `gdown` command-line tool included in the dependencies makes this easier to automate. The commands below are for the 2022-12-24 backup. To get the latest version, replace the backup name and url with the newest one from the [ISFDB Downloads page](https://isfdb.org/wiki/index.php/ISFDB_Downloads#Database_Backups).
   ~~~
   gdown --fuzzy https://drive.google.com/file/d/1vtZkNVe0jfHo_4wouA4LZMIPlePAaFl2/view?usp=share_link
   unzip backup-MySQL-55-2022-12-24.zip
   ~~~
   This unzips as a directory called `cgdrive`. Enter mysql shell to restore the backup. This will take several minutes.
   ~~~
   mysql -u root -p
   
   drop database isfdb;
   create database isfdb;
   connect isfdb;
   source cygdrive/c/ISFDB/Backups/backup-MySQL-55-2022-12-24
   ~~~

8. **Configure the script parameters.** <br>
   Look at [`config/default.cfg`](config/default.cfg) to see explanations and defaults for each parameter. Then, look at [`dev.cfg`](config/dev.cfg) and [`prod.cfg`](config/prod.cfg). These both inherit any defaults that they don't override, and are examples of settings you may want to change on development or production systems. You will probably at least need to override `source_db_password` and `dest_db_user`  to the MySQL password and the Postgres user name from the previous steps. The Postgres password (`dest_db_password`) should be explicitly set to nothing if you are using the default `ident` authentication. Set your custom parameters in either `dev.cfg` or `prod.cfg`, or in your own [INI](https://en.wikipedia.org/wiki/INI_file) configuration file with a unique section name.
   To specify which configuration the script should use, set your shell's ENV variable to the file's section name (which may differ from the filename). For instance:
   ~~~
   export ENV=DEV
   ~~~
   If you plan to run the script with the same configuration in the future, this line can be added to user's shell startup script (e.g. `.bashrc`) or the virtual environment's activation script (e.g. `.env/bin/activate`)

9. **Run the migration script.** <br>
This may take multiple hours depending on the number of CPU cores available.
   ~~~
   python migration_script.py
   ~~~
   The script starts by dropping any tables in `recsysetl` from previous runs. To prevent this from occurring accidentally, you will be asked to type the characters `DROP` to confirm this. This behavior may be configurable in a future version.
   When the migration is finished, a backup of the final Postgres database will be dumped to the `/tmp` directory.
   