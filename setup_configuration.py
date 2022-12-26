import configparser
import os
from pathlib import Path

# Get per-environment settings from the config files.
ENV = os.environ.get("ENV", "DEFAULT")
config_dir = Path(Path(__file__).resolve().parent, "config")
config_parser = configparser.ConfigParser()
config_files = [
    Path(config_dir, f) for f in os.listdir(config_dir) if ".cfg" in f
]
config_parser.read(config_files)
config = config_parser[ENV]

# Set configuration variables
N_PROC = config.getint("n_proc")
DEBUG = config.getboolean("debug")
CREATE_SEARCH_INDEXES = config.getboolean("create_search_indexes")
ENGLISH = config.getint("english")
MY_LANG = ENGLISH
EXCLUDED_AUTHORS = config["excluded_authors"]
INCONSISTENT_ISBN_VIRTUAL_TITLE= config.getint("inconsistent_isbn_virtual_title")
PROGRESS_BAR = config.getboolean("progress_bar")
if config["limit"] in ["", None]:
    LIMIT = None
else:
    LIMIT = config.getint("limit")

SOURCE_DB_PARAMS = dict(
    host=config["source_db_host"],
    port=config.getint("source_db_port"),
    user=config["source_db_user"],
    password=config["source_db_password"],
    database=config["source_db_name"],
)

SOURCE_DB_ALCHEMY_CONN_STRING = (
    f"mysql+pymysql://{config['source_db_user']}:"
    f"{config['source_db_password']}@"
    f"{config['source_db_host']}/"
    f"{config['source_db_name']}"
)

DEST_DB_CONN_STRING = (
    f"dbname={config['dest_db_name']} "
    f"user={config['dest_db_user']} " 
    f"password={config['dest_db_password']} "
    f"host={config['dest_db_host']} "
    f"port={config['dest_db_port']}"
)