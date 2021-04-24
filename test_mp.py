from multiprocessing import Pool, cpu_count, Value, get_logger
import psycopg2
import logging
from multiprocessing_logging import install_mp_handler
from datetime import datetime

db_conn_string = "user=postgres"
N_PROC = -2

def f(x):
    logger.info("In the f funtion")
    logger.info("Processing: {}".format(x))
    squared =  x*x
    try:
        if x == 4:
            logger.warning("4 is not allowed!")
            raise Exception("NO!")
        dest_conn = psycopg2.connect(db_conn_string)
        with dest_conn:
            with dest_conn.cursor() as dest_cur:
                dest_cur.execute("""
                    INSERT INTO squares 
                    (x, square) 
                    VALUES (%s, %s);
                    """, (x, squared)
                )
        with titles_added.get_lock():
            titles_added.value += 1
    except:
        logger.exception("oops! {}".format(x))
        with titles_errored.get_lock():
            titles_errored.value += 1
    finally:
        logger.info("Finally: {}".format(x))
        try:
            dest_conn.close()
        except UnboundLocalError:
            pass


    return squared

if __name__ == '__main__':

    start = datetime.now()
    log_path = "/tmp/" + str(start) + ".log"
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logger = logging.getLogger("test_mp")
    logger.info("{}\tStarting Logging".format(str(start)))

    if N_PROC > 0:
        pool_size = N_PROC
    elif N_PROC < -1:
        pool_size = cpu_count() + N_PROC + 1
    else:
        pool_size = cpu_count()

    
    dest_conn = psycopg2.connect(db_conn_string)
    dest_conn.autocommit = True
    with dest_conn:
        with dest_conn.cursor() as dest_cur:
            dest_cur.execute("""
                DROP table IF EXISTS squares;"""
            )
            dest_cur.execute("""
                    CREATE table squares (
                        id      serial PRIMARY KEY,
                        x       integer,
                        square  integer
                    );"""
            )
    dest_conn.close()


    titles_added = Value('i', 0)
    titles_errored = Value('i', 0)

    with Pool(pool_size) as p:
        p.map(f, range(100))

    print("Titles added: {}".format(titles_added.value))
    print("Titles errored: {}".format(titles_errored.value))

    dest_conn = psycopg2.connect(db_conn_string)
    with dest_conn:
        with dest_conn.cursor() as dest_cur:
            dest_cur.execute("""
                SELECT count(*)
                FROM squares;
                """
            )
            print("Titles in db: {}".format(dest_cur.fetchall()[0][0]))
    dest_conn.close()