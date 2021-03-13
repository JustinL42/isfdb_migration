#!/usr/bin/env python3

import sys
import pandas as pd
from sqlalchemy import create_engine


def main(args):

    if args:
        isbn = args[0]
    else:
        isbn = '0441569579'

    alchemyEngine = create_engine(
        'postgresql+psycopg2://postgres:@127.0.0.1/rec_system');
    dbConnection = alchemyEngine.connect();

    sql_query = """
        select isbn, title, author, year 
        from \"books\" 
        where isbn = %(isbn)s
        limit 1"""
    book_df = pd.read_sql(sql_query, dbConnection, 
        params={'isbn': isbn});

    sql_query = """
        select count(*) as num_ratings, avg(rating) from \"ratings\" 
        where isbn = %(isbn)s """
    summary_df = pd.read_sql(sql_query, dbConnection, 
        params={'isbn': isbn});

    num_ratings = float(summary_df['num_ratings'].iloc[0])
    sql_query = """
        select rating, count(*) as count, 
            count(*)/%(num_ratings)s*100 as percent
        from \"ratings\" 
        where isbn = %(isbn)s 
        group by rating
        order by rating"""
    ratings_count_df = pd.read_sql(sql_query, dbConnection, 
        params={'isbn': isbn, 'num_ratings': num_ratings});


    sql_query = """
        select id, rating
        from \"ratings\" 
        where isbn = %(isbn)s
        order by rating DESC"""
    ratings_df = pd.read_sql(sql_query, dbConnection, 
        params={'isbn': isbn, 'num_ratings': num_ratings});

    dbConnection.close();

    book = book_df.iloc[0]
    print(book.title)
    print("by {}. ".format(book.author), end='')
    if book.year > 0:
        print("{}. ".format(int(book.year)), end='')
    print(book.isbn, end="\n\n")

    print(summary_df, end="\n\n")
    print(ratings_count_df)

    print("\nTop 5 users:")
    for i, row, in ratings_df.head(5).iterrows():
        print("{}: {}\t{}".format(i + 1, 
            int(row.id), row.rating))

    remaining_ratings = ratings_df.shape[0] - 5

    if remaining_ratings > 0:
        bottom_x = min(5, remaining_ratings)
        print("\nBottom {}:".format(bottom_x))
        for i, row, in ratings_df.tail(bottom_x).iterrows():
            print("{}: {}\t{}".format(i + 1, 
                int(row.id), row.rating))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))


