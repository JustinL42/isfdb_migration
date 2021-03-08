import pandas as pd
from sqlalchemy import create_engine

user_id = 500016

alchemyEngine = create_engine(
    'postgresql+psycopg2://postgres:@127.0.0.1/rec_system');
dbConnection = alchemyEngine.connect();

sql_query = """
    select * 
    from \"users\" 
    where id = %(id)s"""
user_df = pd.read_sql(sql_query, dbConnection, 
    params={'id': user_id});

sql_query = """
    select count(*) as num_ratings, avg(rating) from \"ratings\" 
    where id = %(id)s """
summary_df = pd.read_sql(sql_query, dbConnection, 
    params={'id': user_id});

num_ratings = float(summary_df['num_ratings'].iloc[0])
sql_query = """
    select rating::int, count(*) as count, 
        count(*)/%(num_ratings)s*100 as percent
    from \"ratings\" 
    where id = %(id)s 
    group by rating
    order by rating"""
ratings_count_df = pd.read_sql(sql_query, dbConnection, 
    params={'id': user_id, 'num_ratings': num_ratings});


sql_query = """
    select ratings.isbn, title, year, author, rating
    from \"ratings\" 
    inner join books on ratings.isbn = books.isbn
    where id = %(id)s
    order by rating DESC"""
ratings_df = pd.read_sql(sql_query, dbConnection, 
    params={'id': user_id, 'num_ratings': num_ratings});

dbConnection.close();

print(user_df, end="\n\n")
print(summary_df, end="\n\n")
print(ratings_count_df, end="\n\n")


print("User has {} rating(s) for unknown books".format( \
    int(num_ratings - ratings_df.shape[0])), end="\n\n")

print("Top 5:")
for i, row, in ratings_df.head(5).iterrows():
    print("{}: {} ".format(i + 1, row.title))
    print("\tby {}. ".format(row.author), end='')
    if row.year > 0:
        print("{}. ".format(int(row.year)), end='')
    print("{} {}\n".format(row.isbn, row.rating))

remaining_ratings = ratings_df.shape[0] - 5

if remaining_ratings > 0:
    bottom_x = min(5, remaining_ratings)
    print("Bottom {}:".format(bottom_x))
    for i, row, in ratings_df.tail(bottom_x).iterrows():
        print("{}: {} ".format(i + 1, row.title))
        print("\tby {}. ".format(row.author), end='')
        if row.year > 0:
            print("{}. ".format(int(row.year)), end='')
        print("{} {}\n".format(row.isbn, row.rating))



