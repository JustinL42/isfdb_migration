import pandas as pd
from sqlalchemy import create_engine
from surprise import Dataset, Reader
from surprise import SVD, NMF
from surprise.model_selection import cross_validate, train_test_split
from numpy import NaN, isnan

print("Connecting to db and loading data into memory...")
alchemyEngine = create_engine(
    'postgresql+psycopg2://postgres:@127.0.0.1/rec_system');
dbConnection = alchemyEngine.connect();
ratings_df = pd.read_sql("select id, isbn, rating from \"ratings\"", dbConnection);
books_df = pd.read_sql("select isbn, title, author, year from \"books\"", dbConnection);
dbConnection.close();

reader = Reader(rating_scale=(1, 10))
data = Dataset.load_from_df(ratings_df, reader)

print("""
    Number of ratings: %d\n
    Number of books: %d\n
    Number of users: %d""" % (len(ratings_df), 
        len(ratings_df['isbn'].unique()), 
        len(ratings_df['id'].unique())))

print("Fitting model...")
model = SVD(n_factors=80, n_epochs=20, lr_all=0.005, reg_all=0.2)
# model = NMF()
model.fit(data.build_full_trainset())

print("Getting test predictions")

selected_id = 500016
predictions = [(x, model.predict(selected_id, x).est) for x in ratings_df['isbn'].unique()]
predictions_df = pd.DataFrame(predictions, 
    columns=['isbn', 'rating']).sort_values(by="rating", 
    ascending=False).merge(books_df, on='isbn', how='left')    

for i, row, in predictions_df.head(10).iterrows():
    print("{}: {}".format(i + 1, row.title), end="")
    if ~isnan(row.year):
        print(" ({})".format(int(row.year)), end="")
    print("\nby {}".format(row.author))
    print("{}\n".format(row.rating))