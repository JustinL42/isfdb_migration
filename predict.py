import pandas as pd
from sqlalchemy import create_engine
from surprise import Dataset, Reader, SVD, NMF, KNNBasic, accuracy
from surprise.model_selection import cross_validate, train_test_split, GridSearchCV
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

algo = SVD(n_factors=80, n_epochs=20, lr_all=0.005, reg_all=0.2)
# algo = KNNBasic()
# algo = NMF()
print("algo: {}".format(algo))
print("Cross validating...")
cross_validate(algo, data, measures=['RMSE', 'MAE'], cv=3, verbose=True)

print("Getting test predictions")
selected_user = str(500016)
predictions = [(isbn, algo.predict(selected_user, isbn).est) \
                for isbn in ratings_df['isbn'].unique()]
predictions_df = pd.DataFrame(predictions, 
    columns=['isbn', 'rating']).sort_values(by="rating", 
    ascending=False).merge(books_df, on='isbn', how='left')    

for i, row, in predictions_df.head(10).iterrows():
    print("{}: {}".format(i + 1, row.title), end="")
    if ~isnan(row.year):
        print(" ({})".format(int(row.year)), end="")
    print("\nby {}".format(row.author))
    print(row.isbn)
    print("{}\n".format(row.rating))