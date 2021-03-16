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
train_set = data.build_full_trainset()

print("""
    Number of ratings: %d\n
    Number of books: %d\n
    Number of users: %d""" % (len(ratings_df), 
        len(ratings_df['isbn'].unique()), 
        len(ratings_df['id'].unique())))

algo = SVD(biased=False, random_state=777, verbose=True)
# algo = KNNBasic(verbose=True)
print("algo: {}".format(algo))
print("Fitting train_set")
algo.fit(train_set)


print("Getting test predictions")
# selected_user = 171118
selected_user = 500016
predictions = [(isbn, algo.predict(selected_user, isbn).est) \
                for isbn in ratings_df['isbn'].unique()]
predictions_df = pd.DataFrame(predictions, 
    columns=['isbn', 'rating']).sort_values(by="rating", 
    ascending=False).merge(books_df, on='isbn', how='left')    

for i, row, in predictions_df.head(40).iterrows():
    print("{}: {}".format(i + 1, row.title), end="")
    if ~isnan(row.year):
        print(" ({})".format(int(row.year)), end="")
    print("\nby {}".format(row.author))
    print(row.isbn)
    print("{}\n".format(row.rating))


for i, row, in predictions_df.tail(20).iterrows():
    print("{}: {}".format(i + 1, row.title), end="")
    if ~isnan(row.year):
        print(" ({})".format(int(row.year)), end="")
    print("\nby {}".format(row.author))
    print(row.isbn)
    print("{}\n".format(row.rating))