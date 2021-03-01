import pandas as pd
from sqlalchemy import create_engine
from surprise import Dataset, Reader
from surprise import SVD, NMF
from surprise.model_selection import cross_validate, train_test_split
from numpy import NaN

print("Connecting to db and querying ratings data...")
alchemyEngine = create_engine(
    'postgresql+psycopg2://postgres:@127.0.0.1/rec_system');
dbConnection = alchemyEngine.connect();
df = pd.read_sql("select id, isbn, rating from \"ratings\"", dbConnection);
df_books = pd.read_sql("select isbn, title from \"books\"", dbConnection);
dbConnection.close();

print("Loading data into memory...")
reader = Reader(rating_scale=(1, 10))
data = Dataset.load_from_df(df, reader)

print("""
    Number of ratings: %d\n
    Number of books: %d\n
    Number of users: %d""" % (len(df), 
        len(df['isbn'].unique()), 
        len(df['id'].unique())))


print("SVD: crossvalidating...")
model_svd = SVD()
cv_results_svd = cross_validate(model_svd, data, cv=3)
pd.DataFrame(cv_results_svd).mean()

print("Fitting model...")
model = SVD(n_factors=80, n_epochs=20, lr_all=0.005, reg_all=0.2)
model.fit(data.build_full_trainset())

print("getting test predictions")

selected_id = 500016
predictions = [(x, model.predict(selected_id, x).est) for x in df['isbn'].unique()]
predictions_df = pd.DataFrame(predictions, 
    columns=['isbn', 'rating']).sort_values(by="rating", 
    ascending=False).merge(df_books, on='isbn', how='left')    
print(predictions_df)








