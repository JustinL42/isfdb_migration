from surprise import SVD, KNNBaseline
from surprise import Dataset, Reader
from surprise.model_selection import GridSearchCV
from sqlalchemy import create_engine
import pandas as pd
import pickle

print("Connecting to db and loading data into memory...")
alchemyEngine = create_engine(
    'postgresql+psycopg2://postgres:@127.0.0.1/rec_system');
conn = alchemyEngine.connect();
ratings_df = pd.read_sql("""
    SELECT id, isbn, rating 
    FROM "ratings"
    """, conn);
conn.close();

reader = Reader(rating_scale=(1, 10))
data = Dataset.load_from_df(ratings_df, reader)

print("Grid search...")
param_grid = {
	'random_state': [777],
	'biased': [True], 
	'n_factors': [30],
	'n_epochs': [40], 

	# 'lr_all': [0.0115, 0.0117, 0.0120],

	# 'lr_bu': [0.0120, 0.0500, 0.1000],
	'lr_bu': [0.0120],

	# 'lr_bi': [0.0050, 0.0100,  0.0115],
	'lr_bi': [0.0050],

	# 'lr_pu': [0.0050, 0.0100,  0.0115],
	'lr_pu': [0.0115],

	# 'lr_qi': [0.0050, 0.0100,  0.0115],
	'lr_qi': [0.0050],


	# 'reg_all': [0.10, 0.21, 0.30],
	
	# 'reg_bu': [0.10],
	'reg_bu': [0.05, 0.10, 0.13],
	
	# 'reg_bi': [0.30],
	'reg_bi': [0.20, 0.30, 0.5],
	
	# 'reg_pu': [0.30],
	'reg_pu': [0.35, 0.40, 0.50],
	
	# 'reg_qi': [0.30]
	'reg_qi': [0.30, 0.40, 0.7]
	
	}

# param_grid = {'n_epochs': [30], 'lr_all': [0.012],
#               'reg_all': [ 0.20], 'biased': [True, False]}
algo = SVD
# algo = KNNBasic
gs = GridSearchCV(algo, param_grid, measures=['rmse', 'mae'], 
					# cv=3, n_jobs = -2)
					cv=BalancedKFold(
						n_splits=3, random_state=3, shuffle=True), 
					n_jobs = -2)

gs.fit(data)

best_rmse = gs.best_score['rmse']
print("\nBest RMSE score:\n{}".format(best_rmse))
print(algo.__name__)
print(gs.best_params['rmse'])

try:
	rmse_to_beat = pickle.load(open("rmse_to_beat.pickle", "rb"))
except FileNotFoundError: 
	rmse_to_beat = 0

if best_rmse <= rmse_to_beat:
	print("A new record")
	pickle.dump(best_rmse, open("rmse_to_beat.pickle", "wb"))
	logFile = open("rmse_records.txt", "a")
	logFile.write("\n{}\nMAE: {}\nRMSE: {}\n{}\n".format(
		algo.__name__,
		gs.best_params['mae'],
		gs.best_params['rmse'],
		best_rmse))
	logFile.close()
