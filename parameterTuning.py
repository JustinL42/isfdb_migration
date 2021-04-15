from surprise import SVD, KNNBaseline
from surprise import Dataset, Reader, dump
from surprise.model_selection import GridSearchCV
from sqlalchemy import create_engine
import pandas as pd
import pickle
from customFolds import JumpStartKFolds

print("Connecting to db and loading data into memory...")
alchemyEngine = create_engine(
    'postgresql+psycopg2://postgres:@127.0.0.1/rec_system')
conn = alchemyEngine.connect()
small_df = pd.read_sql("""
    SELECT id, isbn, rating 
    FROM "ratings"
    WHERE id > 500000
    AND id < 600000
    """, conn)
large_df = pd.read_sql("""
    SELECT id, isbn, rating 
    FROM "ratings"
    WHERE id < 500000
    """, conn)
conn.close();

reader = Reader(rating_scale=(1, 10))
small_data = Dataset.load_from_df(small_df, reader)
large_data = Dataset.load_from_df(large_df, reader)

print("Grid search...")
param_grid = {
	'random_state': [777],
	'biased': [True], 
	'n_factors': [40],
	'n_epochs': [50], 

	'lr_all': [0.001, 0.004, 0.005, 0.006],
	# 'lr_all': [ 0.005],

	'lr_pu': [0.0007, 0.0009,  0.0011],
	# 'lr_pu': [0.0009],

	'lr_bu': [0.005, 0.006, 0.007],
	# 'lr_bu': [0.006],

	'lr_bi': [0.005, 0.01, 0.02],
	# 'lr_bi': [0.01],

	'lr_qi': [0.1, 0.3,  0.5],
	# 'lr_qi': [0.3],


	# 'reg_all': [0.2, 0.3, 0.4],
	# 'reg_all': [0.4],

	# 'reg_bu': [0.05, 0.10, 0.13],
	'reg_bu': [0.001, 0.1, .5],

	# 'reg_bi': [0.20, 0.30, 0.5],
	'reg_bi': [0.03, 0.5, 0.7],
	
	# 'reg_pu': [0.35, 0.40, 0.50],
	'reg_pu': [0.001, 0.01, 0.05],

	# 'reg_qi': [0.30, 0.40, 0.7]
	'reg_qi': [0.03, 0.5, 0.7]

	
	}

# param_grid = {'n_epochs': [30], 'lr_all': [0.012],
#               'reg_all': [ 0.20], 'biased': [True, False]}
algo = SVD
# algo = KNNBasic
gs = GridSearchCV(algo, param_grid, measures=['rmse', 'mae'], 
					# cv=3, n_jobs = -2)
					cv=JumpStartKFolds(
						large_data=large_data, n_splits=3, 
						random_state=3, shuffle=True), 
					n_jobs = -2,
					refit=True)

gs.fit(small_data)

best_rmse = gs.best_score['rmse']
print("\nBest RMSE score:\n{}".format(best_rmse))
print(algo.__name__)
print(gs.best_params['rmse'])

try:
	rmse_to_beat = pickle.load(open("rmse_to_beat.pickle", "rb"))
except FileNotFoundError: 
	rmse_to_beat = 99999

if best_rmse <= rmse_to_beat:
	print("A new record")
	dump.dump("bestAlgo.pickle", algo=algo)

	pickle.dump(best_rmse, open("rmse_to_beat.pickle", "wb"))
	logFile = open("rmse_records.txt", "a")
	logFile.write("\n{}\nMAE: {}\nRMSE: {}\n{}\n".format(
		algo.__name__,
		gs.best_params['mae'],
		gs.best_params['rmse'],
		best_rmse))
	logFile.close()
