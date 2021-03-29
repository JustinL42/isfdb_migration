#!/usr/bin/python3
import pandas as pd
from surprise import Dataset, Reader, KNNBasic

#########################################
# Configurable Parameters:

# Number of neighbors to try to find
k = 10

# Exclude people who have fewer than the minimum number of ratings
min_ratings = 3

# A higher value on this parameter gives less similarity to pairs of 
# people with few book ratings in common
shrinkage_parameter = 10

#########################################


filename = "ratings by book title and user 2021-03-16.csv"
df = pd.read_csv(filename)

#remove null ratings
df = df[~df.Value.isnull()]

# For duplicates on name, title, and rating, keep only the first one
df = df[~df.duplicated(subset=['Name', 'Title', 'Value'], keep='first')]

# For any duplicates that still exist on Name and Title, the ratings are
# different. These may be different people, but I don't know who to 
# assign the ratings to. Drop these for now.
df = df[~df.duplicated(subset=['Name', 'Title'], keep=False)]

# remove users with less than min_ratings ratings
df = df.groupby('Name').filter(lambda x : len(x) >= min_ratings)

# Fix some inconsistent naming
df.loc[(df.Name =="Mike") | (df.Name == "Michael"), 'Name'] = "Mike N."

# update some of my own ratings
df.loc[(df.Title == "The Cyberiad") &\
	 (df.Name == "Justin"), 'Value'] = 1
df.loc[(df.Title == "The Moon is a Harsh Mistress") &\
	(df.Name == "Justin"), 'Value'] = 0
df.loc[(df.Title == "Trail of Lightning") &\
	(df.Name == "Justin"), 'Value'] = 0	

# Before 1/21/2020, ratings were recorded from -1 to 1
# convert these -1, -0.5, 0, 0.5, 1 ratings to 1-5
conversion = lambda x : (x + 1.5)*2
df.Timestamp = pd.to_datetime(df.Timestamp)
df.loc[df.Timestamp < '1/21/20', 'Value'] = df['Value'].map(conversion)

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df[['Name', 'Title', 'Value']], reader)
train_set = data.build_full_trainset()
algo = KNNBasic(k=k, verbose=True, 
	sim_options={'name': 'pearson_baseline', 'shrinkage': shrinkage_parameter})
algo.fit(train_set)

names = df.Name.sort_values().unique()

for name in names:
	user_inner_id = train_set.to_inner_uid(name)
	neighbors = algo.get_neighbors(user_inner_id, k)
	
	print("\n{}:".format(name))
	print("   {:<12}Similarity (%)\tBooks in Common".format("Name"))
	for i in range(len(neighbors)):
		neighbor_name = train_set.to_raw_uid(neighbors[i])
		similarity = algo.sim[user_inner_id, neighbors[i]]
		if similarity < 0.0001:
			# stop reporting neighbors with neglible similarity
			break

		user_books = set(df[df.Name == name].Title)
		neighbor_books = set(df[df.Name == neighbor_name].Title)
		books_in_common = len(user_books.intersection(neighbor_books))
		print("{}: {:<9}\t{:0.2f}\t\t\t{}".format(i + 1, 
			neighbor_name, similarity * 100, books_in_common))