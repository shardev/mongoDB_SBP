import os
import pandas as pd
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
dataPath = '../../immo_data.csv'

# Load data
data1 = pd.read_csv(dataPath).head(10)

# Create db in mongo
mydb = myclient["germanyRent_v1"]
# Create rent collection
rentsCollection = mydb["rents"]
rents = [] 

# Load rows from dataset into dictionaries
for _, row in data1.iterrows():
    singleRent = {}
    for columnName in data1.columns: singleRent[columnName] = row[columnName]
    rents.append(singleRent)

insertedRows = rentsCollection.insert_many(rents)
print("Inserted ids into rents collection: ")
print(insertedRows.inserted_ids)

print("Mongo handles: ")
print(myclient.list_database_names())
print("Newly created db has this collections: ")
print(mydb.list_collection_names())
