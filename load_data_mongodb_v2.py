import os
import pandas as pd
import pymongo

JOINING_COLUMN = 'geo_krs'

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
dataPath = '../../immo_data.csv'

# Load data
data1 = pd.read_csv(dataPath)
# Create db in mongo
mydb = myclient["germanyRent_v2"]
# Create rent collection
rentsCollection = mydb["rents"]
rents = [] 
summedRentsPerCity = {}

# Initialize summ for each city
for c in data1.geo_krs.unique():
    summedRentsPerCity[c] = [0,0] # first 0 is summ, second is counter; this can be expanded for another properties

# Load rows from dataset into dictionaries
for _, row in data1.iterrows():
    singleRent = {}
    booleanGroup = {}
    for columnName in data1.columns:
        if columnName == JOINING_COLUMN:
            summedRentsPerCity[row[JOINING_COLUMN]][0] += row['baseRent']
            summedRentsPerCity[row[JOINING_COLUMN]][1] += 1
        
        # Group boolean values in one attribute
        if columnName in ["newlyConst", "balcony", "hasKitchen", "cellar", "lift", "garden"]:
            booleanGroup[columnName] = row[columnName]
        else: 
            singleRent[columnName] = row[columnName]
    
    singleRent["has"] = booleanGroup
    rents.append(singleRent)

insertedRows = rentsCollection.insert_many(rents)
print("Inserted ids into rents collection: ")
print(insertedRows.inserted_ids)

# Create cities collection
citiesCollection = mydb["cities"]

aggrs = []
for key, value in summedRentsPerCity.items():
    singleAggr = {}
    singleAggr["city"] = key
    singleAggr["rentSum"] = value[0]
    singleAggr["unitCounter"] = value[1]

    aggrs.append(singleAggr)

insertedRows = citiesCollection.insert_many(aggrs)
print("Inserted ids into cities collection: ")
print(insertedRows.inserted_ids)

print("Mongo handles: ")
print(myclient.list_database_names())
print("Newly created db has this collections: ")
print(mydb.list_collection_names())
