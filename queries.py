from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['test10']
collection = db['spark_aggregated']
collection_raw = db['spark_raw']

collection.create_index([('time'), ('link')])
collection_raw.create_index([('origin'), ('destination')])

# Define the time range
time1 = datetime(2024, 9, 19, 0, 0, 0)  # start time definition
time2 = datetime(2024, 9, 19, 23, 59, 59)  # end time definition

# Query 1: Perform the aggregation
query1 = [
    {"$match": {"time": {"$gte": time1, "$lte": time2}}},
    {"$group": {
        "_id": "$link",
        "total_vcount": {"$sum": "$vcount"}
    }},
    {"$sort": {"total_vcount": 1}},
    {"$limit": 1}
]

result = list(collection.aggregate(query1))
print("Query 1 returned: ",result)

# Query2: Perform the aggregation
query2 = [
    {"$match": {"time": {"$gte": time1, "$lte": time2}}},
    {"$group": {
        "_id": "$link",
        "average_vspeed": {"$avg": "$vspeed"}
    }},
    {"$sort": {"average_vspeed": -1}},
    {"$limit": 1}
]

result = list(collection.aggregate(query2))
print("Query 2 returned: ",result)

# Query 3: Perform the aggregation
query3 = [
    # Match documents within the time range
    {"$match": {"time": {"$gte": time1, "$lte": time2}}},
    
    # Group by origin and destination and count links
    {"$group": {
        "_id": {"origin": "$origin", "destination": "$destination"},
        "uniqueLinks": {"$addToSet": "$link"}
    }},
    
    # Get the size of uniqueLinks set
    {"$project": {
        "uniqueLinkCount": {"$size": "$uniqueLinks"}
    }},
    
    # Sort by unique link count in descending order
    {"$sort": {"uniqueLinkCount": -1}},
    
    #Limit to the top result
    {"$limit": 1}
    ]

result = list(collection_raw.aggregate(query3))
print("Query 3 returned: ", result)