from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db =  client.streetlightsDB
streetlights = db.streetlights

json_data = {}
json_data["ApplicationName"] = "App1"
json_data["MaxStreetlights"] = 10
json_data["Policy"] = {}
policy_params = {}
policy_params["AutoTimer"] = {"TimeOn":"1800","TimeOff":"0600"}
json_data["Policy"] = policy_params

print(json_data)
def RegisterApplication(json_data):

    streetlights.update({"ApplicationName":json_data["ApplicationName"]}, json_data, upsert=True)


def DeleteApplication(name):

    streetlights.delete_one({"ApplicationName":name})

def FindAll():
    collection = streetlights.find()
    for document in collection:
        print(document)

#RegisterApplication(json_data)
FindAll()
#DeleteApplication("App1")


