"""

    Server for managing streetlights.

    API's

    RegisterApp :- {"AppName":"App1","MaxStreetlights":10,
                        "Policy":"<AutoTimer>|<Manual>|<AutoLux>", "PolicyParams":<PolicyParams>, "Lights" = [123,234]}

                                
        Policy Params Expansion :- AutoTimer     ["hour":18,"minute":30,"brightness":50]} # Time in 24hours format 
                                   Manual        {"Brightness":100}} # Integer Value less than 100
                                   AutoLux       {"OnThreshold":1,"OffThreshold":2}} # Integer 

    RegisterLight :- {"AppName":"App1","Light":70b3d58ff0031de5}


    appendPolicy :- {"AppName":"App1","Policy": "AutoTimer", "PolicyParams": {[{"hour":12,"minute":4,"brightness":0}]}}

    Hierarchy in MongoDB :- streetlightsDB -> <applicationID> -> <streetlightId>
                            (Collection)
"""
#!/usr/bin/python3

from flask import Flask, request
from flask_restful import Resource, Api, reqparse
import json
import requests
import os
import subprocess as sub
import sys
from shutil import copyfile
import zmq
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.streetlightsDB
streetlights = db.streetlights

app = Flask(__name__)
api = Api(app)

workingDir = sys.path[0]
items = {}

context = zmq.Context()
print("Connecting to socket with ports %s" % 4725)
socket = context.socket(zmq.PUB)
socket.connect("tcp://localhost:%s" % 4725)


class registerApp(Resource):
    def post(self):
        json_data = request.get_json()
        print(json_data)
        appId = streetlights.update(
            {"AppName": json_data["AppName"]}, json_data, upsert=True)

        socket.send_string("streetlights" + '$' + "newApp" + '$' + json.dumps(json_data))

class appendPolicy(Resource):
    def post(self):
        json_data = request.get_json()
        print(json_data)
        if("AutoTimer" in json_data["Policy"]):
            streetlights.update({"AppName": json_data["AppName"]},{"$push":{"PolicyParams":json_data["PolicyParams"][0]}})
        socket.send_string("streetlights" + '$' + "appendPolicy" + '$' + json.dumps(json_data))

class registerLight(Resource):
    def post(self):
        json_data = request.get_json()
        lights = json_data["Lights"]
        appName = json_data["AppName"]
        for light in lights: 
            streetlights.update({"AppName": appName}, {"$push": {"Lights": light}})
        socket.send_string("streetlights" + '$' + "newLight" + '$' + json.dumps(json_data))

# To be filled
#class deletePolicy(Resource):
#    def delete():

api.add_resource(registerApp, '/registerApp')
api.add_resource(registerLight, '/registerLight')
api.add_resource(appendPolicy, '/appendPolicy')
#To be Filled
#api.add_resource(deleteApp, '/deleteApp')
#api.add_resource(deleteLight, '/deleteLight')
#api.add_resource(deletePolicy, '/deletePolicy')

if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1")
