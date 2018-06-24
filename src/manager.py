"""
    Management Application

    1. Reads MongoDB for all applications and related streetlights
    2. If a new application/streetlight is registered at runtime, 
        queries mongodb and instantiates it here.
    3. Ensures streetlight is following expected behaviour by polling sensors

"""
import zmq
import threading
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import json
import sys
import base64
import time


username = "admin"
password = "password"

protosDir = "./protos/"
sys.path.insert(0, protosDir)
import actuated_pb2
import sensed_pb2

appDict = {}

txStruct = {}
rxStruct = {}

SENSOR_POLL_PERIOD = 1 # in minutes
AVERAGING_LENGTH = 10

with open("./api_structs/txStruct.json") as f:
    txStruct = json.load(f)

with open("./api_structs/authStruct.json") as f:
    authStruct = json.load(f)

middlewareURL = "https://gateways.rbccps.org:8080/"
api = "api/nodes/"
getData = "frames?limit=" + str(AVERAGING_LENGTH)
postData = "queue"
auth = "api/internal/login"
jwt = ""
grpc = {}

configuration = actuated_pb2.targetConfigurations()
sensors = sensed_pb2.sensor_values()


scheduler = BackgroundScheduler()
scheduler.add_jobstore('mongodb', collection='application_jobs')


client = MongoClient('localhost', 27017)
db = client.streetlightsDB
streetlights = db.streetlights


def getJWT(username, password):
    global grpc
    print("Auth starting ...")
    authStruct["username"] = username
    authStruct["password"] = password
    try:
        r = requests.post(middlewareURL+auth, json=authStruct, verify=False)
        if(r.status_code == 200):
            jwt = r.json()["jwt"]
            grpc = {"Grpc-Metadata-Authorization": jwt}
            print("Auth done")
        else:
            print("Authorization Failed")
    except Exception as e:
        print("Failed to get token")
        print(e)


def setBrightness(lights, brightness):
    # Populate with API to set Brightness in manual mode
    global grpc
    global configuration
    tempUrl = ""
    configuration.targetManualControlParams.targetBrightnessLevel = brightness
    txStruct["fPort"] = 1  # Read from registration info later
    txStruct["data"] = (base64.b64encode(
        configuration.SerializeToString())).decode('utf-8')
    txStruct["confirmed"] = False
    for light in lights.keys():
        tempUrl = middlewareURL + api + light + "/" + postData
        txStruct["devEUI"] = light
        txStruct["reference"] = light
        try:
            r = requests.post(tempUrl, data=json.dumps(
                txStruct), headers=grpc, verify=False)
        except Exception as e:
            print("Failed to send request for", light)
            print(e)

def getSensors(lights):

    global grpc
    global sensors
    tempUrl = ""
    for light in lights.keys():
        tempUrl = middlewareURL + api + light + "/" + getData
        try:
            r = requests.get(tempUrl, headers=grpc, verify=False).json()
            for i in range (AVERAGING_LENGTH-1,0,-1):
                phyPayload = json.loads(r["result"][i]["phyPayloadJSON"])
                if(phyPayload["mhdr"]["mType"] == "UnconfirmedDataUp"):
                    msg = phyPayload["macPayload"]["frmPayload"][0]["bytes"]
                    print(msg)
                    decodedData = base64.b64decode(msg)
                    sensors.ParseFromString(decodedData)
                    lights[light]["currTemp"] = sensors.caseTemperature
                    lights[light]["currPower"] = sensors.powerConsumption
                    lights[light]["currLux"] = sensors.luxOutput
                    print(lights[light])
        except Exception as e:
            print("Failed to get sensor data for", light)
            print(e)


class Application:

    def __init__(self, params):
        global scheduler
        self.policy = params["Policy"]
        self.appName = params["AppName"]
        self.lights = {}
        for light in params["Lights"]:
            self.lights[light] = {}
            self.lights[light]["currTemp"] = 0.0
            self.lights[light]["currLedLux"] = 0.0
            self.lights[light]["currPower"] = 0.0
            self.lights[light]["isActive"] = False 
        
        if "AutoTimer" in self.policy:
            for setPoint in self.policy["AutoTimer"]:
                scheduler.add_job(setBrightness, 'cron', hour=setPoint["hour"],
                                  minute=setPoint["minute"], args=[self.lights, setPoint["brightness"]])

                #scheduler.add_job(getSensors, 'interval', minutes=SENSOR_POLL_PERIOD, args=[self.lights])


    def deleteApp(self):
        streetlights.delete_one({"AppName": self.appName})

    def addLight(self, light):
        self.policy["Lights"].append(light)


def server():

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, 'streetlights')
    socket.bind("tcp://*:%s" % 4725)
    while True:
        topic, action, message = socket.recv().decode('UTF-8').split('$')
        msgJson = json.loads(message)
        print("Received request  %s" % topic)
        if "newApp" in action:
            appDict[msgJson["AppName"]] = Application(msgJson)
        if "newLight" in action:
            if msgJson["AppName"] in appDict:
                appDict[msgJson["AppName"]].addLight(msgJson["Light"])


if __name__ == '__main__':

    serverThread = threading.Thread(target=server)
    serverThread.start()
    getJWT(username, password)
    scheduler.remove_all_jobs()

    allApps = streetlights.find()
    for app in allApps:
        appDict[app["AppName"]] = Application(app)

    scheduler.start()
    while(True):
        time.sleep(1)
