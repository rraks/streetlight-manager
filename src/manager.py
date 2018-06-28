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
from MQTTPubSub import MQTTPubSub
from influxdb import InfluxDBClient


#Proto Info
protosDir = "./protos/"
sys.path.insert(0, protosDir)
import actuated_pb2
import sensed_pb2

# Global datastructs
appDict = {}
txStruct = {}
rxStruct = {}

# Statistical Params
SENSOR_POLL_PERIOD = 1 # in minutes
AVERAGING_LENGTH = 10

# API Structs
with open("./api_structs/txStruct.json") as f:
    txStruct = json.load(f)
with open("./api_structs/authStruct.json") as f:
    authStruct = json.load(f)

# HTTP API's 
middlewareURL = ""
api = "api/nodes/"
# Middleware Credentials
username = "admin"
password = "password"
getData = "frames?limit=" + str(AVERAGING_LENGTH)
postData = "queue"
auth = "api/internal/login"
jwt = ""
grpc = {}

# MQTT Info
nsSubTopic = "application/1/node/"
# Default Network Server Params
nsSubParams = {}
nsSubParams["url"] = "127.0.0.1"
nsSubParams["port"] = 1883
nsSubParams["timeout"] = 60
nsSubParams["username"] = "uname"
nsSubParams["password"] = "password"

tsDBUrl = "127.0.0.1"
tsDBPort = 8086
tsDBUname = "root"
tsDBPwd = "root"

# Proto Classes
configuration = actuated_pb2.targetConfigurations()
sensors = sensed_pb2.sensor_values()

# Scheduler Config
scheduler = BackgroundScheduler()
scheduler.add_jobstore('mongodb', collection='application_jobs')

# Meta Data Store
client = MongoClient('localhost', 27017)
db = client.streetlightsDB
streetlights = db.streetlights


# Get Authorization Token for HTTP API's
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


##HTTP API
#def setBrightness(lights, brightness):
#    # Populate with API to set Brightness in manual mode
#    global grpc
#    global configuration
#    tempUrl = ""
#    configuration.targetManualControlParams.targetBrightnessLevel = brightness
#    txStruct["fPort"] = 1  # Read from registration info later
#    txStruct["data"] = (base64.b64encode(
#        configuration.SerializeToString())).decode('utf-8')
#    txStruct["confirmed"] = False
#    for light in lights.keys():
#        tempUrl = middlewareURL + api + light + "/" + postData
#        txStruct["devEUI"] = light
#        txStruct["reference"] = light
#        try:
#            r = requests.post(tempUrl, data=json.dumps(
#                txStruct), headers=grpc, verify=False)
#        except Exception as e:
#            print("Failed to send request for", light)
#            print(e)

#HTTP API
#def getSensors(lights):
#
#    global grpc
#    global sensors
#    tempUrl = ""
#    for light in lights.keys():
#        tempUrl = middlewareURL + api + light + "/" + getData
#        try:
#            r = requests.get(tempUrl, headers=grpc, verify=False).json()
#            for i in range (AVERAGING_LENGTH-1,0,-1):
#                phyPayload = json.loads(r["result"][i]["phyPayloadJSON"])
#                if(phyPayload["mhdr"]["mType"] == "UnconfirmedDataUp"):
#                    msg = phyPayload["macPayload"]["frmPayload"][0]["bytes"]
#                    print(msg)
#                    decodedData = base64.b64decode(msg)
#                    sensors.ParseFromString(decodedData)
#                    lights[light]["currTemp"] = sensors.caseTemperature
#                    lights[light]["currPower"] = sensors.powerConsumption
#                    lights[light]["currLux"] = sensors.luxOutput
#                    print(lights[light])
#        except Exception as e:
#            print("Failed to get sensor data for", light)
#            print(e)

# Default Callback for Scheduler 
def schedCallback(appName, brightness):
    appDict[appName].setBrightness(brightness)

# Application Handler Class
class Application:
    def __init__(self, params, serverDetails):
        global scheduler
        self.appName = params["AppName"]
        self.policy = params["Policy"]
        self.policyParams = []
        self.lights = {}
        self.currBrightness = 0
        self.nsSubParams = {}
        self.nsSubParams = serverDetails
        self.influxClient = InfluxDBClient(tsDBUrl, tsDBPort, tsDBUname, tsDBPwd, self.appName)
        self.influxClient.create_database(self.appName)

        for light in params["Lights"]:
            self.lights[light] = {}
            self.lights[light]["currTemp"] = 0.0
            self.lights[light]["currLedLux"] = 0.0
            self.lights[light]["currPower"] = 0.0
            self.lights[light]["isActive"] = False 

        self.nsSubParams["topic"] = []
        self.nsSubParams["topic"] = nsSubTopic+"+/rx"
        self.nsSubParams["onMessage"] = self.NSSub_onMessage
        self.nsSubParams["onConnect"] = self.NSSub_onConnect
        self.nsSub = MQTTPubSub(nsSubParams)
        self.nsSub_rc = self.nsSub.run()

        if "AutoTimer" in params["Policy"]:
            self.addPolicy(params["Policy"], params["PolicyParams"])


    def deleteApp(self):
        streetlights.delete_one({"AppName": self.appName})

    def addLights(self, lights):
        localLights = {}
        for light in lights:
            localLights[light] = {}
            localLights[light]["currTemp"] = 0.0
            localLights[light]["currLedLux"] = 0.0
            localLights[light]["currPower"] = 0.0
            localLights[light]["isActive"] = False 
            self.setBrightness(self.currBrightness, localLights) # Actuate only new lights to current values
            self.lights[light] = {} 
            self.lights[light] = localLights[light] # Fill in class lights list
        print(self.lights)

    def getLights(self):
        return self.lights

    def addPolicy(self, policy, params):
        if "AutoTimer" in policy:
            self.policyParams.append(params)
            for setPoint in params:
                scheduler.add_job(schedCallback, 'cron', hour=setPoint["hour"],
                                  minute=setPoint["minute"], args=[self.appName, setPoint["brightness"]])

    # MQTT Callback for Sensor uplink from server
    def NSSub_onMessage(self, mqttc, obj, msg):
        global lights
    # NS Message topics are of the form application/{applicationId}/node/{id}/rx
        try:
            try:
                topic = msg.topic.split('/')
                devId = topic[3] #{id} is the 4th field
                print('Received ', devId, ' from NS')
                if devId in self.lights:
                    print(msg.payload)
                    decodedData = base64.b64decode(json.loads(msg.payload.decode("utf-8"))["data"])
                    #decodedData = base64.b64decode(json.loads(msg.payload"))["data"])
                    sensors.ParseFromString(decodedData)
                    print(sensors)
                    self.lights[devId]["currTemp"] = sensors.caseTemperature
                    self.lights[devId]["currLedLux"] = sensors.luxOutput
                    self.lights[devId]["currPower"] = sensors.powerConsumption

                    epochTime = int(time.time()) * 1000000000
                    series = []
                    pointValues = {
                        "time": epochTime,
                        "measurement": "caseTemperature",
                        'fields': {
                            'value': sensors.caseTemperature,
                        },
                        'tags': {
                            "sensorName": "caseTemperature",
                            "deviceId" : devId
                        },
                    }
                    series.append(pointValues)

                    pointValues = {
                        "time": epochTime,
                        "measurement": "powerConsumption",
                        'fields': {
                            'value': sensors.powerConsumption,
                        },
                        'tags': {
                            "sensorName": "powerConsumption",
                            "deviceId" : devId
                        },
                    }
                    series.append(pointValues)

                    pointValues = {
                        "time": epochTime,
                        "measurement": "luxOutput",
                        'fields': {
                            'value': sensors.luxOutput,
                        },
                        'tags': {
                            "sensorName": "luxOutput",
                            "deviceId" : devId
                        },
                    }
                    series.append(pointValues)

                    print(series)
                    self.influxClient.write_points(series, time_precision='n')

            except Exception as e:
                print("Couldn't save", topic)
                print(e)
        except Exception as e:
            print("DECODE ERROR")
            print(e)

    # Brightness Actuation
    def setBrightness(self, brightness, lights=None):
        self.currBrightness = brightness
        configuration.targetManualControlParams.targetBrightnessLevel = brightness
        payload = (base64.b64encode(
            configuration.SerializeToString())).decode('utf-8')
        data = {}
        data['reference'] = 'a'
        data['confirmed'] = False
        data['fport'] = 1
        data['data'] = payload 
        if lights is None:
            for light in self.lights.keys():
                print(light)
                self.nsSub.publish(nsSubTopic+light+"/tx", json.dumps(data))
        else:
            for light in lights.keys():
                self.nsSub.publish(nsSubTopic+light+"/tx", json.dumps(data))


    # MQTT Callback for connection
    def NSSub_onConnect(self, client, userdata, flags, rc):
        print("Connected to NS SUB result code " + str(rc))

# ZMQ Server for IPC for application/light registration 
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
            appDict[msgJson["AppName"]] = Application(msgJson, nsSubParams)
        if "newLight" in action:
            if msgJson["AppName"] in appDict:
                appDict[msgJson["AppName"]].addLights(msgJson["Lights"])
        if "appendPolicy" in action:
            if msgJson["AppName"] in appDict:
                appDict[msgJson["AppName"]].addPolicy(msgJson["Policy"], msgJson["PolicyParams"])


if __name__ == '__main__':

    serverThread = threading.Thread(target=server)
    serverThread.start()
    getJWT(username, password)
    scheduler.remove_all_jobs()

    allApps = streetlights.find()
    for app in allApps:
        appDict[app["AppName"]] = Application(app, nsSubParams)


    scheduler.start()
    while(True):
        time.sleep(1)
