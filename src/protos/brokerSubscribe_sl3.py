#!/usr/bin/env python


import paho.mqtt.client as mqtt
import sys
import json
import requests
import time
import base64
import sensed_pb2





rxmsg =sensed_pb2.sensor_values()




# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("application/1/node/70b3d58ff0031f00/rx")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):

    try :
        decodedData = str(base64.b64decode(json.loads(str(msg.payload))["data"]))
        rxmsg.ParseFromString(decodedData)
        
        epochTime = int(time.time()) * 1000000000

        decodedData = json.loads(msg.payload)["data"]
        series = []
        pointValues = {
            "time": epochTime,
            "measurement": "caseTemperature",
            'fields': {
                'value': rxmsg.caseTemperature,
            },
            'tags': {
                "sensorName": "caseTemperature",
            },
        }
        series.append(pointValues)




        pointValues = {
            "time": epochTime,
            "measurement": "powerConsumption",
            'fields': {
                'value': rxmsg.powerConsumption,
            },
            'tags': {
                "sensorName": "powerConsumption",
            },
        }
        series.append(pointValues)




        pointValues = {
            "time": epochTime,
            "measurement": "luxOutput",
            'fields': {
                'value': rxmsg.luxOutput,
            },
            'tags': {
                "sensorName": "luxOutput",
            },
        }
        series.append(pointValues)



        pointValues = {
            "time": epochTime,
            "measurement": "ambientLux",
            'fields': {
                'value': rxmsg.ambientLux,
            },
            'tags': {
                "sensorName": "ambientLux",
            },
        }
        series.append(pointValues)



        

        
    except Exception as e:
        print(e)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("loraserver","loraserver")

client.connect("127.0.0.1", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()








