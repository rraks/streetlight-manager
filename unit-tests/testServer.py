import requests
import json

authUrl = "https://gateways.rbccps.org:8080/api/internal/login"
authUrl = "https://gateways.rbccps.org:8080/api/node/70b3d58ff0031de5"

txStruct = {
	      "confirmed": True,
	      "data": "012==",
	      "devEUI": "70b3d58ff0031de5",
	      "fPort": 0,
	      "reference": "string"
	    }
authStruct = {"username":"admin","password":"1!Rbccps@123"}

r = requests.post(authUrl,json=authStruct, verify=False)
print(r)
