# We showcase the different methods for passing parameters to the web service.

import requests

# The parameters can be appended to the URL
result = requests.get("http://<IP-address>:5000/?msg=HelloWorld!")
print(result.json())

# The parameters can be specified using the params object when using a GET command
result = requests.get("http://<IP-address>:5000/",
params = { 'msg': 'Hello from params' })

print(result.json())

# The parameters can be passed using the json parameter when using a POST command
result = requests.post("http://<IP-address>:5000/",
json = { 'msg': 'Hello from data' })

print(result.json())
