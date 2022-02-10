#Use this file to send the text to the sentiment analysis deployed using Flask.

import requests
result = requests.get("http://<IP address>:5000/?msg=I like programming")
print(result.json())