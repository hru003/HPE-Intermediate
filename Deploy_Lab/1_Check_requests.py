# Check the web requests in Python, using Cat Facts Heroku app
# The Cat Facts service provides a simple API that provides
# a JSON response containing interesting tidbits about felines. We
# use the /facts/random endpoint to retrieve a random fact using
# the requests library

#loads the requests library
import requests

# Use the get function to perform an HTTP get for the passed in URL
result = requests.get("http://cat-fact.herokuapp.com/facts/random")

# Result is a response object that provides a response code
print(result)

# Process the payload using the json function to 
# get the payload as a Python dictionary 
print(result.json())

# Display the value for the text key in the returned dictionary object
print(result.json()['text'])