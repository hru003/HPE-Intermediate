# Build a simple application to test the working of Flask
# The service can be called on the open web, using the public IP of the EC2 instance
# In order to access the function, you’ll need to enable access on port 5000

# The application receives a text and returns the text in Uppercase

# Loading the Flask library 
import flask

# Create a Flask object using the name special variable
app = flask.Flask(__name__)

# The function is hosted at “/” and accessible by HTTP GET and POST commands.
@app.route("/", methods=["GET","POST"])

# Define a predict function to convert the text to upper case.
def predict():
        data = {"success": False}
        # check for passed in parameters
        params = flask.request.json
        if params is None:
                params = flask.request.args
        # if parameters are found, echo the msg parameter
        if "msg" in params.keys():
                data["response"] = params.get("msg").upper()
                data["success"] = True
        return flask.jsonify(data)

# The application should run using 0.0.0.0 as the host, 
# which enables remote machines to access the application
if __name__ == '__main__':
        app.run(host='0.0.0.0')

# Upon running the code visit the app at <IP:5000>
# The result will be {"response":null,"success":false}, which indicates 
# that the service was called but that no message was provided to the echo service.