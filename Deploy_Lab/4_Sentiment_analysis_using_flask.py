# Import the Pyspark packages
import pyspark
import findspark

findspark.init("/opt/spark")

from pyspark.sql import SparkSession

# Initialize the Spark Session
sc = SparkSession \
    .builder \
    .appName("sentiment-flask") \
    .master('local[2]') \
    .getOrCreate()

# Retrieve the Registered model using ML FLow
import mlflow.pyfunc
model = mlflow.spark.load_model("/home/user1/mlops/mlruns/0/3c5f5e05a5ea4494af0d0737abdaba5a/artifacts/model")


# Modify the simple Flask application to make predictions using the trained model
import flask
app = flask.Flask(__name__)
@app.route("/", methods=["GET","POST"])

def predict():
        data = {"success": False}
        # check for passed in parameters
        params = flask.request.json
        if params is None:
                params = flask.request.args
        # if parameters are found, echo the msg parameter
        if "msg" in params.keys():
                ip  =  params.get("msg")
                test = sc.createDataFrame([(81, ip)], ['id', 'text'])
                prediction = model.transform(test)
                data["response"] = prediction.select("prediction").toJSON().first()
                data["success"] = True
        return flask.jsonify(data)

if __name__ == '__main__':
        app.run(host='0.0.0.0')

#sc.stop()

