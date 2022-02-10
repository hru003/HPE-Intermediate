import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output

import pyspark
import findspark

findspark.init("/opt/spark")

from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("sentiment-flask") \
    .master('local[2]') \
    .getOrCreate()

import mlflow.pyfunc
model = mlflow.spark.load_model("/home/user1/mlops/mlruns/0/3c5f5e05a5ea4494af0d0737abdaba5a/artifacts/model")

#Define the Layout for the input and output
app = dash.Dash(__name__)
app.layout = html.Div(children=[
    html.H1(children='Sentiment Model UI'),
    html.P([
        html.Label('Text Input'),
        dcc.Input(value='Text for sentiment', type='text', id='ip'),
    ]),

    html.P([
        html.Label('Prediction '),
        dcc.Input(value='0=pos 1=neg', type='text', id='pred')
    ]),
])

# Associate the components in the layout as either inputs or outputs appropriately.
@app.callback(
    Output(component_id='pred', component_property='value'),
    [Input(component_id='ip', component_property='value')]
)

# Generate the predictions for the input and display in the interactive web service.
def update_prediction(ip):
    test = sc.createDataFrame([(81, ip)], ['id', 'text'])
    prediction = model.transform(test)
    print(prediction.select("prediction").toJSON().first())
    return str(prediction.select("prediction").toJSON().first())

if __name__ == '__main__':
        app.run(host='0.0.0.0')
