import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

"""
NOTE:

This python file does not make up part of the API. Instead this python script should be used as a data source for Power
BI (or any other visualisation tool that you wish to use) in order to make requests to the API and load data into the
visualisation tool.
"""

"""
This is an example request for retrieving the last 12 months of data from Azure where it's stored.
In this example the JSON from the API is then converted into a pandas DataFrame.
The host for this request is currently localhost. Change this depending on what you use to host the API on if you choose
to host it somewhere.
"""
JSONContent = requests.get("http://127.0.0.1:5000/psat/pastyear", auth=HTTPBasicAuth('your_chosen_username', 'your_chosen_password')).json()
feedback = pd.read_json(JSONContent)
print(feedback)

"""
This is an example request to retrieve a specific months worth of data.
In this case it is retrieving data for January 2019.
In this example we are simply printing out the JSON returned to us by the API.
The host for this request is currently localhost. Change this depending on what you use to host the API on if you choose
to host it somewhere.
"""
# JSONContent = requests.get("http://127.0.0.1:5000/psat/specificmonth?month=1&year=19", auth=HTTPBasicAuth('your_chosen_username', 'your_chosen_password')).json()
# feedback = pd.read_json(JSONContent)
# print(feedback)

"""
This is an example request to retrieve the x most recent months worth of data from Azure where x is the number of months
you want to get data for. Replace <int:no_of_months> in the URL with your x.
In this example the JSON from the API is then converted into a pandas DataFrame
The host for this request is currently localhost. Change this depending on what you use to host the API on if you choose
to host it somewhere.
"""
# JSONContent = requests.get("http://127.0.0.1:5000/psat/mostrecentmonths/<int:no_of_months>", auth=HTTPBasicAuth('your_chosen_username', 'your_chosen_password')).json()
# feedback = pd.read_json(JSONContent)
# print(feedback)

"""
This is an example request to delete a specific months worth of data from the database.
The month and year are passed in the URL as parameters.
Before running this know that it takes around 8 to 10 minutes to reanalyse and store the months worth of data that gets
deleted and write it back to the database.
"""
# JSONContent = requests.delete("http://localhost:5000/psat/specificmonth?month=11&year=11", auth=HTTPBasicAuth('your_chosen_username', 'your_chosen_password')).json()
# print(JSONContent.headers)




