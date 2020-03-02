from flask import Flask, jsonify, abort, make_response
from flask_restful import Api, Resource, reqparse
from flask_httpauth import HTTPBasicAuth
from ProcessData import DataForMultipleMonths
from Database import Database

app = Flask(__name__)
api = Api(app)
auth = HTTPBasicAuth()


@auth.get_password
def get_password(username):
    """
    Method used to authenticate any request made to the API. In the future should many users be added may be best to
    store users and passwords on another secure database.

    :param username: username provided by the request made to the API
    :return: If the username provided matches the name we have, then return the password to be compared to the password
    given by the request. Note this isn't returning the password to where the request was made from, the comparison of
    passwords in done internally by the HTTPBasicAuth library. If username not matched then None returned.
    """
    if username == 'your_chosen_username':
        return 'your_chosen_password'
    return None


@auth.error_handler
def unauthorized():
    """
    Method that handles a request with incorrect authorisation details.

    :return: Returns JSON saying access is unauthorised instead of the HTML 401 page to be more API friendly.
    """
    # return 403 instead of 401 to prevent browsers from displaying the default
    # auth dialog
    return make_response(jsonify({'message': 'Unauthorized access'}), 403)


class DataForYearAPI(Resource):
    """
    Class that deals with requests regarding the most recent years (12 months) worth of data. If less than 12 months
    available, will return as much data as it can.
    """

    decorators = [auth.login_required]

    def __init__(self):
        self.months_to_analyse = 12
        super(DataForYearAPI, self).__init__()

    def get(self):
        """
        Method for HTTP GET response. Creates a DataForMultipleMonths object in order to get the most recent 12 months
        worth of data.

        :return: Converts a pandas dataframe containing the data to JSON format and returns that as a response.
        """
        recent_years_data = DataForMultipleMonths()
        database = Database()
        database.create_table()
        recent_years_data.reset_latest_available_data()

        for month_number in range(self.months_to_analyse):
            recent_years_data.main(month_number)

        return recent_years_data.final_dataframe.to_json()


class DataForSpecifiedTimeAPI(Resource):
    """
    Class that deals with requests regarding a specific number of months worth of data.
    """

    decorators = [auth.login_required]

    def get(self, no_of_months):
        """
        Method for HTTP GET response. Creates a DataForMultipleMonths object in order to get a specified number of
        the most recent months worth of data.

        :param no_of_months: Number of months of data to retrieve, specified at end of URL.
        :return: Converts a pandas dataframe containing the data to JSON format and returns that as a response.
        """
        specified_time_data = DataForMultipleMonths()
        database = Database()
        database.create_table()
        specified_time_data.reset_latest_available_data()

        for month_number in range(no_of_months):
            specified_time_data.main(month_number)

        return specified_time_data.final_dataframe.to_json()


class DataForMonthAPI(Resource):
    """
    Class that deals with requests regarding data from a specific month and year.
    """

    decorators = [auth.login_required]

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('month', type=int)
        self.reqparse.add_argument('year', type=int)
        super(DataForMonthAPI, self).__init__()

    def get(self):
        """
        Method for HTTP GET response. Directly queries the database for the specific month and year. Month and year
        passed as URL parameters.

        :return: Converts a pandas dataframe containing the data to JSON format and returns that as a response if data
        for the given month and year is present. Else returns a JSON message saying no data was found.
        """

        args = self.reqparse.parse_args()
        if args['month'] is None or args['year'] is None:
            abort(400)
        database = Database()
        already_stored, month_dataframe = database.use_database_storage(args['month'], args['year'])
        if already_stored:
            return month_dataframe.to_json()
        return {"message": "No data found for given month and year"}

    def delete(self):
        """
        Method for HTTP DELETE response. Deletes data from the database for a specific month and year. Month and year
        passed as URL parameters.
        """

        args = self.reqparse.parse_args()
        if args['month'] is None or args['year'] is None:
            abort(400)
        database = Database()
        database.delete_specific_month(args['month'], args['years'])


api.add_resource(DataForYearAPI, '/psat/pastyear/', endpoint='year')
api.add_resource(DataForMonthAPI, '/psat/specificmonth/', endpoint='month')
api.add_resource(DataForSpecifiedTimeAPI, '/psat/mostrecentmonths/<int:no_of_months>', endpoint='range')

if __name__ == '__main__':
    app.run(debug=True)
