import unittest
import PatientServiceAnalysisTool as PSAT
from PatientServiceAnalysisTool import app


class PSATTest(unittest.TestCase):

    def setUp(self):
        """
        Configures the Flask code to allow it to be tested. Creates a client for the Flask app allowing for HTTP requests
        such as GET to be made.
        """
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        app.config['DEBUG'] = False
        self.client = app.test_client()

    def test_successful_authentication(self):
        """
        Checks to see if given the correct credentials login succeeds
        """
        password = PSAT.get_password("your_chosen_username")
        self.assertEqual(password, "your_chosen_password")

    def test_unsuccessful_authentication(self):
        """
        Checks to see if given the incorrect credentials login fails
        """
        password = PSAT.get_password("wrong username")
        self.assertEqual(password, None)

    def test_data_for_year_unauthorised(self):
        """
        Checks to see that access is denied if a request is made for past years worth of data without credentials
        """
        response = self.client.get("/psat/pastyear/")
        self.assertEqual(response.status_code, 403)

    def test_data_for_month_unauthorised(self):
        """
        Checks to see that access is denied if a request is made for a specific months worth of data without credentials
        """
        response = self.client.get("/psat/specificmonth/")
        self.assertEqual(response.status_code, 403)
