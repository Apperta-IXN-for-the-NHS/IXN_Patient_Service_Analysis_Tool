import unittest
import warnings
import ProcessData
import pandas as pd


# Decorator to ignore warnings in specific tests
def suppress_warnings(test_func):
    def run_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_func(self, *args, **kwargs)

    return run_test


class ProcessDataTest(unittest.TestCase):

    @suppress_warnings
    def setUp(self):
        """
        Creates a ProcessData object before every test to be used in the tests.
        """
        self.process_data = ProcessData.DataForMultipleMonths()

    def test_set_blob_names(self):
        """
        Checks to see that names of files generated are in the correct format when given a month and year.
        """
        blob_name_neg, blob_name_pos = self.process_data.set_blob_names(1, 20)
        self.assertEqual(blob_name_pos, "Positive Comments - January 20.xlsx")
        self.assertEqual(blob_name_neg, "Negative Comments - January 20.xlsx")

    def test_populate_pos_neg_lists(self):
        """
        Checks to see that lists of the correct length and contents are generated when a specified parameter is given.
        """
        neg_list, pos_list = self.process_data.populate_pos_neg_lists(3, 5)
        for item in neg_list:
            self.assertEqual(item, "Negative")
        for item in pos_list:
            self.assertEqual(item, "Positive")
        self.assertEqual(len(neg_list), 3)
        self.assertEqual(len(pos_list), 5)

    def test_populate_month_year_lists(self):
        """
        Chekcs to see that lists of correct length and contents are generated when specified parameters are given.
        """
        month_list, year_list = self.process_data.populate_month_year_lists(5, "May", 20)
        for item in month_list:
            self.assertEqual(item, "May")
        for item in year_list:
            self.assertEqual(item, 20)
        self.assertEqual(len(month_list), 5)
        self.assertEqual(len(year_list), 5)

    def test_data_reset(self):
        """
        Checks to see that data is reset back to default values on method call.
        """
        self.process_data.latest_month = "2"
        self.process_data.latest_year = "20"
        self.process_data.reset_latest_available_data()
        self.assertEqual(self.process_data.latest_month, "")
        self.assertEqual(self.process_data.latest_year, "")

    def test_find_latest_data(self):
        """
        Checks to make sure that the month and year for the most recent data available can be calculated and is correct.
        """
        self.populate_blob_data_names()
        self.process_data.find_latest_data()
        self.assertEqual(self.process_data.latest_month, 1)
        self.assertEqual(self.process_data.latest_year, 20)

    def test_find_latest_data_not_recent(self):
        """
        Checks to see if no data present in list of file names then variables representing latest data remain as their
        default values and no error is thrown.
        """
        self.process_data.azure_storage.blob_data_names.clear()
        self.process_data.find_latest_data()
        self.assertEqual(self.process_data.latest_month, "")
        self.assertEqual(self.process_data.latest_year, "")

    def test_find_required_month_latest_month(self):
        """
        Checks to see if the latest month of data can be found and the details of the month and year returned correctly.
        """
        self.populate_blob_data_names()
        file_month, file_year = self.process_data.find_required_month_data(0)
        self.assertEqual(file_month, 1)
        self.assertEqual(file_year, 20)

    def test_find_required_month_two_months_ago(self):
        """
        Checks to see if data 2 months prior to the latest data can be found and the details of the month and year
        returned correctly.
        """
        self.populate_blob_data_names()
        file_month, file_year = self.process_data.find_required_month_data(2)
        self.assertEqual(file_month, 11)
        self.assertEqual(file_year, 19)

    def test_find_required_month_does_not_exist(self):
        """
        Checks to see in the event that data for a required month does not exist in the list of file names,
        no errors are thrown ad None returned
        """
        self.populate_blob_data_names()
        file_month, file_year = self.process_data.find_required_month_data(10)
        self.assertEqual(file_month, None)
        self.assertEqual(file_year, None)

    def test_find_required_months_no_data(self):
        """
        Checks to see that if the latest data is not available, no errors are thrown and None is returned.
        """
        self.process_data.azure_storage.blob_data_names.clear()
        file_month, file_year = self.process_data.find_required_month_data(0)
        self.assertEqual(file_month, None)
        self.assertEqual(file_year, None)

    def test_clean_up_dataframe_none_dropped(self):
        """
        Checks to see that given a dataframe with some missing values but no row made up of entirely missing values, the
        dataframe is unaltered and returned with the same number of rows as before.
        """
        df = pd.DataFrame({"CLINIC": ['Ex1', 'Ex2', 'Ex3'],
                           "COMMENT": [pd.NaT, 'Good', 'Bad'],
                           "RESPONSE": [pd.NaT, "Likely",
                                        pd.NaT]})

        self.process_data.clean_up_dataframe(3, 3, df, df)
        self.assertEqual(len(df.index), 3)

    def test_clean_up_dataframe_one_dropped(self):
        """
        Checks to see that given a dataframe with missing values, including a row made up of missing values, that the
        empty row is removed but other rows are not changed.
        """
        df = pd.DataFrame({"CLINIC": [pd.NaT, 'Ex2', 'Ex3'],
                           "COMMENT": [pd.NaT, 'Good', 'Bad'],
                           "RESPONSE": [pd.NaT, "Likely",
                                        pd.NaT]})

        self.process_data.clean_up_dataframe(3, 3, df, df)
        self.assertEqual(len(df.index), 2)

    def test_clean_up_dataframe_clinic_to_zero(self):
        """
        Checks to see that given a dataframe with CLINIC having missing values, the dataframe returned replaces the
        missing values with a 0 and the row is not removed
        """
        df = pd.DataFrame({"CLINIC": ['Ex1', pd.NaT, 'Ex3'],
                           "COMMENT": [pd.NaT, 'Good', 'Bad'],
                           "RESPONSE": [pd.NaT, "Likely",
                                        pd.NaT]})

        self.process_data.clean_up_dataframe(3, 3, df, df)
        self.assertEqual(len(df.index), 3)
        self.assertEqual(df["CLINIC"][1], 0)

    def populate_blob_data_names(self):
        """
        Helper method to create a list of file names available
        """
        self.process_data.azure_storage.blob_data_names.clear()
        self.process_data.azure_storage.blob_data_names.append("Positive Comments - January 20.xlsx")
        self.process_data.azure_storage.blob_data_names.append("Positive Comments - December 19.xlsx")
        self.process_data.azure_storage.blob_data_names.append("Positive Comments - November 19.xlsx")
