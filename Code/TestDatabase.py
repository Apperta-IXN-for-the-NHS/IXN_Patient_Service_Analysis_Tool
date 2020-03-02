import unittest
import Database as Database
import pandas as pd


class DatabaseTest(unittest.TestCase):

    def setUp(self):
        """
        Creates a Database object before every test to be used in the tests.
        """
        self.database = Database.Database()

    def test_connection_to_database_can_be_made(self):
        """
        Checks to see if a connection to the database can be made. If a connection can't be made then all other
        tests will fail as they require a connection to the database in order to interact with it.
        """
        db_connection = Database.connect_to_database()
        self.assertNotEqual(db_connection, None)
        db_connection.close()

    def test_database_functions(self):
        """
        Test acts as a controller calling other helper methods to test specific database interactions in a specified
        order.
        """

        self.insert_data()
        self.select_data()
        self.delete_month()
        self.delete_year()

    def insert_data(self):
        """
        Checks to see if data can be correctly inserted into the database. Value for year is deliberately
        set to 999 as to not interfere with real data in the database and to be easier to uniquely identify
        and remove in later tests. If no errors thrown then data was successfully inserted into the database.
        """

        data = [["TestClinicOne", "Likely", "service was good", "Positive", 1, 999, 0.9352],
                ["TestClinicOne", "Unlikely", "service was bad", "Negative", 4, 999, 0.2364],
                ["TestClinicOne", "Unlikely", "nurse couldn't find vein for blood test", "Negative", 4, 999, 0.1344]]

        df = pd.DataFrame(data)

        self.database.insert_data(df)

    def select_data(self):
        """
        Checks to see if data inserted into database can be retrieved correctly. Checks to confirm that the data required
        is already stored in the database and the dataframes returned are not empty.
        """

        already_stored_one, month_dataframe_one = self.database.use_database_storage(1, 999)
        already_stored_two, month_dataframe_two = self.database.use_database_storage(4, 999)
        self.assertTrue(already_stored_one)
        self.assertTrue(already_stored_two)
        self.assertFalse(month_dataframe_one.empty)
        self.assertFalse(month_dataframe_two.empty)

    def delete_month(self):
        """
        Checks to see that data can be deleted from the database correctly. Deletes data for a specific month and
        then checks to see that the deleted data can no longer be found in the database and as a result no dataframe
        is returned. Also ensures the other data remains in the database unaffected.
        """

        self.database.delete_specific_month(1, 999)
        already_stored_one, month_dataframe_one = self.database.use_database_storage(1, 999)
        already_stored_two, month_dataframe_two = self.database.use_database_storage(4, 999)

        self.assertFalse(already_stored_one)
        self.assertTrue(already_stored_two)
        self.assertEqual(month_dataframe_one, None)
        self.assertFalse(month_dataframe_two.empty)

    def delete_year(self):
        """
        Checks to see that data can be deleted from the database correctly. Deletes data for an entire year and
        then checks to see that the deleted data can no longer be found in the database and as a result no dataframe
        is returned.
        """

        self.database.delete_specific_year(999)
        already_stored_one, month_dataframe_one = self.database.use_database_storage(1, 999)
        already_stored_two, month_dataframe_two = self.database.use_database_storage(4, 999)

        self.assertFalse(already_stored_one)
        self.assertFalse(already_stored_two)
        self.assertEqual(month_dataframe_one, None)
        self.assertEqual(month_dataframe_two, None)

    def test_insert_with_invalid_parameter(self):
        """
        Checks to make sure that should an invalid parameter be given as the content to insert into the database, an
        error is thrown, ensuring the integrity of the database.
        """

        with self.assertRaises(RuntimeError):
            self.database.insert_data(None)

        with self.assertRaises(RuntimeError):
            self.database.insert_data("not a dataframe")

        with self.assertRaises(RuntimeError):
            self.database.insert_data(123)

        with self.assertRaises(RuntimeError):
            self.database.insert_data(12.34)

        with self.assertRaises(RuntimeError):
            self.database.insert_data(True)
