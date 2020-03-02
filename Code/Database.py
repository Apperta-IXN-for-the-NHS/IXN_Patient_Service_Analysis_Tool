import mysql.connector
import pandas as pd


def connect_to_database():
    """
    Attempts to connect to the database hosted on azure.
    """
    try:
        db_connection = mysql.connector.connect(user='your_database_username',
                                                password='your_database_password',
                                                database='your_database_name',
                                                host='your_database_host_url'
                                                )
        return db_connection
    except mysql.connector.Error as err:
        print(err)


class Database():

    """
    This class encapsulates all the code that deals with modifying the database analysed patient data is stored on.
    Azure.
    """

    def insert_data(self, dataframe):
        """
        Writes the analysed data (both positive and negative) for a specific month to the MySQL database for future use
        so that there's no need to re-run the analysis on the same data in case it is requested again later on.

        :param dataframe: Pandas dataframe holding the analysed data for a specific month and year
        """

        if not isinstance(dataframe, pd.DataFrame):
            raise RuntimeError("Dataframe not passed to insert_data")

        db_connection = connect_to_database()
        cursor = db_connection.cursor()

        sql_formula = "INSERT INTO feedbackdatabase (Clinic, Comments, Month, PosOrNeg, Response, Sentiment_Score, Year) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        for row in range(len(dataframe.index)):
            cursor.execute(sql_formula, (
                dataframe.values[row][0], dataframe.values[row][2], dataframe.values[row][4], dataframe.values[row][3],
                dataframe.values[row][1], dataframe.values[row][6], dataframe.values[row][5]))
        db_connection.commit()
        db_connection.close()

    def create_table(self):
        """
        Creates the table in the database if it does not already exist. Table should already exist unless it's being
        run for the first time, but this method is here just as a precautionary measure.
        """
        db_connection = connect_to_database()
        cursor = db_connection.cursor()

        cursor.execute("USE your_database_name")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS feedbackdatabase(ID INT NOT NULL AUTO_INCREMENT, Clinic VARCHAR(100) NOT NULL, Comments VARCHAR(1000) NOT NULL, Month INT NOT NULL, PosOrNeg VARCHAR(10) NOT NULL, Response VARCHAR(25), Sentiment_Score FLOAT NOT NULL, Year INT NOT NULL, PRIMARY KEY (ID));")
        db_connection.commit()
        db_connection.close()

    def use_database_storage(self, month, year):
        """
        Gets the already analysed data from the MySQL database on azure for a specific month and year and converts the data
        into a pandas dataframe

        :param month: Integer representing the name of month for which data needs to be retrieved
        :param year: Integer representing the year for which data needs to be retrieved
        :return: tuple in from of (Boolean, Dataframe) Boolean indicates whether the database already had data for the
        required month and year or not. Dataframe is the dataframe for that months worth of data, None is returned if the
        data is not present in the database
        """

        db_connection = connect_to_database()
        cursor = db_connection.cursor()

        sql_formula = "SELECT * FROM feedbackdatabase WHERE Month = " + str(month) + " AND Year = " + str(year)
        cursor.execute(sql_formula)
        rows = cursor.fetchall()
        db_connection.close()

        if len(rows) != 0:
            # drops the ID row from the database that acts as a primary key to conform to the required format
            df = pd.DataFrame(rows, columns=["DROP", "CLINIC", "COMMENTS", "Month", "Pos or Neg", "RESPONSE",
                                             "Sentiment_Score", "Year"])
            df.drop("DROP", axis=1, inplace=True)
            return True, df
        return False, None

    def delete_specific_month(self, month, year):
        """
        Deletes a specific month and year from the database.

        :param month: Integer representing month to be deleted
        :param year: Integer representing year to be deleted
        """

        db_connection = connect_to_database()
        cursor = db_connection.cursor()

        sql_formula = "DELETE FROM feedbackdatabase WHERE Month = " + str(month) + " AND Year = " + str(year)
        cursor.execute(sql_formula)
        db_connection.commit()
        db_connection.close()

    def delete_specific_year(self, year):
        """
        Deletes all data from the database for a specific year.

        :param year: Integer representing year to be deleted
        """

        for month in range(1, 13):
            self.delete_specific_month(month, year)
