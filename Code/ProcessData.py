import pandas as pd
from datetime import datetime

from AzureBlobStorage import AzureStorage
from Database import Database
from TextAnalyticsAPI import TextAnalyticsService

months = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June", 7: "July", 8: "August",
          9: "September", 10: "October", 11: "November", 12: "December"}


class DataForMultipleMonths():
    """
    This class is responsible for creating a dataframe that stores multiple months worth of data. (specific amount
    depends on the request made) This class is acts as a sort of controller class, using other classes to carry out
    specific tasks such as reading from a database or analysing sentiment.
    """

    def __init__(self):
        self.final_dataframe = pd.DataFrame()
        self.latest_month = ""
        self.latest_year = ""
        self.database = Database()
        self.azure_storage = AzureStorage()
        self.azure_storage.get_blob_data_names()
        self.text_analytics = TextAnalyticsService()

    def main(self, prev_month_number):
        """
        Each execution of this method will append a specific months worth of data to final_dataframe. Acts as a control
        method and mainly calls other methods in a required order to get the data for the required month.

        :param prev_month_number: number of months before the latest month for which there is data available we want to find
        data for. If the latest month data is available for is September (9) and prev_month_number is 2 then we want to get
        data for 2 months before September which is July (7).
        """
        file_month, file_year = self.find_required_month_data(prev_month_number)
        if file_month is None:
            return
        already_stored, month_dataframe = self.database.use_database_storage(file_month, file_year)
        if already_stored is True:
            self.final_dataframe = self.final_dataframe.append(month_dataframe, ignore_index=True)
            return
        blob_name_neg, blob_name_pos = self.set_blob_names(file_month, file_year)
        self.azure_storage.get_data_from_azure(blob_name_neg, blob_name_pos)
        negative_dataframe, positive_dataframe = self.azure_storage.load_data_into_pandas_dataframe()
        self.clean_up_dataframe(len(negative_dataframe.columns), len(positive_dataframe.columns), negative_dataframe,
                                positive_dataframe)
        negative, positive = self.populate_pos_neg_lists(len(negative_dataframe.index), len(positive_dataframe.index))
        self.finalise_data_frame(negative, positive, file_month, file_year, negative_dataframe, positive_dataframe)

    def set_blob_names(self, file_month, file_year):
        """
        Creates the names of the files to be retrieved from Azure blob storage

        :param file_month: Int representing month to get data from
        :param file_year: Int representing year to get data from
        :return: names of the files to retrieve data from
        """
        blob_name_pos = "Positive Comments - " + months[file_month] + " " + str(file_year) + ".xlsx"
        blob_name_neg = "Negative Comments - " + months[file_month] + " " + str(file_year) + ".xlsx"
        return blob_name_neg, blob_name_pos

    def find_latest_data(self):
        """
        Find the latest month and year for which data is available for.
        """
        month = datetime.now().month
        year = datetime.now().year % 100
        # looks for data within the past year, if not found stops searching
        for iteration in range(12):
            for name in self.azure_storage.blob_data_names:
                if months[month] in name and str(year) in name:
                    self.latest_month = month
                    self.latest_year = year
                    return
            if month == 1:
                month = 12
                year -= 1
            else:
                month -= 1

    def reset_latest_available_data(self):
        """
        Resets the variables indicating latest month and year for which data is available. Recalculated every time a
        request is made to prevent stale data from being returned.
        """
        self.latest_month = ""
        self.latest_year = ""

    def find_required_month_data(self, prev_month_number):
        """
        Based on the latest month and year for which data is available, works out the month and year for the data we
        want to analyse.

        :param prev_month_number: number of months before the latest month for which there is data available we want to find
        data for. If the latest month data is available for is September (9) and prev_month_number is 2 then we want to get
        data for 2 months before September which is July (7).
        :return: tuple in from of (int, int) representing month and year respectively. None returned if no valid month and
        year can be found
        """

        # recalculate latest month and year if not already done
        if self.latest_month == "":
            self.find_latest_data()

        # if still not assigned a value then no valid month and year was found
        if self.latest_month == "":
            return None, None

        file_month = self.latest_month
        file_year = self.latest_year

        while prev_month_number > 0:
            prev_month_number -= 1
            if file_month == 1:
                file_month = 12
                file_year -= 1
            else:
                file_month -= 1

        # check if an excel file containing data for the month exists in azure storage
        if "Positive Comments - " + months[file_month] + " " + str(file_year) + ".xlsx" in self.azure_storage.blob_data_names:
            return file_month, file_year
        else:
            return None, None

    def clean_up_dataframe(self, neg_cols, pos_cols, negative_dataframe, positive_dataframe):
        """
        Removes any empty rows or columns in the database.

        :param neg_cols: Number of columns in the negative comments dataframe
        :param pos_cols: Number of columns in the positive comments dataframe
        :param negative_dataframe: Dataframe containing the negative comments
        :param positive_dataframe: Dataframe containing the positive comments
        """
        if pos_cols > 3:
            positive_dataframe.drop(positive_dataframe.columns[3], axis=1, inplace=True)
        positive_dataframe.dropna(axis=0, how='all', inplace=True)
        if neg_cols > 3:
            negative_dataframe.drop(negative_dataframe.columns[3], axis=1, inplace=True)
        negative_dataframe.dropna(axis=0, how='all', inplace=True)

        # replaces NaN values in the Clinic field with 0s so they can be filtered and dealt with later on.
        positive_dataframe['CLINIC'].fillna(0, inplace=True)
        positive_dataframe['CLINIC'].fillna(0, inplace=True)

    def populate_pos_neg_lists(self, neg_len, pos_len):
        """
        Creates two lists to be appended to the two dataframes containing positive and negative comments so they can be
        distinguished when later combined into one dataframe.

        :param neg_len: Number of rows in the negative dataframe
        :param pos_len: Number of rows in the positive dataframe
        :return: lists containing the matching number of instances of either "Positive" or "Negative" depending on which
        dataframe they are intended for.
        """
        positive = []
        negative = []

        for row in range(neg_len):
            negative.append("Negative")

        for row in range(pos_len):
            positive.append("Positive")

        return negative, positive

    def populate_month_year_lists(self, length, file_month, file_year):
        """
        Creates two lists containing the month and year for which the data just obtained is for.
        Used to create two new columns in dataframe representing the months combined positive and negative responses.

        :param length: length of the dataframe the lists are to e appended to
        :param file_month: Name of month for the current data
        :param file_year: Name of year for the current data
        :return: lists containing the month and year with lengths matching the dataframe they are to be added to
        """
        month = []
        year = []

        for row in range(length):
            month.append(file_month)
            year.append(file_year)

        return month, year

    def finalise_data_frame(self, negative, positive, file_month, file_year, negative_dataframe, positive_dataframe):
        """
        Merges the dataframe for positive and negative comments into one dataframe. Then appends that dataframe onto the
        final_dataframe to be returned by the api.

        :param negative: list containing "Negative" with correct length to match the negative_dataframe
        :param positive: list containing "Positive" with correct length to match the positive_dataframe
        :param file_month: Month for which data is being collected
        :param file_year: Year for which data is being collected
        :param negative_dataframe: Dataframe containing negative comments
        :param positive_dataframe: Dataframe containing positive comments
        """

        positive_dataframe["Pos or Neg"] = positive
        negative_dataframe["Pos or Neg"] = negative
        temp = positive_dataframe.append(negative_dataframe, ignore_index=True)
        month, year = self.populate_month_year_lists(len(temp.index), file_month, file_year)
        temp["Month"] = month
        temp["Year"] = year
        scores_for_comments = self.text_analytics.calculate_sentiment_scores(temp["COMMENTS"])
        temp["Sentiment_Score"] = pd.Series(scores_for_comments)
        temp = temp.dropna(subset=["Sentiment_Score"])

        # deal with entries that do not specify a clinic name
        temp = temp[temp.CLINIC != 0]

        # if we are in this method then we were not able to use data from the database, hence store it for future use
        self.database.insert_data(temp)
        self.final_dataframe = self.final_dataframe.append(temp, ignore_index=True)
