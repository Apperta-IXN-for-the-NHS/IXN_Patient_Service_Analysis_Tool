from azure.storage.blob import BlockBlobService
import pandas as pd
import os
import json


class AzureStorage():

    """
    This class encapsulates all the code that deals with getting data from Azure.
    """

    def __init__(self):
        with open('config.json') as config_file:
            data = json.load(config_file)
            self.storage_account_name = data["storage_account_name"]
            self.storage_account_key = data["storage_account_key"]
            self.container_name = data["storage_container_name"]
        self.positive_local_file = "temp1.xlsx"
        self.negative_local_file = "temp2.xlsx"
        self.blob_data_names = []

    def get_blob_data_names(self):
        """
        Populates a list with the names of the files stored in Azure blob storage to be used to find the latest month for
        which data is available
        """
        blob_service = BlockBlobService(account_name=self.storage_account_name, account_key=self.storage_account_key)
        datasheets = blob_service.list_blobs(self.container_name)

        for data in datasheets:
            if data.name not in self.blob_data_names:
                self.blob_data_names.append(data.name)

    def get_data_from_azure(self, blob_name_neg, blob_name_pos):
        """
        Gets the data from Azure blob storage.

        :param blob_name_neg: file name containing negative customer feedback
        :param blob_name_pos: file name containing positive customer feedback
        """
        blob_service = BlockBlobService(account_name=self.storage_account_name, account_key=self.storage_account_key)
        blob_service.get_blob_to_path(self.container_name, blob_name_pos, self.positive_local_file)
        blob_service.get_blob_to_path(self.container_name, blob_name_neg, self.negative_local_file)

    def load_data_into_pandas_dataframe(self):
        """
        loads the data retrieved from azure into a pandas dataframe.
        :return: Two dataframes, first containing the negative customer feedback and the second containing the postive
        customer feedback
        """
        dataframe_blobdata_pos = pd.read_excel(self.positive_local_file)
        os.remove(self.positive_local_file)
        dataframe_blobdata_neg = pd.read_excel(self.negative_local_file)
        os.remove(self.negative_local_file)
        return dataframe_blobdata_neg, dataframe_blobdata_pos
