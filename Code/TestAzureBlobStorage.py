import unittest
import warnings
import AzureBlobStorage as AzureStorage


# Decorator to ignore warnings in tests
def suppress_warnings(test_func):
    def run_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_func(self, *args, **kwargs)

    return run_test


class AzureBlobStorageTest(unittest.TestCase):

    def setUp(self):
        """
        Creates an AzureBlobStorage object before every test to be used in the tests.
        """
        self.azure_storage = AzureStorage.AzureStorage()

    @suppress_warnings
    def test_connection_possible_to_BlockBlobService(self):
        """
        Tests whether a connection to the BlockBlobService is possible by running a method that accesses the azure storage
        container and adds names of files stored in the container to a list. If no error is thrown then a connection can
        be made. If an error is thrown then there is an issue connecting to the Azure blob storage container
        """

        self.azure_storage.get_blob_data_names()
