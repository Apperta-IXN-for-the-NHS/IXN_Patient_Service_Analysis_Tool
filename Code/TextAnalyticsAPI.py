from azure.cognitiveservices.language.textanalytics import TextAnalyticsClient
from msrest.authentication import CognitiveServicesCredentials
import json


class TextAnalyticsService():
    """
    This class encapsulates all the code that deals with sending data to the Text Analytics API on Azure.
    """

    def __init__(self):
        with open('config.json') as config_file:
            data = json.load(config_file)
            self.subscription_key = data["text_analytics_key"]
            self.endpoint = data["text_analytics_endpoint"]

    def authenticate_client(self):
        """
        Method uses subscription key and endpoint to establish a connection to the TA API services and authorises us
        to use it.

        :return: A Text Analytics API client object allowing us access to services provided by the API.
        """
        credentials = CognitiveServicesCredentials(self.subscription_key)
        text_analytics_client = TextAnalyticsClient(
            endpoint=self.endpoint, credentials=credentials)
        return text_analytics_client

    def calculate_sentiment_scores(self, comments):
        """
        Method that sends comments to the TA API for sentiment analysis then collates the results into a list.

        :param comments: List of comments to be analysed.
        :return: List of scores corresponding to the comments. So index 0 of scores list is for the comment at index 0
        of the comments list etc.
        """
        comment_sentiment_scores = []
        client = self.authenticate_client()

        for comment in comments:
            data = [{"id": 0, "language": "en", "text": comment}]
            try:
                response = client.sentiment(documents=data)
                for document in response.documents:
                    comment_sentiment_scores.append("{:.4f}".format(document.score))
            except Exception as err:
                print("Encountered exception. {}".format(err))

        return comment_sentiment_scores
