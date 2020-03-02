import unittest
import TextAnalyticsAPI as TextAnalyticsService


class TextAnalyticsTest(unittest.TestCase):

    def setUp(self):
        """
        Creates a TextAnalyticsAPI object before every test to be used in the tests.
        """

        self.text_analytics = TextAnalyticsService.TextAnalyticsService()

    def test_API_authentication(self):
        """
        Checks to see if authentication and connection to the Text Analytics API works and is successful. Also check to
        see if the function returns a client and not an empty None value. This test failing indicates an issue in API
        authentication and a likely issue with the credentials entered.
        """

        client = self.text_analytics.authenticate_client()
        self.assertNotEqual(client, None)

    def test_sentiment_scores_are_generated(self):
        """
        Checks to see if given a list of comments a list of sentiment scores corresponding to those comments are generated.
        So scores[0] should be for comments[0] and so on. Also checks correct number of scores are returned.
        """

        comments = ["I had a very pleasant meeting with my doctor. He was patient while I explained to him my issues",
                    "This visit was a waste of my time and I found Nurse Lisa to be quite rude"]

        scores = self.text_analytics.calculate_sentiment_scores(comments)

        self.assertEqual(len(scores), 2)
        self.assertTrue(float(scores[0]) > 0.5)  # checks positive comment given positive score
        self.assertTrue(float(scores[1]) < 0.5)  # checks negative comment given negative score
