import sys

import praw
from configparser import ConfigParser
from praw.models.reddit.subreddit import SubredditStream

from kafka_producer import Kafka


class RedditScraper:
    def __init__(self):
        """
        Initializes the Reddit Scraper class
        """
        # Config parsing
        self._config = ConfigParser()
        self._config.read(["./config.ini"])
        self._username = self._config.get("REDDIT", "username")
        self._client_id = self._config.get("REDDIT", "client_id")
        self._client_secret = self._config.get("REDDIT", "client_secret")
        self._password = self._config.get("REDDIT", "password")
        self._user_agent = self._config.get("REDDIT", "user_agent")
        self._kafka_topic = self._config.get("TOPICS", "input_topic")

        self.message_sent = 1
        try:
            # Initialise reddit API
            self.reddit = praw.Reddit(
                client_id=self._client_id,
                client_secret=self._client_secret,
                password=self._password,
                user_agent=self._user_agent,
                username=self._username,
            )
            print(f"Connected to Reddit: {self._username}")
        except Exception as e:
            print(f"Failed to connect to Reddit: {e}")

        # Initialize Kafka producer
        self.kafka_producer = Kafka()

    def _publish_submission(self, submission):
        self.kafka_producer.publish(
            text=f"{submission.title}\n{submission.selftext}",
            topic=self._kafka_topic
        )
        sys.stdout.write(f"\rSent {self.message_sent} articles to Kafka queue")
        self.message_sent += 1

    def stream(self, subreddit="all"):
        """
        Stream subreddits to Kafka topic
        :param subreddit: name of subreddit to stream
        :return:
        """
        try:
            # Generate streamer instance
            streamer = SubredditStream(self.reddit.subreddit(subreddit))
            print(f"Connected to Subreddit: {subreddit}")
        except Exception as e:
            print(f"Failed to connect to Subreddit: {e}")

        # Streaming to Kafka
        message_sent = 1
        print(f"Fetching top posts from: {subreddit}")
        for submission in self.reddit.subreddit(subreddit).hot(limit=1000):
            if submission is None:
                break
            self._publish_submission(submission)
        print(f"\nStreaming subreddits: {subreddit}")
        for submission in streamer.submissions():
            if submission is None:
                continue
            self._publish_submission(submission)


if __name__ == "__main__":
    r = RedditScraper()
    r.stream()
