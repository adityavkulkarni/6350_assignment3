import praw
from configparser import ConfigParser
from praw.models.reddit.subreddit import SubredditStream

from kafka_producer import Kafka


class RedditScraper:
    def __init__(self):
        self._config = ConfigParser()
        self._config.read(["./config.ini"])
        self._username = self._config.get("REDDIT", "username")
        self._client_id = self._config.get("REDDIT", "client_id")
        self._client_secret = self._config.get("REDDIT", "client_secret")
        self._password = self._config.get("REDDIT", "password")
        self._user_agent = self._config.get("REDDIT", "user_agent")

        self.kafka_topic = self._config.get("TOPICS", "input_topic")

        try:
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
        self.kafka_producer = Kafka()

    def stream(self, subreddit="all"):
        message_sent = 1
        try:
            streamer = SubredditStream(self.reddit.subreddit(subreddit))
            print(f"Connected to Subreddit: {subreddit}")
        except Exception as e:
            print(f"Failed to connect to Subreddit: {e}")

        for submission in streamer.submissions():
            self.kafka_producer.publish(
                text=f"{submission.title}\n{submission.selftext}",
                topic=self.kafka_topic
            )
            if message_sent == 10:
                print(f"Sent 10 articles to Kafka queue")
                message_sent = 0
            message_sent += 1


if __name__ == "__main__":
    r = RedditScraper()
    r.stream()