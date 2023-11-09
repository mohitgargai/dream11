from models.consumer import Consumer
from models.message import Message
from models.topic import Topic


class Queue:

    def __init__(self):
        self._topics = {}
        self._consumers = {}

    def create_topic(self, slug):
        topic = Topic(slug)
        self._topics[slug] = topic

    def push_message_to_topic(self, topic_slug, message):
        if topic_slug not in self._topics:
            raise Exception("Invalid Topic")

        topic = self._topics[topic_slug]
        message = Message(message)
        topic.push_message(message)

    def register_consumer_topic(self, consumer_slug, topic_slug, offset=0):
        if consumer_slug not in self._consumers:
            consumer = Consumer(consumer_slug)
            self._consumers[consumer_slug] = consumer
        else:
            consumer = self._consumers[consumer_slug]

        if consumer.is_topic_subscribed(topic_slug):
            raise Exception("Consumer is already subscribed to the Topic")

        consumer.subscribe_to_topic(topic_slug, offset)

    def consume_messages(self, consumer_slug, topic_slug, count):
        if consumer_slug not in self._consumers:
            raise Exception("Invalid Consumer")

        if topic_slug not in self._topics:
            raise Exception("Invalid Topic")

        # fetch consumer
        consumer = self._consumers[consumer_slug]
        if not consumer.is_topic_subscribed(topic_slug):
            raise Exception("Consumer is not subscribed to Topic")

        # fetch topic
        topic = self._topics[topic_slug]

        current_offset = consumer.get_topic_offset(topic_slug)
        new_offset = min(current_offset + count, topic.total_message_count())

        consumer.update_offset(topic_slug, new_offset)

        messages = topic.get_messages(current_offset, new_offset)
        return [message.serialize() for message in messages]

    def update_topic_offset(self, consumer_slug, topic_slug, new_offset):
        if consumer_slug not in self._consumers:
            raise Exception("Invalid Consumer")

        if topic_slug not in self._topics:
            raise Exception("Invalid Topic")

        consumer = self._consumers[consumer_slug]
        if not consumer.is_topic_subscribed(topic_slug):
            raise Exception("Consumer is not subscribed to Topic")

        consumer.update_offset(topic_slug, new_offset)
