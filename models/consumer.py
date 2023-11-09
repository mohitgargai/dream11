class Consumer:
    def __init__(self, slug):
        self.slug = slug
        self._subscribed_topics = {}

    def subscribe_to_topic(self, topic_slug, offset):
        self._subscribed_topics[topic_slug] = offset

    def get_topic_offset(self, topic_slug):
        return self._subscribed_topics[topic_slug]

    def update_offset(self, topic_slug, offset):
        if topic_slug not in self._subscribed_topics:
            raise Exception("Invalid Topic")
        self._subscribed_topics[topic_slug] = offset

    def is_topic_subscribed(self, topic_slug):
        return topic_slug in self._subscribed_topics
