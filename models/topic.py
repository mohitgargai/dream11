class Topic:

    def __init__(self, slug):
        self.slug = slug
        self.messages = []

    def push_message(self, message):
        self.messages.append(message)

    def get_messages(self, start, end):
        return self.messages[start:end]

    def total_message_count(self):
        return len(self.messages)
