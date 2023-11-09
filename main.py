from models.queue import Queue


def main():
    queue = Queue()
    queue.create_topic("topic1")
    queue.push_message_to_topic("topic1", "message1")
    queue.push_message_to_topic("topic1", "message2")

    queue.register_consumer_topic("consumer1", "topic1")
    queue.register_consumer_topic("consumer2", "topic1")

    messages1 = queue.consume_messages("consumer1", "topic1", 1)
    print(messages1)
    messages2 = queue.consume_messages("consumer1", "topic1", 1)
    print(messages2)
    messages3 = queue.consume_messages("consumer1", "topic1", 1)
    print(messages3)

    messages4 = queue.consume_messages("consumer2", "topic1", 2)
    print(messages4)
    messages5 = queue.consume_messages("consumer2", "topic1", 2)
    print(messages5)


if __name__ == '__main__':
    main()
