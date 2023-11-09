import uuid


class Message:
    def __init__(self, msg):
        self.id = str(uuid.uuid4())
        self.msg = msg

    def serialize(self):
        return str(self.msg)
