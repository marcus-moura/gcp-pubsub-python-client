from dotenv import load_dotenv
import os
from google.cloud import pubsub_v1
import json

# get env
load_dotenv()

project_id = os.getenv("PROJECT_ID")
topic_id = os.getenv("TOPIC_ID")

class PubSub(object):

    def __init__(self, dict_data: dict) -> None:
        self.dict_data = dict_data

    def producer(self):
  
        publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)

        for data in self.dict_data:
            json_data = json.dumps(data).encode("utf-8")
            # Data must be a bytestring
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, json_data)
            print(json_data)
            print(f"Future: {future.result()}")

        print(f"Published messages to {topic_path}.")

