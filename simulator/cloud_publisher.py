import os
import json
import logging
from google.cloud import pubsub_v1
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

class CloudPublisher:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        key_json = os.getenv("GCP_SA_KEY")

        if key_json:
            info = json.loads(key_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            self.client = pubsub_v1.PublisherClient(credentials=credentials)
            logger.info("CloudPublisher initialized with Service Account Key.")
        else:
            self.client = pubsub_v1.PublisherClient()
            logger.warning("GCP_SA_KEY not found. Using default credentials.")

    def publish(self, topic_id, data):
        """Publishes a dictionary to a Google Pub/Sub topic."""
        try:
            topic_path = self.client.topic_path(self.project_id, topic_id)
            message_json = json.dumps(data)
            message_bytes = message_json.encode("utf-8")
            
            future = self.client.publish(topic_path, data=message_bytes)
            return future.result() 
        except Exception as e:
            logger.error(f"Failed to publish to {topic_id}: {e}")
            return None