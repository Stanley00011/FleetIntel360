# simulator/cloud_publisher.py
import os
import json
import logging
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

class CloudPublisher:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        if not self.project_id:
            logger.warning("GCP_PROJECT_ID not set. Cloud publishing will fail.")
        self.publisher = pubsub_v1.PublisherClient()

    def publish(self, topic_id, data):
        """Publishes a dictionary to a Google Pub/Sub topic."""
        try:
            topic_path = self.publisher.topic_path(self.project_id, topic_id)
            message_json = json.dumps(data)
            message_bytes = message_json.encode("utf-8")
            
            future = self.publisher.publish(topic_path, data=message_bytes)
            return future.result() # Wait for confirmation
        except Exception as e:
            logger.error(f"Failed to publish to {topic_id}: {e}")
            return None