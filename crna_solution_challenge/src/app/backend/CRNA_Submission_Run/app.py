# submit_crna_data_service/app.py

from flask import Flask, request, jsonify
from google.cloud import pubsub_v1 # <--- Import Pub/Sub client
import os
import uuid
from datetime import datetime, timezone, date
from flask.json.provider import JSONProvider
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomJSONProvider(JSONProvider):
    def dumps(self, obj: any, **kwargs: any) -> str:
        logger.info(f"CustomJSONProvider: 'dumps' called for obj of type: {type(obj)}")
        return json.dumps(obj, default=self.default, **kwargs)

    def loads(self, s: str | bytes, **kwargs: any) -> any:
        logger.info("CustomJSONProvider: 'loads' called.")
        return json.loads(s, **kwargs)

    def default(self, obj):
        logger.info(f"CustomJSONProvider: 'default' method called with obj: {repr(obj)}, type: {type(obj)}")
        if isinstance(obj, (datetime, date)):
            iso_val = obj.isoformat()
            logger.info(f"CustomJSONProvider: Converted datetime/date obj to ISO string: {iso_val}")
            return iso_val
        logger.error(f"CustomJSONProvider: 'default' did not handle type {type(obj)}. Letting json.dumps raise TypeError.")
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable by CustomJSONProvider")

app = Flask(__name__)
app.json = CustomJSONProvider(app)
app.logger.info("CustomJSONProvider registered with Flask app.")

# --- Pub/Sub Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mythical-patrol-455417-a7")
PUB_SUB_TOPIC_NAME = os.getenv("PUB_SUB_TOPIC_NAME", "crna-raw-submissions-topic") # Your Pub/Sub topic ID

publisher = None # Initialize publisher globally, create it once
try:
    publisher = pubsub_v1.PublisherClient()
    TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUB_SUB_TOPIC_NAME)
    app.logger.info(f"Pub/Sub publisher initialized for topic: {TOPIC_PATH}")
except Exception as e:
    app.logger.error(f"Failed to initialize Pub/Sub publisher: {e}", exc_info=True)
    # You might want to decide if the app should fail to start if Pub/Sub can't be initialized

@app.route('/submit-crna-compensation', methods=['POST'])
def submit_crna_compensation():
    global publisher, TOPIC_PATH # Allow access to global publisher

    if not publisher: # Safety check if initialization failed
        app.logger.error("Pub/Sub publisher not available.")
        return jsonify({'error': 'Service temporarily unavailable (Pub/Sub configuration error)'}), 503

    try:
        data_from_request = request.get_json()
        if not data_from_request:
            app.logger.warning("No JSON data provided in request body")
            return jsonify({'error': 'No JSON data provided'}), 400

        app.logger.info(f"Received submission data: {data_from_request}")

        # --- 1. Minimal Validation (Optional but Recommended) ---
        # You might still want to check for a few absolutely critical fields
        # to avoid flooding Pub/Sub with completely junk messages.
        # For example:
        if not isinstance(data_from_request, dict):
             app.logger.warning("Request data is not a valid JSON object.")
             return jsonify({'error': 'Request data must be a valid JSON object.'}), 400
        
        # if 'years_experience' not in data_from_request: # Example minimal check
        #     return jsonify({'error': 'Missing a key field like years_experience'}), 400


        # --- 2. Prepare Data for Pub/Sub Message ---
        # Add server-side generated fields.
        # The message payload will be the original data + these server-side fields.
        message_payload = data_from_request.copy() # Start with the client's data
        message_payload["submission_id_server"] = str(uuid.uuid4())
        message_payload["submission_timestamp_server"] = datetime.now(timezone.utc).isoformat()
        # You could also add a 'received_at_api_timestamp' if you want to track processing time.

        # Convert the entire payload to a JSON string (bytes for Pub/Sub)
        message_data_bytes = json.dumps(message_payload).encode("utf-8")
        # Note: We use the standard json.dumps here. If message_payload contains datetimes
        # (e.g., if client sent some), our CustomJSONProvider for Flask won't apply.
        # It's best if the client sends dates as ISO strings.
        # If you *must* handle datetimes from client in message_payload, use:
        # message_data_bytes = app.json.dumps(message_payload).encode("utf-8")

        # --- 3. Publish to Pub/Sub ---
        app.logger.info(f"Publishing message to {TOPIC_PATH} with submission_id_server: {message_payload['submission_id_server']}")
        
        future = publisher.publish(TOPIC_PATH, data=message_data_bytes)
        message_id = future.result()

        app.logger.info(f"Message {message_id} published to {TOPIC_PATH} for submission_id_server: {message_payload['submission_id_server']}")

        # Return 202 Accepted: The request has been accepted for processing,
        # but the processing has not been completed.
        return jsonify({
            "message": "Compensation data submission accepted for processing.",
            "submission_id": message_payload["submission_id_server"] # Return the server-generated ID
        }), 202

    except Exception as e:
        app.logger.error(f"Error processing submission for Pub/Sub: {str(e)}", exc_info=True)
        return jsonify({'error': f'An internal server error occurred: {str(e)}'}), 500

# ... (if __name__ == '__main__': block as before) ...
# --- Helper functions for type conversion (get_float_or_none, get_int_or_none) ---
# These are NOT used in this version of the Cloud Run app,
# they will be needed in the Cloud Function that processes the Pub/Sub message.