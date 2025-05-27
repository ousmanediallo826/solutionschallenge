# submit_crna_data_service/app.py

from flask import Flask, request, jsonify
from flask_cors import CORS # <--- IMPORT CORS
from google.cloud import pubsub_v1
import os
import uuid
from datetime import datetime, timezone, date # 'date' import might not be strictly needed by this file
from flask.json.provider import JSONProvider
import json
import logging

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO) # Basic config for Gunicorn logs
logger = logging.getLogger(__name__) # Use Flask's app.logger for route-specific logging

# --- Custom JSON Provider (if still needed for jsonify responses, good practice) ---
class CustomJSONProvider(JSONProvider):
    def dumps(self, obj: any, **kwargs: any) -> str:
        # logger.info(f"CustomJSONProvider: 'dumps' called for obj of type: {type(obj)}") # Can be verbose
        return json.dumps(obj, default=self.default, **kwargs)

    def loads(self, s: str | bytes, **kwargs: any) -> any:
        # logger.info("CustomJSONProvider: 'loads' called.") # Can be verbose
        return json.loads(s, **kwargs)

    def default(self, obj):
        # logger.info(f"CustomJSONProvider: 'default' method called with obj: {repr(obj)}, type: {type(obj)}")
        if isinstance(obj, (datetime, date)): # datetime.date might not be used by this app directly
            iso_val = obj.isoformat()
            # logger.info(f"CustomJSONProvider: Converted datetime/date obj to ISO string: {iso_val}")
            return iso_val
        # logger.error(f"CustomJSONProvider: 'default' did not handle type {type(obj)}. Letting json.dumps raise TypeError.")
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable by CustomJSONProvider")

# --- Initialize Flask App ---
app = Flask(__name__)
app.json = CustomJSONProvider(app) # Register custom JSON provider

# --- CORS Configuration ---
# This allows requests from your Firebase Hosting URL and localhost (for firebase serve)
# to the /submit-crna-compensation endpoint.
# Replace 'your-project-id' with your actual Firebase project ID.
FIREBASE_HOSTING_URL = f"https://{os.getenv('GCP_PROJECT_ID', 'mythical-patrol-455417-a7')}.web.app"
FIREBASE_HOSTING_URL_ALT = f"https://{os.getenv('GCP_PROJECT_ID', 'mythical-patrol-455417-a7')}.firebaseapp.com"

# Be as specific as possible with origins in production
origins = [
    "http://localhost:5000", # For `firebase serve` or other local dev
    FIREBASE_HOSTING_URL,
    FIREBASE_HOSTING_URL_ALT
    # Add any other specific origins you need to allow, e.g., a custom domain for Firebase Hosting
]
# You can also use "*" to allow all origins for initial testing, but it's less secure for production.
# CORS(app) # Allows all origins for all routes

CORS(app, resources={
    r"/submit-crna-compensation": {"origins": origins}
})
app.logger.info(f"CORS enabled for /submit-crna-compensation, allowed origins: {origins}")
app.logger.info("CustomJSONProvider registered with Flask app.")


# --- Pub/Sub Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mythical-patrol-455417-a7")
PUB_SUB_TOPIC_NAME = os.getenv("PUB_SUB_TOPIC_NAME", "crna-raw-submissions-topic")

publisher = None
TOPIC_PATH = None # Initialize TOPIC_PATH
try:
    publisher = pubsub_v1.PublisherClient()
    TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUB_SUB_TOPIC_NAME)
    app.logger.info(f"Pub/Sub publisher initialized for topic: {TOPIC_PATH}")
except Exception as e:
    app.logger.error(f"Failed to initialize Pub/Sub publisher: {e}", exc_info=True)

# --- Your Data Submission Route ---
@app.route('/submit-crna-compensation', methods=['POST'])
def submit_crna_compensation():
    # Removed global publisher, TOPIC_PATH as they are now module-level and accessed directly

    if not publisher or not TOPIC_PATH: # Check if Pub/Sub client and topic path are initialized
        app.logger.error("Pub/Sub publisher or TOPIC_PATH not available.")
        return jsonify({'error': 'Service temporarily unavailable (Pub/Sub configuration error)'}), 503

    try:
        data_from_request = request.get_json()
        if not data_from_request:
            app.logger.warning("No JSON data provided in request body")
            return jsonify({'error': 'No JSON data provided'}), 400

        app.logger.info(f"Received submission data: {data_from_request}")

        if not isinstance(data_from_request, dict):
             app.logger.warning("Request data is not a valid JSON object.")
             return jsonify({'error': 'Request data must be a valid JSON object.'}), 400
        
        message_payload = data_from_request.copy()
        message_payload["submission_id_server"] = str(uuid.uuid4())
        message_payload["submission_timestamp_server"] = datetime.now(timezone.utc).isoformat()

        # For serializing the message_payload to Pub/Sub, standard json.dumps is usually fine
        # as the critical datetime (submission_timestamp_server) is already an ISO string.
        # If data_from_request could contain other datetime objects from the client, 
        # then app.json.dumps(message_payload) would be safer.
        message_data_bytes = json.dumps(message_payload).encode("utf-8")
        
        app.logger.info(f"Publishing message to {TOPIC_PATH} with submission_id_server: {message_payload['submission_id_server']}")
        
        future = publisher.publish(TOPIC_PATH, data=message_data_bytes)
        message_id = future.result() # Ensure this is .result()

        app.logger.info(f"Message {message_id} published to {TOPIC_PATH} for submission_id_server: {message_payload['submission_id_server']}")

        return jsonify({
            "message": "Compensation data submission accepted for processing.",
            "submission_id": message_payload["submission_id_server"]
        }), 202

    except Exception as e:
        app.logger.error(f"Error processing submission for Pub/Sub: {str(e)}", exc_info=True)
        return jsonify({'error': f'An internal server error occurred: {str(e)}'}), 500

# --- Main execution block (for local development) ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    # When running locally with `python app.py`, Flask's dev server is used.
    # For CORS to work with the dev server and allow localhost:5000 (from firebase serve),
    # the CORS(app) call above is sufficient.
    app.run(host='0.0.0.0', port=port, debug=True)