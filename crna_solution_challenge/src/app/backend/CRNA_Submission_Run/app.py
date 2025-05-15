# submit_crna_data_service/app.py

# --- Essential Imports for a Flask App ---
from flask import Flask, request, jsonify
from google.cloud import bigquery
import os
import uuid # For generating submission_id
from datetime import datetime, timezone, date # If you decide to generate timestamp in Python
from flask.json.provider import JSONProvider
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomJSONProvider(JSONProvider):
    def dumps(self, obj: any, **kwargs: any) -> str:
        logger.info(f"CustomJSONProvider: 'dumps' called for obj of type: {type(obj)}")
        # Ensure 'default=self.default' is passed so our custom default is used
        return json.dumps(obj, default=self.default, **kwargs)

    def loads(self, s: str | bytes, **kwargs: any) -> any:
        logger.info("CustomJSONProvider: 'loads' called.")
        return json.loads(s, **kwargs)

    def default(self, obj):
        logger.info(f"CustomJSONProvider: 'default' method called with obj: {repr(obj)}, type: {type(obj)}") # Use repr(obj)
        if isinstance(obj, (datetime, date)):
            iso_val = obj.isoformat()
            logger.info(f"CustomJSONProvider: Converted datetime/date obj to ISO string: {iso_val}")
            return iso_val
        # If we reach here, it means our isinstance check didn't match.
        # Let the standard json.dumps raise the TypeError.
        logger.error(f"CustomJSONProvider: 'default' did not handle type {type(obj)}. Letting json.dumps raise TypeError.")
        # DO NOT call super().default(obj) here as it might lead to other errors or infinite recursion if not careful.
        # The 'json.dumps' in the 'dumps' method will raise the TypeError if this 'default' doesn't handle it.
        # For clarity, explicitly raise it if that's the intent for unhandled types.
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable by CustomJSONProvider")
    
# --- Initialize Flask App ---
app = Flask(__name__)

app.json = CustomJSONProvider(app)

app.logger.info("CustomJSONProvider registered with Flask app.")

# --- Initialize BigQuery Client ---
client = bigquery.Client()

# --- Define Table ID ---
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "mythical-patrol-455417-a7")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "BLS")
BQ_SUBMISSION_TABLE_NAME = os.getenv("BQ_SUBMISSION_TABLE_NAME", "crna_submitted_compensation")
TABLE_ID = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_SUBMISSION_TABLE_NAME}"

# --- Helper functions for type conversion ---
def get_float_or_none(value, field_name="<unknown>"): # Added field_name for better logging
    if value is None or str(value).strip() == "": # Also check for empty string
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        app.logger.warning(f"Could not convert '{value}' for field '{field_name}' to float, setting to None.")
        return None

def get_int_or_none(value, field_name="<unknown>"):
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        app.logger.warning(f"Could not convert '{value}' for field '{field_name}' to int, setting to None.")
        return None

# --- Your Data Submission Route ---
@app.route('/submit-crna-compensation', methods=['POST'])
def submit_crna_compensation():
    try:
        data = request.get_json()
        if not data:
            app.logger.warning("No JSON data provided in request body")
            return jsonify({'error': 'No JSON data provided'}), 400

        app.logger.info(f"Received submission data: {data}")

        # --- 1. Validate Input Data ---
        required_fields = ['years_experience', 'location_zip_code', 'employment_type', 'work_setting']
        for field in required_fields:
            if field not in data or data[field] is None or str(data[field]).strip() == "": # Check for empty string too
                app.logger.warning(f"Missing required field: {field}")
                return jsonify({'error': f'Missing or empty required field: {field}'}), 400
        
        try:
            years_experience_val = data['years_experience'] # Use a different var name to avoid confusion
            if years_experience_val is None or str(years_experience_val).strip() == "":
                 raise ValueError("Experience cannot be empty")
            years_experience = int(years_experience_val)
            if not (0 <= years_experience <= 60):
                raise ValueError("Experience out of range (0-60)")
        except (ValueError, TypeError) as e:
            app.logger.warning(f"Invalid years_experience: {data.get('years_experience')}. Error: {e}")
            return jsonify({'error': f"years_experience must be a valid integer between 0 and 60. Error: {e}"}), 400

        # ... (Your extensive validation logic for other fields: zip format, enums, etc.) ...
        # Example for employment_type enum validation
        ALLOWED_EMPLOYMENT_TYPES = ["W2", "1099/Contractor", "Part-time W2", "Other"] # Define these globally or from config
        if data.get('employment_type') not in ALLOWED_EMPLOYMENT_TYPES:
            app.logger.warning(f"Invalid employment_type: {data.get('employment_type')}")
            return jsonify({'error': f"Invalid employment_type. Allowed: {', '.join(ALLOWED_EMPLOYMENT_TYPES)}"}), 400


        # --- 2. Prepare Data for BigQuery ---
        row_to_insert = {
            "submission_id": str(uuid.uuid4()),
            "submission_timestamp": datetime.now(timezone.utc).isoformat(),
            "years_experience": years_experience,
            "location_zip_code": str(data.get('location_zip_code')),
            "employment_type": str(data.get('employment_type')),
            "work_setting": str(data.get('work_setting')),
            "primary_state_of_licensure": data.get('primary_state_of_licensure'),
            "base_salary_annual": get_float_or_none(data.get('base_salary_annual'), 'base_salary_annual'),
            "hourly_rate_w2": get_float_or_none(data.get('hourly_rate_w2'), 'hourly_rate_w2'),
            "guaranteed_hours_w2": get_int_or_none(data.get('guaranteed_hours_w2'), 'guaranteed_hours_w2'),
            "hourly_rate_1099": get_float_or_none(data.get('hourly_rate_1099'), 'hourly_rate_1099'),
            "ot_rate_multiplier" : get_float_or_none(data.get('ot_rate_multiplier'), 'ot_rate_multiplier'),
            "call_stipend_type": data.get('call_stipend_type'),
            "call_stipend_amount": get_float_or_none(data.get('call_stipend_amount'), 'call_stipend_amount'),
            "bonus_potential_annual": get_float_or_none(data.get('bonus_potential_annual'), 'bonus_potential_annual'),
            "sign_on_bonus": get_float_or_none(data.get('sign_on_bonus'), 'sign_on_bonus'),
            "retention_bonus_terms": data.get('retention_bonus_terms'),
            "pto_weeks": get_int_or_none(data.get('pto_weeks'), 'pto_weeks'),
            "retirement_match_percentage": get_float_or_none(data.get('retirement_match_percentage'), 'retirement_match_percentage'),
            "cme_allowance_annual": get_float_or_none(data.get('cme_allowance_annual'), 'cme_allowance_annual'),
            "malpractice_coverage_type": data.get('malpractice_coverage_type'),
            "comments": data.get('comments'),
            "data_source": "user_submission", # Hardcoded as this is the user submission endpoint
            "is_validated": False,            # Default for new submissions
            "anomaly_score": None             # Will be populated by BQML later
        }
        loggable_row_to_insert = row_to_insert.copy()

        if "submission_timestamp" in loggable_row_to_insert and isinstance(loggable_row_to_insert["submission_timestamp"], datetime):
            loggable_row_to_insert["submission_timestamp"] = loggable_row_to_insert["submission_timestamp"].isoformat()

        app.logger.info(f"Row to insert into BigQuery (loggable version): {loggable_row_to_insert}")
        

        # --- 3. Insert into BigQuery ---
        errors = client.insert_rows_json(TABLE_ID, [row_to_insert])
        if not errors:
            app.logger.info(f"New CRNA compensation data inserted with ID: {row_to_insert['submission_id']}")
            return jsonify({"message": "Compensation data submitted successfully", "submission_id": row_to_insert['submission_id']}), 201
        else:
            app.logger.error(f"Errors inserting CRNA compensation data into {TABLE_ID}: {errors}")
            return jsonify({"error": "Failed to insert data into BigQuery", "details": errors}), 500

    except Exception as e:
        app.logger.error(f"Unhandled error in /submit-crna-compensation: {str(e)}", exc_info=True)
        return jsonify({'error': f'An internal server error occurred: {str(e)}'}), 500

# --- Main execution block (for local development) ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)