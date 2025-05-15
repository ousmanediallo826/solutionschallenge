# submit_crna_data_service/app.py

# --- Essential Imports for a Flask App ---
from flask import Flask, request, jsonify
from google.cloud import bigquery
import os
import uuid # For generating submission_id
from datetime import datetime, timezone # If you decide to generate timestamp in Python

# --- Initialize Flask App ---
app = Flask(__name__)

# --- Initialize BigQuery Client ---
# This will use the Cloud Run service account credentials by default when deployed
# For local testing, it uses your gcloud CLI authenticated user or GOOGLE_APPLICATION_CREDENTIALS
client = bigquery.Client()

# --- Define Environment Variables (Optional, but good practice to get from env) ---
# You would set these in your Cloud Run service configuration
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "mythical-patrol-455417-a7") # Default if not set
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "BLS")
BQ_SUBMISSION_TABLE_NAME = os.getenv("BQ_SUBMISSION_TABLE_NAME", "crna_submitted_compensation")
TABLE_ID = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_SUBMISSION_TABLE_NAME}"

# --- Your Data Submission Route ---
@app.route('/submit-crna-compensation', methods=['POST'])
def submit_crna_compensation():
    try:
        data = request.get_json() # 'request' is now defined from 'from flask import request'
        if not data:
            # 'jsonify' is now defined from 'from flask import jsonify'
            return jsonify({'error': 'No JSON data provided'}), 400

        # --- 1. Validate Input Data ---
        required_fields = ['years_experience', 'location_zip_code', 'employment_type', 'work_setting']
        for field in required_fields:
            if field not in data or data[field] is None:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        try:
            years_experience = int(data['years_experience'])
            if not (0 <= years_experience <= 60):
                raise ValueError("Experience out of range")
        except (ValueError, TypeError):
            return jsonify({'error': 'years_experience must be a valid integer between 0 and 60'}), 400

        # ... (Your extensive validation logic here) ...

        # --- 2. Prepare Data for BigQuery ---
        row_to_insert = {
            "submission_id": str(uuid.uuid4()), # 'uuid' is now defined
            "submission_timestamp": datetime.now(timezone.utc), # 'datetime', 'timezone' are defined
            "years_experience": years_experience,
            "location_zip_code": str(data.get('location_zip_code')),
            "employment_type": str(data.get('employment_type')),
            "work_setting": str(data.get('work_setting')),
            # ... map all other validated fields ...
        }
        try:
            row_to_insert["base_salary_annual"] = float(data.get('base_salary_annual')) if data.get('base_salary_annual') else None
        except (ValueError, TypeError):
            row_to_insert["base_salary_annual"] = None

        row_to_insert["data_source"] = "user_submission"
        row_to_insert["is_validated"] = False

        # --- 3. Insert into BigQuery ---
        # 'client' is now defined from 'client = bigquery.Client()'
        errors = client.insert_rows_json(TABLE_ID, [row_to_insert])
        if not errors: # Check if the list is empty
            app.logger.info(f"New CRNA compensation data inserted with ID: {row_to_insert['submission_id']}")
            return jsonify({"message": "Compensation data submitted successfully", "submission_id": row_to_insert['submission_id']}), 201
        else:
            app.logger.error(f"Errors inserting CRNA compensation data: {errors}")
            return jsonify({"error": "Failed to insert data into BigQuery", "details": errors}), 500

    except Exception as e:
        # 'app.logger' is available because 'app = Flask(__name__)' was defined
        app.logger.error(f"Error submitting CRNA compensation: {str(e)}", exc_info=True)
        return jsonify({'error': f'An internal server error occurred: {str(e)}'}), 500

# --- Main execution block (for local development) ---
if __name__ == '__main__':
    # 'os' is now defined from 'import os'
    port = int(os.environ.get('PORT', 8080))
    # 'app.run' is available because 'app = Flask(__name__)' was defined
    app.run(host='0.0.0.0', port=port, debug=True)