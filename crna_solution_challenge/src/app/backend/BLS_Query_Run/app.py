# app.py
from flask import Flask, request, jsonify
from google.cloud import bigquery
import os # For PORT environment variable

app = Flask(__name__)
client = bigquery.Client() # This will use the Cloud Run service account credentials by default

@app.route('/get-bls-data', methods=['POST'])
def get_bls_data():
    try:
        # Get data from the incoming POST request
        data = request.get_json()
        if not data:
            app.logger.warning("No JSON data provided in request body") # Added logging
            return jsonify({'error': 'No JSON data provided in request body'}), 400

        # Check if required fields are present in the request
        occ_title_param_from_request = data.get('OCC_TITLE')
        # A_MEAN from request is expected to be a string that can be parsed as a number
        a_mean_str_from_request = data.get('A_MEAN')

        # Log received data
        app.logger.info(f"Received OCC_TITLE: {occ_title_param_from_request}")
        app.logger.info(f"Received A_MEAN (string): {a_mean_str_from_request}")

        if occ_title_param_from_request is None or a_mean_str_from_request is None:
            app.logger.warning("Missing required fields OCC_TITLE or A_MEAN from request") # Added logging
            return jsonify({'error': 'Missing required fields OCC_TITLE or A_MEAN'}), 400

        # Convert the A_MEAN string from the request to a float for the BQ parameter.
        # This float will be compared against the A_MEAN column in BQ (which is a STRING)
        # after casting the BQ column to FLOAT64 in the SQL.
        try:
            a_mean_float_for_bq_param = float(a_mean_str_from_request)
        except ValueError:
            app.logger.warning(f"A_MEAN '{a_mean_str_from_request}' from request is not a valid number") # Added logging
            return jsonify({'error': 'A_MEAN must be a string representing a valid number'}), 400

        # SQL query targeting the table.
        # Assuming OCC_TITLE column in BQ is STRING.
        # Assuming A_MEAN column in BQ is STRING, so we SAFE_CAST it to FLOAT64 for comparison.
        query = """
            SELECT *
            FROM `mythical-patrol-455417-a7.BLS.occupational_employment_and_wage_statistics`
            WHERE OCC_TITLE = @occ_title_param  -- Parameter for OCC_TITLE (STRING)
            AND SAFE_CAST(A_MEAN AS FLOAT64) = @a_mean_float_param -- Parameter for A_MEAN (FLOAT64)
        """
        # Note: Column names in BQ are OCC_TITLE and A_MEAN (as per your previous confirmation)

        app.logger.info(f"Executing BigQuery query: {query}") # Log the query

        # Set up the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("occ_title_param", "STRING", occ_title_param_from_request),
            bigquery.ScalarQueryParameter("a_mean_float_param", "FLOAT64", a_mean_float_for_bq_param)
        ]
        app.logger.info(f"With query params: {[(p.name, p.type_, p.value) for p in query_params]}") # Log params

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = client.query(query, job_config=job_config)
        app.logger.info(f"BigQuery Job ID: {query_job.job_id}") # Log Job ID
        results = query_job.result() # Waits for the query to finish

        # Process results
        output_data = []
        for row in results:
            row_dict = dict(row.items())
            output_data.append(row_dict)

        app.logger.info(f"Query returned {len(output_data)} rows.") # Log result count

        if not output_data:
            return jsonify({'message': 'No results found for the given criteria'}), 404

        return jsonify({'data': output_data})

    except bigquery.exceptions.GoogleCloudError as bq_error: # Catch BigQuery specific errors
        app.logger.error(f"BigQuery error processing request: {str(bq_error)}")
        # Extract more details if possible, like the job ID or reason
        error_details = f"BigQuery API error: {str(bq_error)}"
        if hasattr(bq_error, 'errors') and bq_error.errors:
            error_details += f" - Reasons: {[e.get('reason', '') + ': ' + e.get('message', '') for e in bq_error.errors]}"
        return jsonify({'error': f'An internal server error occurred (BigQuery): {error_details}'}), 500
    except Exception as e:
        app.logger.error(f"Generic error processing request: {str(e)}", exc_info=True) # Log full traceback for generic errors
        return jsonify({'error': f'An internal server error occurred: {str(e)}'}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)