from google.cloud import bigquery
import os
import json  # Importing JSON module to handle JSON export

# --- Authentication Note ---
# Ensure you have authenticated before running this script:
# 1. Preferred for local dev: Run `gcloud auth application-default login` in your terminal.
# 2. Or: Set the GOOGLE_APPLICATION_CREDENTIALS environment variable pointing to your service account key file.
#    (e.g., os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/your/keyfile.json')

# === CONFIGURATION ===
client = bigquery.Client()

# === EXAMPLE: RUN A QUERY ===
# Define the SQL query
sql_query = """
    SELECT
        OCC_CODE,
        OCC_TITLE,
        TOT_EMP,
        H_MEAN,
        A_MEAN
    FROM
        `mythical-patrol-455417-a7.BLS.occupational_employment_and_wage_statistics`
    WHERE
        OCC_CODE = @occ_code_param
"""

# === Configure Query Parameters (Best Practice) ===
job_config = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ScalarQueryParameter("occ_code_param", "STRING", "29-1151")]
)

print(f"Running query for OCC_CODE = '29-1151'")

# Start the query job
try:
    # Pass the sql_query and the job_config
    query_job = client.query(sql_query, job_config=job_config)  # API request

    print("Waiting for job to complete...")
    # Wait for the job to complete and get the results.
    results = query_job.result()  # Waits for job to complete.

    print("\nQuery Results:")
    if results.total_rows > 0:
        # Prepare data to be written to JSON
        results_list = []
        for row in results:
            # Access columns by the names used in your SELECT statement
            row_dict = {
                "OCC_CODE": row.OCC_CODE,
                "OCC_TITLE": row.OCC_TITLE,
                "TOT_EMP": row.TOT_EMP,
                "H_MEAN": row.H_MEAN,
                "A_MEAN": row.A_MEAN
            }
            results_list.append(row_dict)
        
        # Write results to a JSON file
        with open("query_results.json", "w") as json_file:
            json.dump(results_list, json_file, indent=4)

        print("Query results have been written to query_results.json")

    else:
        print("No results found for the specified OCC_CODE.")

except Exception as e:
    print(f"An error occurred: {e}")
    # You might want to inspect query_job.errors here if the job object exists
    if 'query_job' in locals() and hasattr(query_job, 'errors') and query_job.errors:
        print("Job errors:")
        for error in query_job.errors:
            print(f"- {error['message']}")

print("\nScript finished.")
