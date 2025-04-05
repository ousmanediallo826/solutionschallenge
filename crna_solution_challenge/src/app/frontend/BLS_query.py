from google.cloud import bigquery
import os # Good practice to import standard libraries at the top

# --- Authentication Note ---
# Ensure you have authenticated before running this script:
# 1. Preferred for local dev: Run `gcloud auth application-default login` in your terminal.
# 2. Or: Set the GOOGLE_APPLICATION_CREDENTIALS environment variable pointing to your service account key file.
#    (e.g., os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/your/keyfile.json')

# === CONFIGURATION ===
# If using ADC or GOOGLE_APPLICATION_CREDENTIALS env var is set,
# initializing the client without arguments should work and detect the project.
# You can explicitly set the project ID if needed:
# project_id = "mythical-patrol-455417-a7" # Replace if different
# client = bigquery.Client(project=project_id)
client = bigquery.Client()

# === EXAMPLE: RUN A QUERY ===
# Define the SQL query
# Note: The GROUP BY clause might be unnecessary if OCC_CODE='29-1151'
#       returns unique rows or if you aren't aggregating.
#       Removed it here for simplicity, assuming you just want the matching row(s).
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
        OCC_CODE = @occ_code_param  -- Using a query parameter is best practice
"""

# === Configure Query Parameters (Best Practice) ===
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("occ_code_param", "STRING", "29-1151"),
    ]
)

print(f"Running query for OCC_CODE = '29-1151'")

# Start the query job
try:
    # Pass the sql_query and the job_config
    query_job = client.query(sql_query, job_config=job_config) # API request

    print("Waiting for job to complete...")
    # Wait for the job to complete and get the results.
    results = query_job.result() # Waits for job to complete.

    print("\nQuery Results:")
    if results.total_rows > 0:
        # Iterate over the results row by row
        for row in results:
            # Access columns by the names used in your SELECT statement
            print(f"- OCC_CODE: {row.OCC_CODE}, "
                  f"OCC_TITLE: {row.OCC_TITLE}, "
                  f"TOT_EMP: {row.TOT_EMP}, "
                  f"H_MEAN: {row.H_MEAN}, "
                  f"A_MEAN: {row.A_MEAN}")
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