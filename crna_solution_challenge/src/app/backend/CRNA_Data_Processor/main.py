# CRNA_Data_Processor/main.py
import base64
import json
import os
import logging 
import re # Make sure re is imported at the top
from datetime import datetime 

from google.cloud import bigquery

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Ensure logger level is set

bq_client = None
try:
    bq_client = bigquery.Client()
    logger.info("BigQuery client initialized successfully.")
except Exception as e:
    logger.error(f"CRITICAL: Failed to initialize BigQuery client: {e}", exc_info=True)

ENV_VAR_BQ_PROJECT = "BQ_PROJECT_ID_FOR_FUNCTION"
ENV_VAR_BQ_DATASET = "BQ_DATASET_ID_FOR_FUNCTION"
ENV_VAR_BQ_TABLE = "BQ_TABLE_NAME_FOR_FUNCTION"

BQ_PROJECT_ID_FUNC = os.getenv(ENV_VAR_BQ_PROJECT)
BQ_DATASET_ID_FUNC = os.getenv(ENV_VAR_BQ_DATASET)
BQ_TABLE_NAME_FUNC = os.getenv(ENV_VAR_BQ_TABLE)

TABLE_ID = None
if not all([BQ_PROJECT_ID_FUNC, BQ_DATASET_ID_FUNC, BQ_TABLE_NAME_FUNC]):
    logger.critical(
        f"CRITICAL: One or more BigQuery environment variables are missing or empty. "
        f"Expected keys: {ENV_VAR_BQ_PROJECT}, {ENV_VAR_BQ_DATASET}, {ENV_VAR_BQ_TABLE}. "
        f"Got Project: '{BQ_PROJECT_ID_FUNC}', Dataset: '{BQ_DATASET_ID_FUNC}', Table: '{BQ_TABLE_NAME_FUNC}'. "
        f"Function will likely fail."
    )
else:
    TABLE_ID = f"{BQ_PROJECT_ID_FUNC}.{BQ_DATASET_ID_FUNC}.{BQ_TABLE_NAME_FUNC}"
    logger.info(f"Target BigQuery Table ID successfully constructed: {TABLE_ID}")

def get_float_or_none(value, field_name="<unknown>"):
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' for field '{field_name}' to float, setting to None.")
        return None

def get_int_or_none(value, field_name="<unknown>"):
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' for field '{field_name}' to int, setting to None.")
        return None

ALLOWED_EMPLOYMENT_TYPES = ["W2", "1099/Contractor", "Part-time W2", "Other"]
ALLOWED_WORK_SETTINGS = ["Hospital - Academic", "Hospital - Community", "ASC", "Office-Based", "VA/Military", "Locums", "Other"]
ALLOWED_CALL_STIPEND_TYPES = ["Per Diem", "Hourly On Call", "Activation Only", "None", "", None] 
ALLOWED_MALPRACTICE_TYPES = ["Occurrence", "Claims-Made", "Claims-Made with Tail", "None", "", None] 


def process_crna_submission_event(event, context):
    logger.error("%%%%%%% FUNCTION ENTRY POINT REACHED %%%%%%%") 

    if not bq_client:
        logger.error("BigQuery client not available. Cannot process message.")
        raise ConnectionError("BigQuery client not initialized. Function cannot proceed.")
    if not TABLE_ID:
        logger.error("BigQuery TABLE_ID not configured. Cannot process message.")
        raise ConnectionError("BigQuery TABLE_ID not configured. Function cannot proceed.")

    try:
        event_id_str = getattr(context, 'event_id', 'CONTEXT_EVENT_ID_MISSING')
        timestamp_str = getattr(context, 'timestamp', 'CONTEXT_TIMESTAMP_MISSING')
        resource_name_str = "CONTEXT_RESOURCE_OR_NAME_MISSING_OR_UNEXPECTED_TYPE"
        try:
            if hasattr(context, 'resource') and isinstance(context.resource, dict):
                resource_name_str = context.resource.get('name', 'CONTEXT_RESOURCE_NAME_KEY_MISSING')
            elif hasattr(context, 'resource'):
                resource_name_str = f"CONTEXT_RESOURCE_IS_TYPE_{type(context.resource)}_EXPECTED_DICT"
            else:
                resource_name_str = "CONTEXT_HAS_NO_RESOURCE_ATTRIBUTE"
        except Exception as e_context_resource:
            logger.error(f"Error accessing context.resource: {e_context_resource}", exc_info=True)
            resource_name_str = "ERROR_ACCESSING_CONTEXT_RESOURCE"
        logger.info(f"Processing Pub/Sub event: ID={event_id_str}, Timestamp={timestamp_str}, ResourceName={resource_name_str}")

        if 'data' not in event:
            logger.error("Malformed Pub/Sub event: No 'data' field found.")
            return
        try:
            pubsub_message_data_str = base64.b64decode(event['data']).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to decode base64 data from Pub/Sub message: {e}", exc_info=True)
            return
        try:
            data = json.loads(pubsub_message_data_str)
            logger.info(f"Starting detailed validation for submission_id_server: {data.get('submission_id_server')}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from Pub/Sub message string: {e}. String was: {pubsub_message_data_str}")
            return

        validation_errors = []

        server_generated_fields = ['submission_id_server', 'submission_timestamp_server']
        for field in server_generated_fields:
            if field not in data or data.get(field) is None or str(data.get(field, "")).strip() == "":
                validation_errors.append(f"Missing critical server-generated field: {field}.")
        
        required_user_fields = ['years_experience', 'location_zip_code', 'employment_type', 'work_setting']
        for field in required_user_fields:
            val = data.get(field)
            if val is None or str(val).strip() == "":
                validation_errors.append(f"Missing or empty required user field: {field}.")
        
        try:
            years_experience_val = data.get('years_experience')
            if years_experience_val is None or str(years_experience_val).strip() == "": # Already caught by required check but defensive
                validation_errors.append("years_experience cannot be empty.") # Redundant if required check is good
                years_experience = None # Important to set to None if it's to be used later and failed here
            else:
                years_experience = int(years_experience_val) 
                if not (0 <= years_experience <= 60):
                    validation_errors.append(f"years_experience ({years_experience}) out of range (0-60).")
        except (ValueError, TypeError):
            validation_errors.append(f"years_experience ('{data.get('years_experience')}') must be a valid integer.")
            years_experience = None 

        location_zip_code = data.get('location_zip_code', "")
        if not re.match(r"^\d{5}(-\d{4})?$", str(location_zip_code)): # Check if location_zip_code can be None after required check
            validation_errors.append(f"location_zip_code ('{location_zip_code}') has an invalid format.")

        employment_type = data.get('employment_type')
        if employment_type not in ALLOWED_EMPLOYMENT_TYPES:
            validation_errors.append(f"employment_type ('{employment_type}') is not a valid option.")
        
        work_setting = data.get('work_setting')
        if work_setting not in ALLOWED_WORK_SETTINGS:
            validation_errors.append(f"work_setting ('{work_setting}') is not a valid option.")

        primary_state_of_licensure_raw = data.get('primary_state_of_licensure')
        primary_state_of_licensure = None # Default to None
        if primary_state_of_licensure_raw and str(primary_state_of_licensure_raw).strip() != "":
            if isinstance(primary_state_of_licensure_raw, str) and re.match(r"^[A-Z]{2}$", primary_state_of_licensure_raw.upper()):
                primary_state_of_licensure = primary_state_of_licensure_raw.upper()
            else:
                validation_errors.append(f"primary_state_of_licensure ('{primary_state_of_licensure_raw}') if provided, must be a 2-letter state code.")
        
        # --- Numeric Range Validations ---
        # (Your extensive numeric range checks as previously defined go here)
        # Base Salary Annual
        base_salary_annual_val = data.get('base_salary_annual')
        if base_salary_annual_val is not None and str(base_salary_annual_val).strip() != "":
            try:
                bsa = float(base_salary_annual_val)
                if not (0 <= bsa <= 2000000): 
                    validation_errors.append(f"base_salary_annual ({bsa}) is out of a reasonable range (0-2,000,000).")
            except (ValueError, TypeError):
                validation_errors.append(f"base_salary_annual ('{base_salary_annual_val}') is not a valid number.")
        # ... (all other numeric range checks) ...
        # PTO Weeks
        pto_weeks_val = data.get('pto_weeks')
        if pto_weeks_val is not None and str(pto_weeks_val).strip() != "":
            try:
                ptow = int(pto_weeks_val) 
                if not (0 <= ptow <= 52): 
                    validation_errors.append(f"pto_weeks ({ptow}) is out of a reasonable range (0-52).")
            except (ValueError, TypeError):
                validation_errors.append(f"pto_weeks ('{pto_weeks_val}') is not a valid integer.")


        # --- Enum Validations for Optional Fields ---
        call_stipend_type_from_data = data.get('call_stipend_type')
        call_stipend_type_validated = None # Default to None
        if call_stipend_type_from_data is not None and str(call_stipend_type_from_data).strip() != "":
            if call_stipend_type_from_data not in ALLOWED_CALL_STIPEND_TYPES:
                validation_errors.append(f"call_stipend_type ('{call_stipend_type_from_data}') is not valid. Allowed: {ALLOWED_CALL_STIPEND_TYPES}")
            else: # It's valid and not empty/None
                call_stipend_type_validated = call_stipend_type_from_data
        
        malpractice_coverage_type_from_data = data.get('malpractice_coverage_type')
        malpractice_coverage_type_validated = None # Default to None
        if malpractice_coverage_type_from_data is not None and str(malpractice_coverage_type_from_data).strip() != "":
            if malpractice_coverage_type_from_data not in ALLOWED_MALPRACTICE_TYPES:
                validation_errors.append(f"malpractice_coverage_type ('{malpractice_coverage_type_from_data}') is not valid. Allowed: {ALLOWED_MALPRACTICE_TYPES}")
            else: # It's valid and not empty/None
                malpractice_coverage_type_validated = malpractice_coverage_type_from_data


        # --- Conditional Validation ---
        if employment_type == "W2": # Use the validated employment_type
            has_salary = data.get('base_salary_annual') is not None and str(data.get('base_salary_annual')).strip() != ""
            has_hourly_components = (data.get('hourly_rate_w2') is not None and str(data.get('hourly_rate_w2')).strip() != "" and
                                     data.get('guaranteed_hours_w2') is not None and str(data.get('guaranteed_hours_w2')).strip() != "")
            if not (has_salary or has_hourly_components):
                validation_errors.append("For W2 employment, please provide Annual Base Salary OR both W2 Hourly Rate and Guaranteed Hours.")

        if call_stipend_type_validated and call_stipend_type_validated != "None": # Use validated value
            if data.get('call_stipend_amount') is None or str(data.get('call_stipend_amount')).strip() == "":
                validation_errors.append(f"call_stipend_amount is required when call_stipend_type is '{call_stipend_type_validated}'.")
        
        if validation_errors:
            error_message_summary = f"Validation failed for submission_id_server {data.get('submission_id_server')}: {'; '.join(validation_errors)}"
            logger.error(error_message_summary)
            logger.error(f"Invalid data payload: {data}") 
            return

        logger.info(f"Validation successful for submission_id_server: {data.get('submission_id_server')}")
        
        row_to_insert = {
            "submission_id": data.get("submission_id_server"),
            "submission_timestamp": data.get("submission_timestamp_server"),
            "years_experience": years_experience,
            "location_zip_code": str(location_zip_code),
            "employment_type": employment_type,
            "work_setting": work_setting,
            "primary_state_of_licensure": primary_state_of_licensure, # Already None if not valid/provided
            "base_salary_annual": get_float_or_none(data.get('base_salary_annual'), 'base_salary_annual'),
            "hourly_rate_w2": get_float_or_none(data.get('hourly_rate_w2'), 'hourly_rate_w2'),
            "guaranteed_hours_w2": get_int_or_none(data.get('guaranteed_hours_w2'), 'guaranteed_hours_w2'),
            "hourly_rate_1099": get_float_or_none(data.get('hourly_rate_1099'), 'hourly_rate_1099'),
            "ot_rate_multiplier" : get_float_or_none(data.get('ot_rate_multiplier'), 'ot_rate_multiplier'),
            "call_stipend_type": call_stipend_type_validated, # Use the validated one
            "call_stipend_amount": get_float_or_none(data.get('call_stipend_amount'), 'call_stipend_amount'),
            "bonus_potential_annual": get_float_or_none(data.get('bonus_potential_annual'), 'bonus_potential_annual'),
            "sign_on_bonus": get_float_or_none(data.get('sign_on_bonus'), 'sign_on_bonus'),
            "retention_bonus_terms": data.get('retention_bonus_terms'),
            "pto_weeks": get_int_or_none(data.get('pto_weeks'), 'pto_weeks'),
            "retirement_match_percentage": get_float_or_none(data.get('retirement_match_percentage'), 'retirement_match_percentage'),
            "cme_allowance_annual": get_float_or_none(data.get('cme_allowance_annual'), 'cme_allowance_annual'),
            "malpractice_coverage_type": malpractice_coverage_type_validated, # Use the validated one
            "comments": data.get('comments'),
            "data_source": data.get("data_source", "user_submission_pubsub"),
            "is_validated": False,
            "anomaly_score": None
        }
        
        logger.info(f"Attempting to insert row into BigQuery table {TABLE_ID} for submission_id: {row_to_insert['submission_id']}")
        errors = bq_client.insert_rows_json(TABLE_ID, [row_to_insert])
        if not errors:
            logger.info(f"Data inserted successfully into BigQuery for submission_id: {row_to_insert['submission_id']}")
        else:
            logger.error(f"BigQuery insertion errors for submission_id {row_to_insert['submission_id']}: {errors}")
            return 

    except json.JSONDecodeError as e: 
        logger.error(f"Error decoding JSON from Pub/Sub message (outer try): {e}. Raw data (first 100 chars): {str(event.get('data'))[:100]}")
    except Exception as e:
        logger.error(f"Unhandled error processing Pub/Sub message (event_id: {getattr(context, 'event_id', 'UNKNOWN')}): {e}", exc_info=True)
        raise e