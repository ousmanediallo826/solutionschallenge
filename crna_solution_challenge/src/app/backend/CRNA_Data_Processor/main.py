# CRNA_Data_Processor/main.py
import base64
import json
import os
import logging 
import re 
from datetime import datetime 

from google.cloud import bigquery
import pgeocode # Make sure this is imported
import pandas   # Make sure this is imported

# --- Logger Setup (ONCE at the top) ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

# --- Global Initializations (ONCE at the top) ---
bq_client = None
try:
    bq_client = bigquery.Client()
    logger.info("BigQuery client initialized successfully.")
except Exception as e:
    logger.error(f"CRITICAL: Failed to initialize BigQuery client: {e}", exc_info=True)
    # bq_client remains None, handled in function entry

geo_nomi = None 
try:
    geo_nomi = pgeocode.Nominatim('us') 
    logger.info("Pgeocode Nominatim client initialized for US.")
except Exception as e_geo_init:
    logger.error(f"Failed to initialize pgeocode client: {e_geo_init}", exc_info=True)
    # geo_nomi remains None, enrichment logic should handle this

# --- Environment Variable Setup (ONCE at the top) ---
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

# --- Helper functions for type conversion ---
def get_float_or_none(value, field_name="<unknown>"): # Level 0
    if value is None or str(value).strip() == "": # Level 1
        return None # Level 2
    try: # Level 1
        return float(value) # Level 2
    except (ValueError, TypeError): # Level 1
        logger.warning(f"Could not convert '{value}' for field '{field_name}' to float, setting to None.") # Level 2
        return None # Level 2

def get_int_or_none(value, field_name="<unknown>"): # Level 0
    if value is None or str(value).strip() == "": # Level 1
        return None # Level 2
    try: # Level 1
        return int(value) # Level 2
    except (ValueError, TypeError): # Level 1
        logger.warning(f"Could not convert '{value}' for field '{field_name}' to int, setting to None.") # Level 2
        return None # Level 2

# --- Allowed Value Lists --- # Level 0
ALLOWED_EMPLOYMENT_TYPES = ["W2", "1099/Contractor", "Part-time W2", "Other"]
ALLOWED_WORK_SETTINGS = ["Hospital - Academic", "Hospital - Community", "ASC", "Office-Based", "VA/Military", "Locums", "Other"]
ALLOWED_CALL_STIPEND_TYPES = ["Per Diem", "Hourly On Call", "Activation Only", "None", "", None] 
ALLOWED_MALPRACTICE_TYPES = ["Occurrence", "Claims-Made", "Claims-Made with Tail", "None", "", None] 

STATE_TO_REGION = { # Level 0
    'AL': 'South', 'AK': 'West', 'AZ': 'West', 'AR': 'South', 'CA': 'West', 
    'CO': 'West', 'CT': 'Northeast', 'DE': 'South', 'FL': 'South', 'GA': 'South', 
    'HI': 'West', 'ID': 'West', 'IL': 'Midwest', 'IN': 'Midwest', 'IA': 'Midwest', 
    'KS': 'Midwest', 'KY': 'South', 'LA': 'South', 'ME': 'Northeast', 'MD': 'South', 
    'MA': 'Northeast', 'MI': 'Midwest', 'MN': 'Midwest', 'MS': 'South', 'MO': 'Midwest', 
    'MT': 'West', 'NE': 'Midwest', 'NV': 'West', 'NH': 'Northeast', 'NJ': 'Northeast', 
    'NM': 'West', 'NY': 'Northeast', 'NC': 'South', 'ND': 'Midwest', 'OH': 'Midwest', 
    'OK': 'South', 'OR': 'West', 'PA': 'Northeast', 'RI': 'Northeast', 'SC': 'South', 
    'SD': 'Midwest', 'TN': 'South', 'TX': 'South', 'UT': 'West', 'VT': 'Northeast', 
    'VA': 'South', 'WA': 'West', 'WV': 'South', 'WI': 'Midwest', 'WY': 'West'
}

# --- Entry point function ---
def process_crna_submission_event(event, context): # Level 0
    logger.error("%%%%%%% FUNCTION ENTRY POINT REACHED %%%%%%%") # Level 1

    if not bq_client: # Level 1
        logger.error("BigQuery client not available globally. Cannot process message.") # Level 2
        raise ConnectionError("BigQuery client not initialized. Function cannot proceed.") # Level 2
    if not TABLE_ID: # Level 1
        logger.error("BigQuery TABLE_ID not configured globally. Cannot process message.") # Level 2
        raise ConnectionError("BigQuery TABLE_ID not configured. Function cannot proceed.") # Level 2

    try: # Level 1 - Main try block
        event_id_str = getattr(context, 'event_id', 'CONTEXT_EVENT_ID_MISSING') # Level 2
        timestamp_str = getattr(context, 'timestamp', 'CONTEXT_TIMESTAMP_MISSING') # Level 2
        resource_name_str = "CONTEXT_RESOURCE_OR_NAME_MISSING_OR_UNEXPECTED_TYPE" # Level 2
        try: # Level 2 - Inner try for context.resource
            if hasattr(context, 'resource') and isinstance(context.resource, dict): # Level 3
                resource_name_str = context.resource.get('name', 'CONTEXT_RESOURCE_NAME_KEY_MISSING') # Level 4
            elif hasattr(context, 'resource'): # Level 3
                resource_name_str = f"CONTEXT_RESOURCE_IS_TYPE_{type(context.resource)}_EXPECTED_DICT" # Level 4
            else: # Level 3
                resource_name_str = "CONTEXT_HAS_NO_RESOURCE_ATTRIBUTE" # Level 4
        except Exception as e_context_resource: # Level 2
            logger.error(f"Error accessing context.resource: {e_context_resource}", exc_info=True) # Level 3
            resource_name_str = "ERROR_ACCESSING_CONTEXT_RESOURCE" # Level 3
        logger.info(f"Processing Pub/Sub event: ID={event_id_str}, Timestamp={timestamp_str}, ResourceName={resource_name_str}") # Level 2

        if 'data' not in event: # Level 2
            logger.error("Malformed Pub/Sub event: No 'data' field found.") # Level 3
            return # Level 3
        
        pubsub_message_data_str = None # Level 2
        try: # Level 2
            pubsub_message_data_str = base64.b64decode(event['data']).decode('utf-8') # Level 3
        except Exception as e: # Level 2
            logger.error(f"Failed to decode base64 data from Pub/Sub message: {e}", exc_info=True) # Level 3
            return # Level 3
        
        data_from_pubsub = None # Level 2
        try: # Level 2
            data_from_pubsub = json.loads(pubsub_message_data_str) # Level 3
            logger.info(f"Starting detailed validation for submission_id_server: {data_from_pubsub.get('submission_id_server')}") # Level 3
        except json.JSONDecodeError as e: # Level 2
            logger.error(f"Error decoding JSON from Pub/Sub message string: {e}. String was: {pubsub_message_data_str}") # Level 3
            return # Level 3

        validation_errors = [] # Level 2
        enriched_data = data_from_pubsub.copy() # Level 2

        # --- 1. Detailed Validation (operates on 'enriched_data') --- # Level 2
        server_generated_fields = ['submission_id_server', 'submission_timestamp_server'] # Level 2
        for field in server_generated_fields: # Level 2
            if field not in enriched_data or enriched_data.get(field) is None or str(enriched_data.get(field, "")).strip() == "": # Level 3
                validation_errors.append(f"Missing critical server-generated field: {field}.") # Level 4
        
        required_user_fields = ['years_experience', 'location_zip_code', 'employment_type', 'work_setting'] # Level 2
        for field in required_user_fields: # Level 2
            val = enriched_data.get(field) # Level 3
            if val is None or str(val).strip() == "": # Level 3
                validation_errors.append(f"Missing or empty required user field: {field}.") # Level 4
        
        years_experience = None # Level 2
        years_experience_from_data = enriched_data.get('years_experience') # Level 2
        if years_experience_from_data is not None and str(years_experience_from_data).strip() != "": # Level 2
            try: # Level 3
                years_experience_val_int = int(years_experience_from_data) # Level 4
                if not (0 <= years_experience_val_int <= 60): # Level 4
                    validation_errors.append(f"years_experience ({years_experience_val_int}) out of range (0-60).") # Level 5
                else: # Level 4
                    years_experience = years_experience_val_int # Level 5
            except (ValueError, TypeError): # Level 3
                validation_errors.append(f"years_experience ('{years_experience_from_data}') must be a valid integer.") # Level 4
        elif 'years_experience' in required_user_fields: # Level 2
             validation_errors.append("years_experience is required and cannot be empty.") # Level 3

        location_zip_code_from_data = str(enriched_data.get('location_zip_code', "")) # Level 2
        if not re.match(r"^\d{5}(-\d{4})?$", location_zip_code_from_data): # Level 2
            validation_errors.append(f"location_zip_code ('{location_zip_code_from_data}') has an invalid format.") # Level 3

        employment_type_from_data = enriched_data.get('employment_type') # Level 2
        if employment_type_from_data not in ALLOWED_EMPLOYMENT_TYPES: # Level 2
            validation_errors.append(f"employment_type ('{employment_type_from_data}') is not a valid option.") # Level 3
        
        work_setting_from_data = enriched_data.get('work_setting') # Level 2
        if work_setting_from_data not in ALLOWED_WORK_SETTINGS: # Level 2
            validation_errors.append(f"work_setting ('{work_setting_from_data}') is not a valid option.") # Level 3

        primary_state_of_licensure_raw = enriched_data.get('primary_state_of_licensure') # Level 2
        primary_state_of_licensure_validated = None # Level 2
        if primary_state_of_licensure_raw and str(primary_state_of_licensure_raw).strip() != "": # Level 2
            processed_state_val = str(primary_state_of_licensure_raw).upper().strip() # Level 3
            if re.match(r"^[A-Z]{2}$", processed_state_val): # Level 3
                primary_state_of_licensure_validated = processed_state_val # Level 4
            else: # Level 3
                validation_errors.append(f"primary_state_of_licensure ('{primary_state_of_licensure_raw}') if provided, must be a 2-letter state code.") # Level 4
        
        # --- Numeric Range Validations --- # Level 2
        base_salary_annual_val = enriched_data.get('base_salary_annual') # Level 2
        if base_salary_annual_val is not None and str(base_salary_annual_val).strip() != "": # Level 2
            try: # Level 3
                bsa = float(base_salary_annual_val) # Level 4
                if not (0 <= bsa <= 2000000): # Level 4
                    validation_errors.append(f"base_salary_annual ({bsa}) is out of a reasonable range (0-2,000,000).") # Level 5
            except (ValueError, TypeError): # Level 3
                validation_errors.append(f"base_salary_annual ('{base_salary_annual_val}') is not a valid number.") # Level 4
        
        pto_weeks_val = enriched_data.get('pto_weeks') # Level 2
        if pto_weeks_val is not None and str(pto_weeks_val).strip() != "": # Level 2
            try: # Level 3
                ptow = int(pto_weeks_val) # Level 4
                if not (0 <= ptow <= 52): # Level 4
                    validation_errors.append(f"pto_weeks ({ptow}) is out of a reasonable range (0-52).") # Level 5
            except (ValueError, TypeError): # Level 3
                validation_errors.append(f"pto_weeks ('{pto_weeks_val}') is not a valid integer.") # Level 4

        # --- Enum Validations for Optional Fields --- # Level 2
        call_stipend_type_from_data_raw = enriched_data.get('call_stipend_type') # Level 2
        call_stipend_type_validated = None # Level 2
        if call_stipend_type_from_data_raw is not None and str(call_stipend_type_from_data_raw).strip() != "": # Level 2
            if call_stipend_type_from_data_raw not in [val for val in ALLOWED_CALL_STIPEND_TYPES if val is not None and val != ""]: # Level 3
                validation_errors.append(f"call_stipend_type ('{call_stipend_type_from_data_raw}') is not valid. Allowed: {[val for val in ALLOWED_CALL_STIPEND_TYPES if val]}") # Level 4
            else: # Level 3
                call_stipend_type_validated = call_stipend_type_from_data_raw if call_stipend_type_from_data_raw and str(call_stipend_type_from_data_raw).strip() != "" else None # Level 4
        
        malpractice_coverage_type_from_data_raw = enriched_data.get('malpractice_coverage_type') # Level 2
        malpractice_coverage_type_validated = None # Level 2
        if malpractice_coverage_type_from_data_raw is not None and str(malpractice_coverage_type_from_data_raw).strip() != "": # Level 2
            if malpractice_coverage_type_from_data_raw not in [val for val in ALLOWED_MALPRACTICE_TYPES if val is not None and val != ""]: # Level 3
                validation_errors.append(f"malpractice_coverage_type ('{malpractice_coverage_type_from_data_raw}') is not valid. Allowed: {[val for val in ALLOWED_MALPRACTICE_TYPES if val]}") # Level 4
            else: # Level 3
                malpractice_coverage_type_validated = malpractice_coverage_type_from_data_raw if malpractice_coverage_type_from_data_raw and str(malpractice_coverage_type_from_data_raw).strip() != "" else None # Level 4

        # --- Conditional Validation --- # Level 2
        if employment_type_from_data == "W2": # Level 2
            has_salary = enriched_data.get('base_salary_annual') is not None and str(enriched_data.get('base_salary_annual')).strip() != "" # Level 3
            has_hourly_components = (enriched_data.get('hourly_rate_w2') is not None and str(enriched_data.get('hourly_rate_w2')).strip() != "" and # Level 3
                                     enriched_data.get('guaranteed_hours_w2') is not None and str(enriched_data.get('guaranteed_hours_w2')).strip() != "") # Level 3
            if not (has_salary or has_hourly_components): # Level 3
                validation_errors.append("For W2 employment, please provide Annual Base Salary OR both W2 Hourly Rate and Guaranteed Hours.") # Level 4

        if call_stipend_type_validated and call_stipend_type_validated != "None": # Level 2
            if enriched_data.get('call_stipend_amount') is None or str(enriched_data.get('call_stipend_amount')).strip() == "": # Level 3
                validation_errors.append(f"call_stipend_amount is required when call_stipend_type is '{call_stipend_type_validated}'.") # Level 4
        
        # --- Final Check for Validation Errors --- # Level 2
        if validation_errors: # Level 2
            error_message_summary = f"Validation failed for submission_id_server {enriched_data.get('submission_id_server')}: {'; '.join(validation_errors)}" # Level 3
            logger.error(error_message_summary) # Level 3
            logger.error(f"Invalid data payload: {enriched_data}") # Level 3
            return # Level 3

        logger.info(f"Validation successful for submission_id_server: {enriched_data.get('submission_id_server')}") # Level 2
        
        # --- 2. Data Enrichment --- # Level 2
        logger.info(f"Starting data enrichment for submission_id_server: {enriched_data.get('submission_id_server')}") # Level 2
        
        # A. Derive State, City, County from ZIP # Level 2
        enriched_data['derived_location_state'] = None # Level 2
        enriched_data['derived_location_city'] = None # Level 2
        enriched_data['derived_location_county'] = None # Level 2
    
        current_location_zip_code = str(enriched_data.get('location_zip_code', "")) # Level 2

        if geo_nomi and current_location_zip_code: # Level 2
            logger.info(f"Attempting geocoding for ZIP: '{current_location_zip_code}'") # Level 3
            try: # Level 3
                zip_info = geo_nomi.query_postal_code(current_location_zip_code) # Level 4
                logger.info(f"Pgeocode query_postal_code raw result for '{current_location_zip_code}':\n{zip_info.to_string() if isinstance(zip_info, pandas.Series) else zip_info}") # Level 4
                logger.info(f"Type of zip_info: {type(zip_info)}") # Level 4
                
                if isinstance(zip_info, pandas.Series) and not zip_info.empty: # Level 4
                    logger.info(f"zip_info Series index (keys): {zip_info.index.tolist()}") # Level 5
                    
                    if 'state_code' in zip_info and not pandas.isna(zip_info['state_code']): # Level 5
                        enriched_data['derived_location_state'] = zip_info['state_code'] # Level 6
                    else: # Level 5
                        logger.warning(f"Field 'state_code' missing or NaN in Series for ZIP: {current_location_zip_code}.") # Level 6

                    if 'place_name' in zip_info and not pandas.isna(zip_info['place_name']): # Level 5
                        enriched_data['derived_location_city'] = zip_info['place_name'] # Level 6
                    else: # Level 5
                        logger.warning(f"Field 'place_name' missing or NaN in Series for ZIP: {current_location_zip_code}.") # Level 6

                    if 'county_name' in zip_info and not pandas.isna(zip_info['county_name']): # Level 5
                        enriched_data['derived_location_county'] = zip_info['county_name'] # Level 6
                    else: # Level 5
                        logger.warning(f"Field 'county_name' missing or NaN in Series for ZIP: {current_location_zip_code}.") # Level 6
                    
                    logger.info(f"Geocoded ZIP {current_location_zip_code}: State={enriched_data['derived_location_state']}, City={enriched_data['derived_location_city']}, County={enriched_data['derived_location_county']}") # Level 5
                else: # Level 4 
                    logger.warning(f"Pgeocode returned an empty or non-Series result for ZIP: {current_location_zip_code}. Result: {zip_info}") # Level 5
            except Exception as e_geo: # Level 3
                logger.error(f"Error during geocoding execution for ZIP {current_location_zip_code}: {e_geo}", exc_info=True) # Level 4
        elif not geo_nomi: # Level 2
            logger.warning(f"Pgeocode client (geo_nomi) not initialized, skipping geocoding for ZIP: {current_location_zip_code}") # Level 3
        else: # Level 2
             logger.info(f"No valid location_zip_code ('{current_location_zip_code}') provided to geocode, skipping.") # Level 3


        # B. Create Experience Bucket # Level 2
        if years_experience is not None: # Level 2
            if years_experience <= 2: enriched_data["experience_bucket"] = "0-2 yrs" # Level 3
            elif years_experience <= 5: enriched_data["experience_bucket"] = "3-5 yrs" # Level 3
            elif years_experience <= 10: enriched_data["experience_bucket"] = "6-10 yrs" # Level 3
            elif years_experience <= 15: enriched_data["experience_bucket"] = "11-15 yrs" # Level 3
            else: enriched_data["experience_bucket"] = ">15 yrs" # Level 3
            logger.info(f"Derived experience_bucket: {enriched_data.get('experience_bucket')}") # Level 3
        else: # Level 2
            enriched_data["experience_bucket"] = None # Level 3

        # C. Calculate Total Estimated Annual Compensation # Level 2
        total_comp = 0.0 # Level 2
        if employment_type_from_data == "W2": # Level 2
            base_val = get_float_or_none(enriched_data.get('base_salary_annual')) # Level 3
            hourly_val = get_float_or_none(enriched_data.get('hourly_rate_w2')) # Level 3
            guar_hours_val = get_int_or_none(enriched_data.get('guaranteed_hours_w2')) # Level 3
            if base_val: total_comp += base_val # Level 4
            elif hourly_val and guar_hours_val: total_comp += hourly_val * guar_hours_val * 52 # Level 4
        elif employment_type_from_data == "1099/Contractor": # Level 2
            hourly_1099 = get_float_or_none(enriched_data.get('hourly_rate_1099')) # Level 3
            if hourly_1099: # Level 3
                assumed_annual_hours_1099 = 1800 # Level 4
                total_comp += hourly_1099 * assumed_annual_hours_1099 # Level 4
        
        total_comp += get_float_or_none(enriched_data.get('bonus_potential_annual')) or 0 # Level 2
        total_comp += get_float_or_none(enriched_data.get('sign_on_bonus')) or 0 # Level 2
        
        current_call_stipend_amount = get_float_or_none(enriched_data.get('call_stipend_amount')) or 0 # Level 2
        if call_stipend_type_validated == "Per Diem" and current_call_stipend_amount > 0: # Level 2
            assumed_call_days_per_year = 60 # Level 3
            total_comp += current_call_stipend_amount * assumed_call_days_per_year # Level 3
        elif call_stipend_type_validated == "Hourly On Call" and current_call_stipend_amount > 0: # Level 2
            assumed_on_call_hours_per_year = 500 # Level 3
            total_comp += current_call_stipend_amount * assumed_on_call_hours_per_year # Level 3
        
        enriched_data["total_estimated_annual_compensation"] = total_comp if total_comp > 0 else None # Level 2
        logger.info(f"Derived total_estimated_annual_compensation: {enriched_data.get('total_estimated_annual_compensation')}") # Level 2

        # D. Derive Region from State # Level 2
        current_derived_state_for_region = enriched_data.get('derived_location_state') # Level 2
        if current_derived_state_for_region and current_derived_state_for_region in STATE_TO_REGION: # Level 2
            enriched_data['location_region'] = STATE_TO_REGION[current_derived_state_for_region] # Level 3
            logger.info(f"Derived location_region: {enriched_data.get('location_region')}") # Level 3
        else: # Level 2
            enriched_data['location_region'] = None # Level 3

        # --- 3. Prepare Final Row for BigQuery --- # Level 2
        row_to_insert = { # Level 2
            "submission_id": enriched_data.get("submission_id_server"), # Level 3
            "submission_timestamp": enriched_data.get("submission_timestamp_server"), # Level 3
            "years_experience": years_experience, # Level 3
            "location_zip_code": location_zip_code_from_data, # Level 3
            "derived_location_state": enriched_data.get("derived_location_state"), # Level 3
            "derived_location_city": enriched_data.get("derived_location_city"), # Level 3
            "derived_location_county": enriched_data.get("derived_location_county"), # Level 3
            "location_region": enriched_data.get("location_region"), # Level 3
            "experience_bucket": enriched_data.get("experience_bucket"), # Level 3
            "total_estimated_annual_compensation": enriched_data.get("total_estimated_annual_compensation"), # Level 3
            "employment_type": employment_type_from_data, # Level 3
            "work_setting": work_setting_from_data, # Level 3
            "primary_state_of_licensure": primary_state_of_licensure_validated, # Level 3
            "base_salary_annual": get_float_or_none(enriched_data.get('base_salary_annual'), 'base_salary_annual'), # Level 3
            "hourly_rate_w2": get_float_or_none(enriched_data.get('hourly_rate_w2'), 'hourly_rate_w2'), # Level 3
            "guaranteed_hours_w2": get_int_or_none(enriched_data.get('guaranteed_hours_w2'), 'guaranteed_hours_w2'), # Level 3
            "hourly_rate_1099": get_float_or_none(enriched_data.get('hourly_rate_1099'), 'hourly_rate_1099'), # Level 3
            "ot_rate_multiplier" : get_float_or_none(enriched_data.get('ot_rate_multiplier'), 'ot_rate_multiplier'), # Level 3
            "call_stipend_type": call_stipend_type_validated, # Level 3
            "call_stipend_amount": get_float_or_none(enriched_data.get('call_stipend_amount'), 'call_stipend_amount'), # Level 3
            "bonus_potential_annual": get_float_or_none(enriched_data.get('bonus_potential_annual'), 'bonus_potential_annual'), # Level 3
            "sign_on_bonus": get_float_or_none(enriched_data.get('sign_on_bonus'), 'sign_on_bonus'), # Level 3
            "retention_bonus_terms": enriched_data.get('retention_bonus_terms'), # Level 3
            "pto_weeks": get_int_or_none(enriched_data.get('pto_weeks'), 'pto_weeks'), # Level 3
            "retirement_match_percentage": get_float_or_none(enriched_data.get('retirement_match_percentage'), 'retirement_match_percentage'), # Level 3
            "cme_allowance_annual": get_float_or_none(enriched_data.get('cme_allowance_annual'), 'cme_allowance_annual'), # Level 3
            "malpractice_coverage_type": malpractice_coverage_type_validated, # Level 3
            "comments": enriched_data.get('comments'), # Level 3
            "data_source": enriched_data.get("data_source", "user_submission_pubsub"), # Level 3
            "is_validated": False, # Level 3
            "anomaly_score": None # Level 3
        }
        
        BQ_ACTUAL_COLUMN_NAMES = [ # Level 2
            "submission_id", "submission_timestamp", "years_experience", "location_zip_code",
            "derived_location_state", "derived_location_city", "derived_location_county", "location_region",
            "experience_bucket", "total_estimated_annual_compensation", 
            "employment_type", "work_setting", "primary_state_of_licensure", "base_salary_annual", 
            "hourly_rate_w2", "guaranteed_hours_w2", "hourly_rate_1099", "ot_rate_multiplier", 
            "call_stipend_type", "call_stipend_amount", "bonus_potential_annual", "sign_on_bonus", 
            "retention_bonus_terms", "pto_weeks", "retirement_match_percentage", "cme_allowance_annual", 
            "malpractice_coverage_type", "comments", "data_source", "is_validated", "anomaly_score"
        ]
        final_row_for_bq = {k: v for k, v in row_to_insert.items() if k in BQ_ACTUAL_COLUMN_NAMES} # Level 2

        # Critical Log: Print the exact row being sent
        logger.info(f"Final row data being sent to BigQuery: {final_row_for_bq}") # Level 2
        missing_bq_cols = [col for col in BQ_ACTUAL_COLUMN_NAMES if col not in final_row_for_bq] # Level 2
        if missing_bq_cols: # Level 2
            logger.error(f"CRITICAL: The following BQ columns are missing from the final row to be inserted: {missing_bq_cols}") # Level 3
            # This might indicate typos in row_to_insert keys or BQ_ACTUAL_COLUMN_NAMES

        logger.info(f"Attempting to insert row into BigQuery table {TABLE_ID} for submission_id: {final_row_for_bq.get('submission_id')}") # Level 2
        errors = bq_client.insert_rows_json(TABLE_ID, [final_row_for_bq]) # Level 2
        if not errors: # Level 2
            logger.info(f"Data inserted successfully into BigQuery for submission_id: {final_row_for_bq.get('submission_id')}") # Level 3
        else: # Level 2
            logger.error(f"BigQuery insertion errors for submission_id {final_row_for_bq.get('submission_id')}: {errors}") # Level 3
            return  # Level 3

    # except blocks aligned with the main 'try' (Level 1)
    except json.JSONDecodeError as e: # Level 1
        logger.error(f"Error decoding JSON from Pub/Sub message (outer try): {e}. Raw data (first 100 chars): {str(event.get('data'))[:100]}") # Level 2
    except Exception as e: # Level 1
        logger.error(f"Unhandled error processing Pub/Sub message (event_id: {getattr(context, 'event_id', 'UNKNOWN')}): {e}", exc_info=True) # Level 2
        raise e # Level 2