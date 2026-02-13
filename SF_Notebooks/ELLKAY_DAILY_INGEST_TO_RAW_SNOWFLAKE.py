# ELLKAY_DAILY_INGEST_TO_RAW_SNOWFLAKE.py
# ==========================================================
# Config-driven daily ingestion for all ELLKAY practices
# Schedule: 7:00 AM CT (America/Chicago) via Snowflake Task
# ==========================================================
#
# This notebook:
# - Reads all ELLKAY practices from config (identified by site_service_key)
# - Processes each practice sequentially
# - Processes each endpoint in config order (respects dependencies)
# - Continues on failure, logs all results
#
# ==========================================================

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, current_timestamp
import pandas as pd
import requests
import json
import uuid
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple

# ==========================================================
# Secrets helper
# ==========================================================
try:
    import streamlit as st
    _HAS_STREAMLIT = True
except Exception:
    st = None
    _HAS_STREAMLIT = False

def _get_secret_obj(alias: str):
    """Return the secret object bound to this notebook under the given alias."""
    if _HAS_STREAMLIT:
        try:
            return st.secrets[alias]
        except Exception as e:
            raise KeyError(
                f"Secret alias '{alias}' not found in notebook secrets. "
                f"Ensure ALTER NOTEBOOK ... SET SECRETS includes '{alias}'."
            ) from e
    raise RuntimeError("streamlit (st.secrets) is not available in this runtime.")

def get_password_secret(alias: str) -> Tuple[str, str]:
    """Get (username, password) from a PASSWORD-type Snowflake secret."""
    sec = _get_secret_obj(alias)
    if hasattr(sec, "username") and hasattr(sec, "password"):
        return sec.username, sec.password
    if isinstance(sec, dict) and "username" in sec and "password" in sec:
        return sec["username"], sec["password"]
    raise TypeError(f"Secret '{alias}' is not a PASSWORD secret.")

def get_generic_string_secret(alias: str) -> str:
    """Get a GENERIC_STRING-type Snowflake secret as a string."""
    sec = _get_secret_obj(alias)
    if isinstance(sec, str):
        return sec
    if isinstance(sec, dict):
        if "secret_string" in sec:
            return sec["secret_string"]
        if len(sec) == 1:
            return next(iter(sec.values()))
    return str(sec)

# ==========================================================
# Session and Run Configuration
# ==========================================================
session = get_active_session()

RUN_ID = str(uuid.uuid4())
LOG_TABLE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_INGEST_LOG"
CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
CONFIG_FILENAME = "practice_ingest_config_v17.json"

# Rate limiting
API_CALL_DELAY_MS = 100
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
RATE_LIMIT_RETRY_COUNT = 3
RATE_LIMIT_RETRY_DELAY_SECONDS = 30
DEFAULT_DAYS_BACK = 7

print("=" * 70)
print("ELLKAY DAILY INGEST")
print("=" * 70)
print(f"Run ID: {RUN_ID}")
print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ==========================================================
# Logging Function
# ==========================================================
def log_ingest(practice_name: str, file_type: str, source_type: str, source_name: str,
               target_table: str, step_name: str, row_count: int, status: str,
               start_time: float, end_time: float = None, error_message: str = None):
    """Log to RAW_INGEST_LOG table."""
    try:
        log_id = str(uuid.uuid4())
        if end_time is None:
            end_time = time.time()
        duration_seconds = round(end_time - start_time, 3)
        start_ts = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_ts = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        error_value = f"'{(error_message or '').replace(chr(39), chr(39)+chr(39))[:500]}'" if error_message else "NULL"
        
        insert_sql = f"""
            INSERT INTO {LOG_TABLE}
            (LOG_ID, PARENT_RUN_ID, PRACTICE_NAME, FILE_TYPE, SOURCE_TYPE, FILE_NAME,
             TARGET_TABLE, STEP_NAME, ROW_COUNT, STATUS, ERROR_MESSAGE,
             START_TIME, END_TIME, DURATION_SECONDS)
            VALUES (
                '{log_id}', '{RUN_ID}', '{practice_name}', '{file_type}', 
                '{source_type}', '{source_name}', '{target_table}', '{step_name}',
                {row_count}, '{status}', {error_value}, '{start_ts}', '{end_ts}', {duration_seconds}
            )
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        print(f"[WARN] Failed to write to log table: {e}")

# ==========================================================
# OAuth Authentication
# ==========================================================
def get_oauth_token(token_url: str, client_id: str, client_secret: str) -> str:
    """Get OAuth 2.0 access token using client credentials flow."""
    response = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        },
        timeout=30
    )
    
    if response.status_code != 200:
        raise Exception(f"OAuth token request failed: {response.status_code} - {response.text}")
    
    token_data = response.json()
    access_token = token_data.get('access_token')
    
    if not access_token:
        raise Exception(f"No access_token in response: {token_data}")
    
    return access_token

def build_api_headers(access_token: str, site_service_key: str) -> Dict[str, str]:
    """Build headers for ELLKAY API calls."""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "SiteServiceKey": site_service_key
    }

# ==========================================================
# Date Range
# ==========================================================
def get_date_range(days_back: int = None, hours_back: int = None) -> Dict:
    """Build date range object for requests that need it."""
    # Subtract 6 hours from UTC to avoid timezone validation errors
    end_date = datetime.utcnow() - timedelta(hours=6)
    
    if hours_back is not None:
        start_date = end_date - timedelta(hours=hours_back)
    elif days_back is not None:
        start_date = end_date - timedelta(days=days_back)
    else:
        start_date = end_date - timedelta(days=DEFAULT_DAYS_BACK)
    
    return {
        "start": start_date.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        "end": end_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    }

# ==========================================================
# API Call Functions
# ==========================================================
def api_call_with_retry(method: str, url: str, headers: Dict,
                        json_body: Optional[Dict] = None,
                        max_retries: int = MAX_RETRIES,
                        delay_ms: int = API_CALL_DELAY_MS) -> requests.Response:
    """Make API call with retry logic and rate limit handling."""
    last_exception = None
    rate_limit_retries = 0
    
    for attempt in range(max_retries):
        try:
            time.sleep(delay_ms / 1000)
            
            if method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=json_body or {}, timeout=120)
            else:
                response = requests.get(url, headers=headers, timeout=120)
            
            # Check for rate limit
            is_rate_limited = False
            if response.status_code == 429:
                is_rate_limited = True
            elif response.status_code == 400:
                try:
                    error_data = response.json()
                    if error_data.get('ErrorCode') == 'ExternalApiRateLimitReached':
                        is_rate_limited = True
                except:
                    pass
            
            if is_rate_limited:
                rate_limit_retries += 1
                if rate_limit_retries <= RATE_LIMIT_RETRY_COUNT:
                    print(f"      [WARN] Rate limited, waiting {RATE_LIMIT_RETRY_DELAY_SECONDS}s...")
                    time.sleep(RATE_LIMIT_RETRY_DELAY_SECONDS)
                    continue
                else:
                    return response
            
            if response.status_code < 500:
                return response
            
            last_exception = Exception(f"API returned {response.status_code}: {response.text[:200]}")
            
        except requests.exceptions.RequestException as e:
            last_exception = e
        
        if attempt < max_retries - 1:
            time.sleep(RETRY_DELAY_SECONDS * (attempt + 1))
    
    raise last_exception

def call_ellkay_endpoint(base_url: str, service: str, endpoint: str,
                         headers: Dict, request_body: Optional[Dict] = None,
                         delay_ms: int = API_CALL_DELAY_MS) -> List[Dict]:
    """Call an ELLKAY endpoint and return the results."""
    url = f"{base_url}/{service}/{endpoint}"
    
    response = api_call_with_retry('POST', url, headers, request_body, delay_ms=delay_ms)
    
    if response.status_code != 200:
        raise Exception(f"{endpoint} failed: {response.status_code} - {response.text[:500]}")
    
    data = response.json()
    
    # Extract records
    records = []
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                records = value
                break
        if not records and data:
            records = [data]
    
    return records

# ==========================================================
# Load ELLKAY Practices from Config
# ==========================================================
print("\n[INFO] Loading ELLKAY practices from config...")

session.sql("CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG TYPE = JSON").collect()

practices_sql = f"""
    SELECT DISTINCT
        p.value:practice_name::string AS practice_name,
        p.value:display_name::string AS display_name,
        p.value:site_service_key::string AS site_service_key
    FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
         LATERAL FLATTEN(input => cfg.$1:Practices) p
    WHERE p.value:site_service_key IS NOT NULL
"""

practices_df = session.sql(practices_sql).collect()
practices = [dict(row.asDict()) for row in practices_df]

print(f"[INFO] Found {len(practices)} ELLKAY practices:")
for p in practices:
    print(f"       - {p['PRACTICE_NAME']} ({p['DISPLAY_NAME']})")

# ==========================================================
# Main Processing Loop
# ==========================================================
run_start_time = time.time()
total_success = 0
total_failure = 0
all_results = []

# Log run start
log_ingest("ALL", "ALL", "orchestrator", "ELLKAY_DAILY_INGEST", "N/A",
           "RUN_START", 0, "RUNNING", run_start_time)

for practice_idx, practice_info in enumerate(practices, 1):
    practice_name = practice_info['PRACTICE_NAME']
    display_name = practice_info['DISPLAY_NAME']
    site_service_key = practice_info['SITE_SERVICE_KEY']
    
    print(f"\n{'='*70}")
    print(f"PRACTICE {practice_idx}/{len(practices)}: {display_name}")
    print(f"{'='*70}")
    
    practice_start_time = time.time()
    practice_success = 0
    practice_failure = 0
    
    # Log practice start
    log_ingest(practice_name, "ALL", "orchestrator", "practice_start", "N/A",
               "PRACTICE_START", 0, "RUNNING", practice_start_time)
    
    # =========================================================
    # Get OAuth token for this practice (once per practice)
    # =========================================================
    try:
        # Query first endpoint to get OAuth config (same for all endpoints in practice)
        auth_cfg_sql = f"""
            SELECT
                f.value:source:token_url::string AS token_url,
                f.value:source:auth:oauth_secret_alias::string AS oauth_secret_alias
            FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
                 LATERAL FLATTEN(input => cfg.$1:Practices) p,
                 LATERAL FLATTEN(input => p.value:ingest) f
            WHERE LOWER(p.value:practice_name::string) = LOWER('{practice_name}')
              AND f.value:source:token_url IS NOT NULL
            LIMIT 1
        """
        auth_cfg = session.sql(auth_cfg_sql).collect()[0]
        token_url = auth_cfg['TOKEN_URL']
        oauth_secret_alias = auth_cfg['OAUTH_SECRET_ALIAS']
        
        print(f"\n[INFO] Authenticating...")
        client_id, client_secret = get_password_secret(oauth_secret_alias)
        access_token = get_oauth_token(token_url, client_id, client_secret)
        headers = build_api_headers(access_token, site_service_key)
        print(f"[INFO] OAuth token acquired")
        
    except Exception as e:
        error_msg = str(e)[:500]
        print(f"[ERROR] Failed to authenticate for {practice_name}: {error_msg}")
        log_ingest(practice_name, "ALL", "api", "oauth", "N/A",
                   "AUTHENTICATE", 0, "ERROR", practice_start_time, error_message=error_msg)
        # Skip this practice entirely
        continue
    
    # =========================================================
    # Load all endpoints for this practice (in config order)
    # =========================================================
    endpoints_sql = f"""
        SELECT
            f.value:file_type::string AS file_type,
            f.value:source:api_base_url::string AS api_base_url,
            f.value:source:service::string AS service,
            f.value:source:endpoint::string AS endpoint,
            f.value:source:parameters::variant AS parameters,
            COALESCE(f.value:source:requires_date_range::boolean, false) AS requires_date_range,
            COALESCE(f.value:source:requires_body_auth::boolean, false) AS requires_body_auth,
            COALESCE(f.value:source:request_date_format::string, 'dateRange') AS request_date_format,
            f.value:source:pagination_type::string AS pagination_type,
            f.value:source:next_endpoint::string AS next_endpoint,
            COALESCE(f.value:source:requires_patient_loop::boolean, false) AS requires_patient_loop,
            f.value:source:patient_source_table::string AS patient_source_table,
            f.value:source:patient_id_column::string AS patient_id_column,
            COALESCE(f.value:source:api_delay_ms::int, 100) AS api_delay_ms,
            f.value:snowflake:database::string AS target_db,
            f.value:snowflake:schema::string AS target_schema,
            f.value:snowflake:table::string AS target_table,
            COALESCE(LOWER(f.value:snowflake:load_mode::string), 'append') AS load_mode
        FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
             LATERAL FLATTEN(input => cfg.$1:Practices) p,
             LATERAL FLATTEN(input => p.value:ingest) f
        WHERE LOWER(p.value:practice_name::string) = LOWER('{practice_name}')
          AND f.value:source:api_base_url IS NOT NULL
       
    """
    
    endpoints_df = session.sql(endpoints_sql).collect()
    endpoints = [dict(row.asDict()) for row in endpoints_df]
    
    print(f"[INFO] Found {len(endpoints)} endpoints to process")
    
    # =========================================================
    # Process each endpoint
    # =========================================================
    for ep_idx, ep_config in enumerate(endpoints, 1):
        file_type = ep_config['FILE_TYPE']
        api_base_url = ep_config['API_BASE_URL']
        service = ep_config['SERVICE']
        endpoint = ep_config['ENDPOINT']
        requires_date_range = ep_config['REQUIRES_DATE_RANGE']
        requires_body_auth = ep_config['REQUIRES_BODY_AUTH']
        request_date_format = ep_config['REQUEST_DATE_FORMAT']
        pagination_type = ep_config['PAGINATION_TYPE']
        next_endpoint = ep_config['NEXT_ENDPOINT']
        requires_patient_loop = ep_config['REQUIRES_PATIENT_LOOP']
        patient_source_table = ep_config['PATIENT_SOURCE_TABLE']
        patient_id_column = ep_config['PATIENT_ID_COLUMN']
        api_delay_ms = ep_config['API_DELAY_MS']
        fq_table = f"{ep_config['TARGET_DB']}.{ep_config['TARGET_SCHEMA']}.{ep_config['TARGET_TABLE']}"
        load_mode = ep_config['LOAD_MODE']
        
        # Parse parameters
        raw_params = ep_config['PARAMETERS']
        if raw_params is None:
            parameters = {}
        elif isinstance(raw_params, (dict, list)):
            parameters = raw_params if isinstance(raw_params, dict) else {}
        else:
            try:
                parameters = json.loads(raw_params)
            except:
                parameters = {}
        
        days_back = parameters.get('days_back', None)
        hours_back = parameters.get('hours_back', None)
        
        print(f"\n  [{ep_idx}/{len(endpoints)}] {file_type}...", end=" ", flush=True)
        
        endpoint_start_time = time.time()
        records = []
        
        try:
            # Build date range
            date_range = get_date_range(days_back=days_back, hours_back=hours_back)
            
            # Build request body
            request_body = None
            if requires_body_auth:
                request_body = {
                    "authentication": {
                        "subscriberKey": client_id,
                        "siteServiceKey": site_service_key
                    }
                }
                if requires_date_range:
                    if request_date_format == 'startEndDate':
                        request_body["request"] = {
                            "startDate": date_range['start'],
                            "endDate": date_range['end']
                        }
                    else:
                        request_body["request"] = {
                            "dateRange": date_range
                        }
            elif requires_date_range:
                if request_date_format == 'startEndDate':
                    request_body = {
                        "startDate": date_range['start'],
                        "endDate": date_range['end']
                    }
                else:
                    request_body = {"dateRange": date_range}
            
            # =====================================================
            # Execute based on pattern
            # =====================================================
            
            if requires_patient_loop:
                # -------------------------------------------------
                # Patient Loop Pattern
                # -------------------------------------------------
                patient_sql = f"SELECT DISTINCT {patient_id_column} FROM {patient_source_table}"
                patient_id_key = patient_id_column.strip('"')
                
                try:
                    patient_rows = session.sql(patient_sql).collect()
                    patient_ids = [str(row[patient_id_key]) for row in patient_rows]
                except Exception as e:
                    patient_ids = []
                
                if not patient_ids:
                    records = []
                else:
                    records = []
                    auth_body = {
                        "authentication": {
                            "subscriberKey": client_id,
                            "siteServiceKey": site_service_key
                        }
                    }
                    
                    for patient_id in patient_ids:
                        try:
                            patient_request_body = {
                                **auth_body,
                                "request": {"patientId": patient_id}
                            }
                            
                            if requires_date_range:
                                if request_date_format == 'startEndDate':
                                    patient_request_body["request"]["startDate"] = date_range['start']
                                    patient_request_body["request"]["endDate"] = date_range['end']
                                else:
                                    patient_request_body["request"]["dateRange"] = date_range
                            
                            patient_records = call_ellkay_endpoint(
                                api_base_url, service, endpoint, headers,
                                patient_request_body, delay_ms=api_delay_ms
                            )
                            
                            if patient_records:
                                for rec in patient_records:
                                    if isinstance(rec, dict):
                                        rec['_source_patient_id'] = patient_id
                                records.extend(patient_records)
                                
                        except Exception as e:
                            # Continue with next patient
                            continue
            
            elif pagination_type == 'beginNext':
                # -------------------------------------------------
                # Begin/Next Pagination Pattern
                # -------------------------------------------------
                begin_response = call_ellkay_endpoint(
                    api_base_url, service, endpoint, headers,
                    request_body, delay_ms=api_delay_ms
                )
                
                next_token = None
                if begin_response and len(begin_response) > 0:
                    if isinstance(begin_response[0], dict) and 'nextToken' in begin_response[0]:
                        next_token = begin_response[0]['nextToken']
                    elif isinstance(begin_response, dict) and 'nextToken' in begin_response:
                        next_token = begin_response['nextToken']
                
                if not next_token:
                    records = []
                else:
                    records = []
                    page_count = 0
                    max_pages = 1000
                    
                    auth_body = {
                        "authentication": {
                            "subscriberKey": client_id,
                            "siteServiceKey": site_service_key
                        }
                    }
                    
                    while next_token and page_count < max_pages:
                        page_count += 1
                        
                        next_request_body = {
                            **auth_body,
                            "token": {"nextToken": next_token}
                        }
                        
                        page_response = call_ellkay_endpoint(
                            api_base_url, service, next_endpoint, headers,
                            next_request_body, delay_ms=api_delay_ms
                        )
                        
                        page_records = []
                        next_token = None
                        
                        if page_response:
                            for item in page_response:
                                if isinstance(item, dict):
                                    if 'nextToken' in item:
                                        next_token = item.get('nextToken')
                                    else:
                                        page_records.append(item)
                        
                        records.extend(page_records)
                        
                        if not next_token:
                            break
            
            else:
                # -------------------------------------------------
                # Standard Single-Call Pattern
                # -------------------------------------------------
                records = call_ellkay_endpoint(
                    api_base_url, service, endpoint, headers,
                    request_body, delay_ms=api_delay_ms
                )
            
            # =====================================================
            # Write to Snowflake
            # =====================================================
            if records:
                df_pandas = pd.DataFrame(records)
                
                # Flatten nested structures to JSON strings
                for col_name in df_pandas.columns:
                    if df_pandas[col_name].apply(lambda x: isinstance(x, (dict, list))).any():
                        df_pandas[col_name] = df_pandas[col_name].apply(
                            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                        )
                
                df_snowpark = session.create_dataframe(df_pandas)
                
                df_with_meta = (
                    df_snowpark
                    .withColumn("_FILE_NAME", lit(f"ellkay_{file_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
                    .withColumn("_FILE_LOAD_TIME", current_timestamp())
                    .withColumn("_PARENT_RUN_ID", lit(RUN_ID))
                )
                
                df_with_meta.write.mode(load_mode).save_as_table(fq_table)
            
            # =====================================================
            # Log Success
            # =====================================================
            endpoint_end_time = time.time()
            duration = round(endpoint_end_time - endpoint_start_time, 2)
            
            print(f"SUCCESS ({len(records)} rows, {duration}s)")
            
            log_ingest(practice_name, file_type, "api", f"{service}/{endpoint}", fq_table,
                       "ENDPOINT_COMPLETE", len(records), "SUCCESS",
                       endpoint_start_time, endpoint_end_time)
            
            practice_success += 1
            total_success += 1
            all_results.append({
                "practice": practice_name,
                "endpoint": file_type,
                "success": True,
                "rows": len(records),
                "duration": duration
            })
            
        except Exception as e:
            # =====================================================
            # Log Failure and Continue
            # =====================================================
            endpoint_end_time = time.time()
            duration = round(endpoint_end_time - endpoint_start_time, 2)
            error_msg = str(e)[:500]
            
            print(f"FAILED ({duration}s)")
            print(f"      Error: {error_msg[:80]}...")
            
            log_ingest(practice_name, file_type, "api", f"{service}/{endpoint}", fq_table,
                       "ENDPOINT_COMPLETE", 0, "ERROR",
                       endpoint_start_time, endpoint_end_time, error_msg)
            
            practice_failure += 1
            total_failure += 1
            all_results.append({
                "practice": practice_name,
                "endpoint": file_type,
                "success": False,
                "rows": 0,
                "duration": duration,
                "error": error_msg
            })
            
            # Continue to next endpoint
            continue
    
    # =========================================================
    # Log Practice Complete
    # =========================================================
    practice_end_time = time.time()
    practice_duration = round(practice_end_time - practice_start_time, 2)
    practice_status = "SUCCESS" if practice_failure == 0 else "PARTIAL"
    
    log_ingest(practice_name, "ALL", "orchestrator", "practice_complete", "N/A",
               "PRACTICE_COMPLETE", practice_success, practice_status,
               practice_start_time, practice_end_time,
               f"{practice_failure} endpoint(s) failed" if practice_failure > 0 else None)
    
    print(f"\n  Practice Complete: {practice_success} succeeded, {practice_failure} failed, {practice_duration}s")

# ==========================================================
# Final Summary
# ==========================================================
run_end_time = time.time()
run_duration = round(run_end_time - run_start_time, 2)
run_status = "SUCCESS" if total_failure == 0 else "PARTIAL"

log_ingest("ALL", "ALL", "orchestrator", "ELLKAY_DAILY_INGEST", "N/A",
           "RUN_COMPLETE", total_success, run_status,
           run_start_time, run_end_time,
           f"{total_failure} endpoint(s) failed" if total_failure > 0 else None)

print("\n")
print("=" * 70)
print("RUN COMPLETE")
print("=" * 70)
print(f"Run ID: {RUN_ID}")
print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {run_duration} seconds ({round(run_duration/60, 1)} minutes)")
print(f"Endpoints Processed: {len(all_results)}")
print(f"Successful: {total_success}")
print(f"Failed: {total_failure}")
print(f"Status: {run_status}")
print("=" * 70)

if total_failure > 0:
    print("\nFAILED ENDPOINTS:")
    print("-" * 70)
    for r in all_results:
        if not r["success"]:
            print(f"  {r['practice']} / {r['endpoint']}")
            if 'error' in r:
                print(f"    Error: {r['error'][:80]}...")
    print("-" * 70)
    print(f"\nQuery failures:")
    print(f"  SELECT * FROM {LOG_TABLE}")
    print(f"  WHERE PARENT_RUN_ID = '{RUN_ID}' AND STATUS = 'ERROR'")
    print(f"  ORDER BY START_TIME;")

print("\n[DONE]")
