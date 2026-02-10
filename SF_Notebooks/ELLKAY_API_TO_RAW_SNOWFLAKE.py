# ELLKAY_API_TO_RAW_SNOWFLAKE.py
# ==========================================================
# Notebook for loading ELLKAY LKCloud API data into Snowflake
# Uses OAuth 2.0 authentication
# ==========================================================
#
# API DETAILS:
# ==========================================================
# Token URL: https://auth2.lkidentity.com/connect/token
# API Base URL (Sandbox): https://services.lkstaging.com
# API Base URL (Production): https://services.lkcloud.com
# Auth: OAuth 2.0 Bearer Token + SiteServiceKey header
#
# ==========================================================

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
import pandas as pd
import requests
import json
import uuid
import time
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from snowflake.snowpark.context import get_active_session
import os

# ==========================================================
# Secrets helper (Snowflake Secret objects via Notebook st.secrets)
# ==========================================================
try:
    import streamlit as st  # available in Snowflake Notebooks
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
    raise RuntimeError(
        "streamlit (st.secrets) is not available in this runtime. "
        "Run this in a Snowflake Notebook, or provide environment variable fallbacks."
    )

def get_password_secret(alias: str) -> Tuple[str, str]:
    """Get (username, password) from a PASSWORD-type Snowflake secret."""
    sec = _get_secret_obj(alias)
    # Snowflake Notebook secrets typically expose PASSWORD secrets with username/password attributes
    if hasattr(sec, "username") and hasattr(sec, "password"):
        return sec.username, sec.password
    if isinstance(sec, dict) and "username" in sec and "password" in sec:
        return sec["username"], sec["password"]
    raise TypeError(f"Secret '{alias}' is not a PASSWORD secret (missing username/password).")

def get_generic_string_secret(alias: str) -> str:
    """Get a GENERIC_STRING-type Snowflake secret as a string."""
    sec = _get_secret_obj(alias)
    if isinstance(sec, str):
        return sec
    if isinstance(sec, dict):
        # Some runtimes may return dict-like values; try common shapes
        if "secret_string" in sec:
            return sec["secret_string"]
        if len(sec) == 1:
            return next(iter(sec.values()))
    # Last resort
    return str(sec)

# ==========================================================
# 0) Session initialization
# ==========================================================
session = get_active_session()

PARENT_RUN_ID = str(uuid.uuid4())
LOG_TABLE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_INGEST_LOG"

# Default lookback period
DEFAULT_DAYS_BACK = 7

# Rate limiting defaults
API_CALL_DELAY_MS = 100
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# ==========================================================
# 1) PARAMETER HANDLING
# ==========================================================
def _strip_wrapping_quotes(s: str) -> str:
    if not s:
        return s
    s2 = s.strip()
    if len(s2) >= 2 and s2[0] == s2[-1] and s2[0] in ("'", '"'):
        return s2[1:-1]
    return s2

def parse_parameters():
    practices = ["ellkay"]
    file_types = ["Facilities"]  # Default to simplest endpoint

    print(f"[DEBUG] sys.argv = {sys.argv}")
    
    argv = []
    for a in (sys.argv or []):
        if a is None:
            continue
        s = _strip_wrapping_quotes(str(a).strip())
        if not s or s.startswith("-"):
            continue
        argv.append(s)

    if not argv:
        print("[INFO] No parameters provided, using defaults")
        return practices, file_types

    pipe_arg = next((a for a in argv if "|" in a), None)
    if pipe_arg is not None:
        parts = pipe_arg.split("|", 1)
        practice_param = _strip_wrapping_quotes(parts[0].strip())
        file_type_param = _strip_wrapping_quotes(parts[1].strip()) if len(parts) > 1 else ""
        if practice_param and practice_param.lower() not in ("null", "none", ""):
            practices = [practice_param]
        if file_type_param and file_type_param.lower() not in ("null", "none", ""):
            file_types = [file_type_param]
        return practices, file_types

    if len(argv) >= 2:
        practices = [_strip_wrapping_quotes(argv[0].strip())]
        file_types = [_strip_wrapping_quotes(argv[1].strip())]
        return practices, file_types

    practices = [_strip_wrapping_quotes(argv[0].strip())]
    return practices, file_types


PRACTICES_TO_RUN, FILE_TYPES_TO_RUN = parse_parameters()
PRACTICE_NAME = PRACTICES_TO_RUN[0] if PRACTICES_TO_RUN else None
FILE_TYPE = FILE_TYPES_TO_RUN[0] if FILE_TYPES_TO_RUN else None

print(f"[INFO] ========== ELLKAY API INGEST STARTED ==========")
print(f"[INFO] Parent Run ID: {PARENT_RUN_ID}")
print(f"[INFO] Practice: {PRACTICE_NAME or 'NONE'}")
print(f"[INFO] File Type: {FILE_TYPE or 'NONE'}")


# ==========================================================
# 2) Parameter requirement enforcement
# ==========================================================
if not PRACTICE_NAME or not FILE_TYPE:
    print("\n" + "-" * 60)
    print("[INFO] Parameters are required for this notebook to run.")
    print(f"[INFO] Received: practice={PRACTICE_NAME or 'NONE'}, file_type={FILE_TYPE or 'NONE'}")
    print("-" * 60)
else:
    # ==========================================================
    # 3) Load config from JSON
    # ==========================================================
    CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
    CONFIG_FILENAME = "practice_ingest_config_v17.json"

    session.sql("""
        CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG
        TYPE = JSON
    """).collect()

    cfg_sql = f"""
        SELECT
          LOWER(p.value:practice_name::string)                         AS practice_name,
          p.value:site_service_key::string                             AS site_service_key,
          f.value:file_type::string                                    AS file_type,
          COALESCE(f.value:source_type::string, 'api')                 AS source_type,
          f.value:source:api_base_url::string                          AS api_base_url,
          f.value:source:token_url::string                             AS token_url,
          f.value:source:auth:oauth_secret_alias::string               AS oauth_secret_alias,
          f.value:source:auth:site_service_key_secret_alias::string    AS site_service_key_secret_alias,
          f.value:source:endpoint::string                              AS endpoint,
          f.value:source:service::string                               AS service,
          f.value:source:parameters::variant                           AS parameters,
          COALESCE(f.value:source:requires_date_range::boolean, false) AS requires_date_range,
          COALESCE(f.value:source:requires_body_auth::boolean, false)  AS requires_body_auth,
          COALESCE(f.value:source:request_date_format::string, 'dateRange') AS request_date_format,
          f.value:source:pagination_type::string                       AS pagination_type,
          f.value:source:next_endpoint::string                         AS next_endpoint,
          COALESCE(f.value:source:requires_patient_loop::boolean, false) AS requires_patient_loop,
          f.value:source:patient_source_table::string                  AS patient_source_table,
          f.value:source:patient_id_column::string                     AS patient_id_column,
          COALESCE(f.value:source:api_delay_ms::int, 100)              AS api_delay_ms,
          f.value:snowflake:database::string                           AS target_db,
          f.value:snowflake:schema::string                             AS target_schema,
          f.value:snowflake:table::string                              AS target_table,
          LOWER(f.value:snowflake:load_mode::string)                   AS load_mode
        FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
             LATERAL FLATTEN(input => cfg.$1:Practices) p,
             LATERAL FLATTEN(input => p.value:ingest) f
        WHERE LOWER(p.value:practice_name::string) = '{PRACTICE_NAME.lower()}'
          AND LOWER(f.value:file_type::string) = '{FILE_TYPE.lower()}'
    """

    cfg_df = session.sql(cfg_sql)
    cfg_rows = cfg_df.collect()

    if not cfg_rows:
        raise ValueError(f"No config found for practice={PRACTICE_NAME}, file_type={FILE_TYPE}")

    config = cfg_rows[0]
    print(f"[INFO] Config loaded successfully")
    print(f"[INFO] API Base URL: {config['API_BASE_URL']}")
    print(f"[INFO] Token URL: {config['TOKEN_URL']}")
    print(f"[INFO] Service: {config['SERVICE']}")
    print(f"[INFO] Endpoint: {config['ENDPOINT']}")
    print(f"[INFO] Target table: {config['TARGET_DB']}.{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}")

    # ==========================================================
    # 4) Functions
    # ==========================================================
    def generate_guid():
        return str(uuid.uuid4())

    def log_ingest(practice_name, file_type, source_type, source_name, target_table, 
                   step_name, row_count, status, start_time, end_time=None, error_message=None):
        try:
            log_id = generate_guid()
            if end_time is None:
                end_time = time.time()
            duration_seconds = round(end_time - start_time, 3)
            start_ts = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            end_ts = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            error_value = f"'{(error_message or '').replace(chr(39), chr(39)+chr(39))}'" if error_message else "NULL"
            
            insert_sql = f"""
                INSERT INTO {LOG_TABLE}
                (LOG_ID, PARENT_RUN_ID, PRACTICE_NAME, FILE_TYPE, SOURCE_TYPE, FILE_NAME,
                 TARGET_TABLE, STEP_NAME, ROW_COUNT, STATUS, ERROR_MESSAGE,
                 START_TIME, END_TIME, DURATION_SECONDS)
                VALUES (
                    '{log_id}', '{PARENT_RUN_ID}', '{practice_name}', '{file_type}', 
                    '{source_type}', '{source_name}', '{target_table}', '{step_name}',
                    {row_count}, '{status}', {error_value}, '{start_ts}', '{end_ts}', {duration_seconds}
                )
            """
            session.sql(insert_sql).collect()
            print(f"[LOG] {step_name}: {practice_name}/{file_type} - {status} ({row_count} rows, {duration_seconds}s)")
        except Exception as e:
            print(f"[WARN] Failed to write to log table: {e}")

    # ==========================================================
    # 5) OAuth 2.0 Authentication
    # ==========================================================
    def get_oauth_token(token_url: str, client_id: str, client_secret: str) -> str:
        """
        Get OAuth 2.0 access token using client credentials flow.
        
        POST https://auth2.lkidentity.com/connect/token
        Content-Type: application/x-www-form-urlencoded
        
        grant_type=client_credentials
        client_id={client_id}
        client_secret={client_secret}
        """
        print(f"[INFO] Requesting OAuth token from {token_url}...")
        
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
        expires_in = token_data.get('expires_in', 3600)
        
        if not access_token:
            raise Exception(f"No access_token in response: {token_data}")
        
        print(f"[INFO] OAuth token acquired, expires in {expires_in} seconds")
        return access_token

    def build_api_headers(access_token: str, site_service_key: str) -> Dict[str, str]:
        """
        Build headers for ELLKAY API calls.
        
        Authorization: Bearer {access_token}
        SiteServiceKey: {site_service_key}
        Content-Type: application/json
        """
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "SiteServiceKey": site_service_key
        }

    # ==========================================================
    # 6) API Call Functions
    # ==========================================================
    def get_date_range(days_back: int = None, hours_back: int = None) -> Dict:
        """Build date range object for requests that need it."""
        # Subtract 6 hours from UTC to avoid 'End date cannot be greater than today'
        # errors from APIs that validate dates in US timezones
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

    # Rate limit retry settings
    RATE_LIMIT_RETRY_COUNT = 3
    RATE_LIMIT_RETRY_DELAY_SECONDS = 30

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
                
                # Check for rate limit (429 or 400 with rate limit error)
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
                        print(f"[WARN] Rate limited, waiting {RATE_LIMIT_RETRY_DELAY_SECONDS}s before retry {rate_limit_retries}/{RATE_LIMIT_RETRY_COUNT}")
                        time.sleep(RATE_LIMIT_RETRY_DELAY_SECONDS)
                        # Don't increment attempt counter for rate limits - retry same attempt
                        continue
                    else:
                        print(f"[ERROR] Rate limit retries exhausted ({RATE_LIMIT_RETRY_COUNT} attempts)")
                        return response
                
                # Success or client error (don't retry 4xx except rate limits)
                if response.status_code < 500:
                    return response
                
                print(f"[WARN] API returned {response.status_code}, attempt {attempt + 1}/{max_retries}")
                last_exception = Exception(f"API returned {response.status_code}: {response.text[:200]}")
                
            except requests.exceptions.RequestException as e:
                print(f"[WARN] Request failed, attempt {attempt + 1}/{max_retries}: {e}")
                last_exception = e
            
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY_SECONDS * (attempt + 1))
        
        raise last_exception

    def call_ellkay_endpoint(base_url: str, service: str, endpoint: str, 
                              headers: Dict, request_body: Optional[Dict] = None,
                              delay_ms: int = API_CALL_DELAY_MS) -> List[Dict]:
        """
        Call an ELLKAY endpoint and return the results.
        
        URL format: {base_url}/{service}/{endpoint}
        Example: https://services.lkcloud.com/LKAppointments/GetFacilities
        """
        url = f"{base_url}/{service}/{endpoint}"
        
        print(f"[INFO] Calling {endpoint}...")
        print(f"[DEBUG] POST {url}")
        if request_body:
            print(f"[DEBUG] Body: {json.dumps(request_body, indent=2)[:500]}...")
        
        response = api_call_with_retry('POST', url, headers, request_body, delay_ms=delay_ms)
        
        print(f"[INFO] Response status: {response.status_code}")
        
        if response.status_code != 200:
            error_text = response.text[:500]
            print(f"[ERROR] API error response: {error_text}")
            raise Exception(f"{endpoint} failed: {response.status_code} - {error_text}")
        
        data = response.json()
        
        # Debug: show response structure
        if isinstance(data, dict):
            print(f"[DEBUG] Response keys: {list(data.keys())}")
        else:
            print(f"[DEBUG] Response is a list with {len(data)} items")
        
        # Extract records - handle different response structures
        records = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Look for any key that contains a list (generic approach)
            for key, value in data.items():
                if isinstance(value, list):
                    print(f"[DEBUG] Found list in key '{key}' with {len(value)} items")
                    records = value
                    break
            # If no list found, treat the dict as a single record
            if not records and data:
                records = [data]
        
        print(f"[INFO] Retrieved {len(records)} records from {endpoint}")
        return records

    # ==========================================================
    # 7) Main ETL Logic
    # ==========================================================
    api_base_url = config['API_BASE_URL']
    token_url = config['TOKEN_URL']
    oauth_secret_alias = config['OAUTH_SECRET_ALIAS']
    site_key_secret_alias = config['SITE_SERVICE_KEY_SECRET_ALIAS']

    client_id, client_secret = get_password_secret(oauth_secret_alias)
    
    # Use site_service_key from config if present, otherwise fall back to secret
    if config['SITE_SERVICE_KEY']:
        site_service_key = config['SITE_SERVICE_KEY']
        print(f"[INFO] Using site_service_key from config")
    else:
        site_service_key = get_generic_string_secret(site_key_secret_alias)
        print(f"[INFO] Using site_service_key from secret '{site_key_secret_alias}'")
    
    service = config['SERVICE']
    endpoint = config['ENDPOINT']
    requires_date_range = config['REQUIRES_DATE_RANGE']
    requires_body_auth = config['REQUIRES_BODY_AUTH']
    request_date_format = config['REQUEST_DATE_FORMAT']  # 'dateRange' or 'startEndDate'
    pagination_type = config['PAGINATION_TYPE']  # None or 'beginNext'
    next_endpoint = config['NEXT_ENDPOINT']  # For beginNext pagination
    requires_patient_loop = config['REQUIRES_PATIENT_LOOP']  # True if need to loop through patients
    patient_source_table = config['PATIENT_SOURCE_TABLE']  # Table to get patient IDs from
    patient_id_column = config['PATIENT_ID_COLUMN']  # Column name for patient ID
    api_delay_ms = config['API_DELAY_MS']  # Delay between API calls in milliseconds
    fq_table = f"{config['TARGET_DB']}.{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}"
    load_mode = config['LOAD_MODE'] or 'append'
    
    print(f"[INFO] API delay: {api_delay_ms}ms")

    # Parse parameters
    raw_params = config['PARAMETERS']
    if raw_params is None:
        parameters = {}
    elif isinstance(raw_params, (dict, list)):
        parameters = raw_params if isinstance(raw_params, dict) else {}
    else:
        try:
            parameters = json.loads(raw_params)
        except Exception:
            parameters = {}

    days_back = parameters.get('days_back', None)
    hours_back = parameters.get('hours_back', None)

    etl_start_time = time.time()
    records = []

    try:
        # Step 1: Get OAuth token
        print(f"\n[INFO] Step 1: Authenticating with OAuth 2.0...")
        auth_start_time = time.time()
        
        access_token = get_oauth_token(token_url, client_id, client_secret)
        headers = build_api_headers(access_token, site_service_key)
        
        print(f"[INFO] Auth configured (oauth_secret='{oauth_secret_alias}')")
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "oauth", fq_table,
                   "AUTHENTICATE", 0, "SUCCESS", auth_start_time)

        # Step 2: Build request body
        request_body = None
        date_range = get_date_range(days_back=days_back, hours_back=hours_back)
        
        if requires_body_auth:
            # Build body with authentication object (per ELLKAY API spec)
            request_body = {
                "authentication": {
                    "subscriberKey": client_id,
                    "siteServiceKey": site_service_key
                }
            }
            if requires_date_range:
                if request_date_format == 'startEndDate':
                    # Format: request.startDate / request.endDate
                    request_body["request"] = {
                        "startDate": date_range['start'],
                        "endDate": date_range['end']
                    }
                else:
                    # Default format: request.dateRange.start / request.dateRange.end
                    request_body["request"] = {
                        "dateRange": date_range
                    }
                print(f"[INFO] Date range: {date_range['start']} to {date_range['end']}")
            print(f"[INFO] Using body authentication")
        elif requires_date_range:
            if request_date_format == 'startEndDate':
                request_body = {
                    "startDate": date_range['start'],
                    "endDate": date_range['end']
                }
            else:
                request_body = {
                    "dateRange": date_range
                }
            print(f"[INFO] Date range: {date_range['start']} to {date_range['end']}")

        # Step 3: Fetch data
        print(f"\n[INFO] Step 2: Fetching {FILE_TYPE} data...")
        fetch_start_time = time.time()
        
        if requires_patient_loop:
            # Patient loop pattern - call API for each patient
            print(f"[INFO] Using patient loop pattern")
            print(f"[INFO] Patient source: {patient_source_table}.{patient_id_column}")
            
            # Query patient IDs from source table
            patient_sql = f"SELECT DISTINCT {patient_id_column} FROM {patient_source_table}"
            print(f"[DEBUG] Patient query: {patient_sql}")
            
            # Strip quotes from column name for row access
            patient_id_key = patient_id_column.strip('"')
            
            try:
                patient_rows = session.sql(patient_sql).collect()
                patient_ids = [str(row[patient_id_key]) for row in patient_rows]
                print(f"[INFO] Found {len(patient_ids)} patients to process")
            except Exception as e:
                print(f"[ERROR] Failed to query patient IDs: {e}")
                patient_ids = []
            
            if not patient_ids:
                print(f"[WARN] No patient IDs found - no data to fetch")
                records = []
            else:
                records = []
                success_count = 0
                error_count = 0
                
                # Build auth body
                auth_body = {
                    "authentication": {
                        "subscriberKey": client_id,
                        "siteServiceKey": site_service_key
                    }
                }
                
                for i, patient_id in enumerate(patient_ids, 1):
                    try:
                        if i % 10 == 0 or i == 1:
                            print(f"[INFO] Processing patient {i} of {len(patient_ids)} (ID: {patient_id})...")
                        
                        # Build request body with patient ID
                        patient_request_body = {
                            **auth_body,
                            "request": {
                                "patientId": patient_id
                            }
                        }
                        
                        # Add date range if required
                        if requires_date_range:
                            if request_date_format == 'startEndDate':
                                patient_request_body["request"]["startDate"] = date_range['start']
                                patient_request_body["request"]["endDate"] = date_range['end']
                            else:
                                patient_request_body["request"]["dateRange"] = date_range
                        
                        # Call API
                        patient_records = call_ellkay_endpoint(api_base_url, service, endpoint, headers, patient_request_body, delay_ms=api_delay_ms)
                        
                        if patient_records:
                            # Add patient_id to each record for tracking
                            for rec in patient_records:
                                if isinstance(rec, dict):
                                    rec['_source_patient_id'] = patient_id
                            records.extend(patient_records)
                        
                        success_count += 1
                        
                    except Exception as e:
                        error_count += 1
                        print(f"[WARN] Failed for patient {patient_id}: {str(e)[:100]}")
                        # Continue with next patient
                        continue
                
                print(f"[INFO] Patient loop complete: {success_count} successful, {error_count} errors, {len(records)} total records")
        
        elif pagination_type == 'beginNext':
            # Begin/Next pagination pattern
            print(f"[INFO] Using Begin/Next pagination pattern")
            
            # Step 3a: Call Begin endpoint to get initial nextToken
            print(f"[INFO] Calling Begin endpoint: {endpoint}")
            begin_response = call_ellkay_endpoint(api_base_url, service, endpoint, headers, request_body, delay_ms=api_delay_ms)
            
            # Extract nextToken from response
            next_token = None
            if begin_response and len(begin_response) > 0:
                if isinstance(begin_response[0], dict) and 'nextToken' in begin_response[0]:
                    next_token = begin_response[0]['nextToken']
                elif isinstance(begin_response, dict) and 'nextToken' in begin_response:
                    next_token = begin_response['nextToken']
            
            if not next_token:
                print(f"[WARN] No nextToken returned from Begin endpoint - no data to fetch")
                records = []
            else:
                print(f"[INFO] Got nextToken, calling Next endpoint: {next_endpoint}")
                records = []
                page_count = 0
                max_pages = 1000  # Safety limit
                
                # Build auth body for Next calls
                auth_body = {
                    "authentication": {
                        "subscriberKey": client_id,
                        "siteServiceKey": site_service_key
                    }
                }
                
                while next_token and page_count < max_pages:
                    page_count += 1
                    
                    # Build Next request body
                    next_request_body = {
                        **auth_body,
                        "token": {
                            "nextToken": next_token
                        }
                    }
                    
                    print(f"[INFO] Fetching page {page_count}...")
                    page_response = call_ellkay_endpoint(api_base_url, service, next_endpoint, headers, next_request_body, delay_ms=api_delay_ms)
                    
                    # Extract records and nextToken from response
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
                    print(f"[INFO] Page {page_count}: {len(page_records)} records (total: {len(records)})")
                    
                    if not next_token:
                        print(f"[INFO] No more pages (nextToken is empty)")
                        break
                
                print(f"[INFO] Pagination complete: {page_count} pages, {len(records)} total records")
        else:
            # Standard single-call fetch
            records = call_ellkay_endpoint(api_base_url, service, endpoint, headers, request_body, delay_ms=api_delay_ms)
        
        fetch_end_time = time.time()
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", f"{service}/{endpoint}", fq_table,
                   "FETCH_DATA", len(records), "SUCCESS", fetch_start_time, fetch_end_time)

        # Step 4: Write to Snowflake
        print(f"\n[INFO] Step 3: Writing {len(records)} records to {fq_table}...")
        write_start_time = time.time()
        
        if not records:
            print("[INFO] No records to write")
            log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "api_response", fq_table,
                       "WRITE_TABLE", 0, "SUCCESS", write_start_time)
        else:
            # Convert to pandas DataFrame
            df_pandas = pd.DataFrame(records)
            
            # Flatten nested structures to JSON strings
            for col_name in df_pandas.columns:
                if df_pandas[col_name].apply(lambda x: isinstance(x, (dict, list))).any():
                    df_pandas[col_name] = df_pandas[col_name].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )
            
            print(f"[INFO] DataFrame created: {len(df_pandas)} rows, {len(df_pandas.columns)} columns")
            print(f"[INFO] Columns: {df_pandas.columns.tolist()}")
            
            # Convert to Snowpark DataFrame
            df_snowpark = session.create_dataframe(df_pandas)
            
            # Add metadata columns
            df_with_meta = (
                df_snowpark
                .withColumn("_FILE_NAME", lit(f"ellkay_{FILE_TYPE}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
                .withColumn("_FILE_LOAD_TIME", current_timestamp())
                .withColumn("_PARENT_RUN_ID", lit(PARENT_RUN_ID))
            )
            
            # Check pre-load count
            try:
                before_cnt = session.sql(f"SELECT COUNT(*) AS C FROM {fq_table}").collect()[0]["C"]
                print(f"[INFO] Pre-load row count: {before_cnt}")
            except:
                print(f"[INFO] Target table {fq_table} does not exist; it will be created.")
                before_cnt = -1
            
            # Write data
            df_with_meta.write.mode(load_mode).save_as_table(fq_table)
            
            write_end_time = time.time()
            
            # Check post-load count
            after_cnt = session.sql(f"SELECT COUNT(*) AS C FROM {fq_table}").collect()[0]["C"]
            print(f"[SUCCESS] Wrote {len(records)} row(s) to {fq_table}")
            print(f"[INFO] Post-load row count: {after_cnt}")
            
            log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "api_response", fq_table,
                       "WRITE_TABLE", len(records), "SUCCESS", write_start_time, write_end_time)
        
        # Final success log
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", f"{service}/{endpoint}", fq_table,
                   "PROCESS_COMPLETE", len(records), "SUCCESS", etl_start_time)
        
    except Exception as e:
        etl_end_time = time.time()
        error_msg = str(e)[:500]
        print(f"\n[ERROR] ETL failed: {error_msg}")
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", f"{service}/{endpoint}", fq_table,
                   "PROCESS_COMPLETE", 0, "ERROR", etl_start_time, etl_end_time, error_msg)
        raise

    # ==========================================================
    # 8) Summary
    # ==========================================================
    etl_end_time = time.time()
    etl_duration = round(etl_end_time - etl_start_time, 3)

    print("\n" + "-" * 60)
    print("[SUMMARY]")
    print(f"  Parent Run ID     : {PARENT_RUN_ID}")
    print(f"  Practice          : {PRACTICE_NAME}")
    print(f"  File Type         : {FILE_TYPE}")
    print(f"  Endpoint          : {service}/{endpoint}")
    print(f"  Target Table      : {fq_table}")
    print(f"  Records Processed : {len(records)}")
    print(f"  Total Duration    : {etl_duration} seconds")
    print("-" * 60)
    print("\n[DONE] ELLKAY API ingest completed.")
