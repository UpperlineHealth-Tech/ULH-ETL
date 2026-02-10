# ADLS_FILE_PRECHECK.py
# ==========================================================
# Notebook Name: ADLS_FILE_PRECHECK
# Location: UPPERLINE.RAW_INBOUND_AFFILIATEDATA.ADLS_FILE_PRECHECK
# ==========================================================
#
# Pre-ingestion validation script to check files before loading to Snowflake.
# For the given practice/file_type:
# - Validates file exists and is readable
# - Validates delimiter matches config
# - Validates headers match expected schema (column names, count, order)
# - Checks for problematic special characters in headers
# - Validates minimum row count expectations
# - Checks for BOM (Byte Order Mark) issues
# - Logs detailed results to PRECHECK_INGEST_LOG table
# - On FAILURE: Calls Logic App to move failed files to error location
# - On SUCCESS: Triggers RAW_ADLS_TO_RAW_SNOWFLAKE notebook for ingestion
#
# ==========================================================
# PIPELINE FLOW:
# ==========================================================
#
#   ADLS Blob Created (Event Grid)
#       ↓
#   Logic App triggers SP_EXEC_PRECHECK_NOTEBOOK
#       ↓
#   ADLS_FILE_PRECHECK notebook (this script)
#       ├── FAIL → Call Archive Logic App → Move files to error location
#       └── PASS → Call SP_RAW_ADLS_TO_RAW_SNOWFLAKE → Practices_ETL
#                       ↓
#                  RAW_ADLS_TO_RAW_SNOWFLAKE notebook
#                       ↓
#                  Archive files via Logic App
#
# ==========================================================
# PARAMETER REQUIREMENT:
# ==========================================================
# This notebook is triggered from Azure Logic App via stored procedure:
#
#   CALL UPPERLINE.RAW_INBOUND_AFFILIATEDATA.SP_EXEC_PRECHECK_NOTEBOOK('practice', 'file_type')
#
# On SUCCESS: Triggers RAW_ADLS_TO_RAW_SNOWFLAKE for ingestion
# On FAILURE: Moves files to error location, does NOT trigger ingestion
#
# ==========================================================

import json
import sys
import re
import uuid
import time
import csv
from io import StringIO
from datetime import datetime
from snowflake.snowpark.context import get_active_session

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    import urllib.request
    import urllib.error
    HAS_REQUESTS = False
    print("[INFO] requests library not available, using urllib")

# ==========================================================
# 0) Session & config
# ==========================================================
session = get_active_session()
print(session)

PARENT_RUN_ID = str(uuid.uuid4())

CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
CONFIG_FILENAME = "practice_ingest_config_v17.json"

# Log table for tracking precheck results
PRECHECK_LOG_TABLE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.PRECHECK_INGEST_LOG"

# Logic App URL for file archiving/error handling
# This Logic App triggers ADF to move files to archive or error locations
ARCHIVE_LOGIC_APP_URL = "https://prod-28.northcentralus.logic.azure.com:443/workflows/5b7115640e194503a3eacf6fa004874b/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=Xe2zUAhAvu8zbw0EQU2KFZyH6NAaJFo32unA75A0u_E"

# Set to True to enable calling Logic App on failure
ENABLE_ERROR_FILE_MOVE = True

# ==========================================================
# TRIGGER INGESTION ON SUCCESS
# ==========================================================
# When precheck passes, trigger the RAW_ADLS_TO_RAW_SNOWFLAKE notebook
TRIGGER_INGESTION_ON_SUCCESS = True
INGESTION_STORED_PROCEDURE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.SP_RAW_ADLS_TO_RAW_SNOWFLAKE"

# Validation thresholds
MIN_ROW_COUNT_DEFAULT = 1  # Minimum rows expected (excluding header)
MAX_HEADER_LENGTH = 128    # Maximum allowed header column name length

# HTTP timeout for Logic App calls (seconds)
LOGIC_APP_TIMEOUT_SECONDS = 90

# Problematic characters in headers that could cause issues
PROBLEMATIC_HEADER_CHARS = [
    '\x00',      # Null byte
    '\r',        # Carriage return
    '\n',        # Newline
    '\t',        # Tab (might be confused with delimiter)
    '"',         # Quote inside header
    "'",         # Single quote
    ';',         # SQL injection risk
    '--',        # SQL comment
    '/*',        # SQL block comment
    '*/',        # SQL block comment end
]

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
    practice_name = None
    file_type = None
    
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
        return practice_name, file_type
    
    pipe_arg = next((a for a in argv if "|" in a), None)
    if pipe_arg is not None:
        parts = pipe_arg.split("|")
        if len(parts) >= 1:
            practice_name = _strip_wrapping_quotes(parts[0].strip()) or None
        if len(parts) >= 2:
            file_type = _strip_wrapping_quotes(parts[1].strip()) or None
        return practice_name, file_type
    
    if len(argv) >= 1:
        practice_name = _strip_wrapping_quotes(argv[0].strip()) or None
    if len(argv) >= 2:
        file_type = _strip_wrapping_quotes(argv[1].strip()) or None
    
    return practice_name, file_type

PRACTICE_NAME, FILE_TYPE = parse_parameters()

# ==========================================================
# MANUAL TESTING DEFAULTS
# ==========================================================
DEFAULT_PRACTICE_FOR_TESTING = None  # Set to "fastpace" for testing
DEFAULT_FILE_TYPE_FOR_TESTING = None  # Set to "appointmentdata" for testing

if not PRACTICE_NAME and DEFAULT_PRACTICE_FOR_TESTING:
    PRACTICE_NAME = DEFAULT_PRACTICE_FOR_TESTING
    print(f"[INFO] Using default practice for testing: {PRACTICE_NAME}")

if not FILE_TYPE and DEFAULT_FILE_TYPE_FOR_TESTING:
    FILE_TYPE = DEFAULT_FILE_TYPE_FOR_TESTING
    print(f"[INFO] Using default file_type for testing: {FILE_TYPE}")

if not PRACTICE_NAME or not FILE_TYPE:
    raise ValueError("Missing required parameters: practice and file_type")

print(f"\n[INFO] ========== FILE PRECHECK STARTED ==========")
print(f"[INFO] Parent Run ID: {PARENT_RUN_ID}")
print(f"[INFO] Practice: {PRACTICE_NAME}")
print(f"[INFO] File Type: {FILE_TYPE}")

# ==========================================================
# 2) LOGGING FUNCTION
# ==========================================================
def log_precheck(practice_name: str, file_name: str, check_name: str, 
                 check_result: str, status: str, start_time: float, 
                 end_time: float = None, error_message: str = None,
                 expected_value: str = None, actual_value: str = None,
                 details: str = None):
    """
    Logs a precheck validation result to the PRECHECK_INGEST_LOG table.
    """
    try:
        log_id = str(uuid.uuid4())
        if end_time is None:
            end_time = time.time()
        duration_seconds = round(end_time - start_time, 3)
        start_ts = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_ts = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Escape single quotes
        practice_name_safe = (practice_name or '').replace("'", "''")
        file_name_safe = (file_name or '').replace("'", "''")[:500]
        check_name_safe = (check_name or '').replace("'", "''")
        check_result_safe = (check_result or '').replace("'", "''")
        error_message_safe = (error_message or '').replace("'", "''")[:2000] if error_message else None
        expected_value_safe = (expected_value or '').replace("'", "''")[:1000] if expected_value else None
        actual_value_safe = (actual_value or '').replace("'", "''")[:1000] if actual_value else None
        details_safe = (details or '').replace("'", "''")[:4000] if details else None
        
        error_value = f"'{error_message_safe}'" if error_message_safe else "NULL"
        expected_val = f"'{expected_value_safe}'" if expected_value_safe else "NULL"
        actual_val = f"'{actual_value_safe}'" if actual_value_safe else "NULL"
        details_val = f"'{details_safe}'" if details_safe else "NULL"
        
        insert_sql = f"""
            INSERT INTO {PRECHECK_LOG_TABLE}
            (LOG_ID, PARENT_RUN_ID, PRACTICE_NAME, FILE_TYPE, FILE_NAME, CHECK_NAME, 
             CHECK_RESULT, STATUS, ERROR_MESSAGE, EXPECTED_VALUE, ACTUAL_VALUE, DETAILS,
             START_TIME, END_TIME, DURATION_SECONDS)
            VALUES (
                '{log_id}', '{PARENT_RUN_ID}', '{practice_name_safe}', '{FILE_TYPE}',
                '{file_name_safe}', '{check_name_safe}', '{check_result_safe}', '{status}',
                {error_value}, {expected_val}, {actual_val}, {details_val},
                '{start_ts}', '{end_ts}', {duration_seconds}
            )
        """
        session.sql(insert_sql).collect()
        
        status_icon = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠"
        print(f"[{status_icon}] {check_name}: {status} - {check_result}")
        return log_id
    except Exception as e:
        print(f"[WARN] Failed to write to precheck log table: {e}")
        return None

# ==========================================================
# 2a) FUNCTION TO MOVE FAILED FILES TO ERROR LOCATION
# ==========================================================
def move_failed_files_to_error(practice_name: str, file_type: str, 
                                source_config: dict, archive_config: dict, 
                                error_config: dict, failed_files: list) -> dict:
    """
    Calls the Logic App to trigger ADF pipeline to move failed files to error location.
    """
    result = {
        "called": False,
        "success": False,
        "response": None,
        "error": None
    }
    
    if not ENABLE_ERROR_FILE_MOVE:
        print("[INFO] Error file move is disabled (ENABLE_ERROR_FILE_MOVE=False)")
        return result
    
    if not failed_files:
        print("[INFO] No failed files to move")
        return result
    
    if not ARCHIVE_LOGIC_APP_URL or ARCHIVE_LOGIC_APP_URL.startswith("https://<"):
        print("[WARN] Logic App URL not configured. Skipping file move.")
        return result
    
    try:
        # Build the payload for the Logic App
        payload = {
            "event_type": "precheck_failure",
            "parent_run_id": PARENT_RUN_ID,
            "practice": practice_name,
            "file_type": file_type,
            "status": "PRECHECK_FAILED",
            "completed_utc": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "total_rows": 0,
            "source": {
                "container": source_config.get("container", "raw"),
                "directory": source_config.get("directory", ""),
                "stage_name": source_config.get("stage_name", ""),
                "stage_suffix": ""
            },
            "archive": {
                "container": archive_config.get("container", "raw"),
                "directory": archive_config.get("directory", "")
            },
            "error": {
                "container": error_config.get("container", "raw"),
                "directory": error_config.get("directory", "")
            },
            "files": failed_files
        }
        
        print(f"\n[INFO] Calling Logic App to move {len(failed_files)} failed file(s) to error location...")
        print(f"[INFO] Error location: {error_config.get('container', 'raw')}/{error_config.get('directory', '')}")
        print(f"[INFO] Timeout: {LOGIC_APP_TIMEOUT_SECONDS} seconds")
        
        # Make the HTTP POST request to Logic App
        if HAS_REQUESTS:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                ARCHIVE_LOGIC_APP_URL,
                headers=headers,
                json=payload,
                timeout=LOGIC_APP_TIMEOUT_SECONDS
            )
            
            result["called"] = True
            result["response"] = {
                "status_code": response.status_code,
                "body": response.text[:500] if response.text else None
            }
            
            if response.status_code in [200, 202]:
                result["success"] = True
                print(f"[INFO] Logic App call successful (HTTP {response.status_code})")
            else:
                result["error"] = f"HTTP {response.status_code}: {response.text[:200]}"
                print(f"[ERROR] Logic App call failed: {result['error']}")
        else:
            # Use urllib (fallback)
            payload_bytes = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                ARCHIVE_LOGIC_APP_URL,
                data=payload_bytes,
                headers={"Content-Type": "application/json"},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=LOGIC_APP_TIMEOUT_SECONDS) as response:
                status_code = response.getcode()
                response_body = response.read().decode('utf-8')
                
                result["called"] = True
                result["response"] = {
                    "status_code": status_code,
                    "body": response_body[:500] if response_body else None
                }
                
                if status_code in [200, 202]:
                    result["success"] = True
                    print(f"[INFO] Logic App call successful (HTTP {status_code})")
                else:
                    result["error"] = f"HTTP {status_code}: {response_body[:200]}"
                    print(f"[ERROR] Logic App call failed: {result['error']}")
        
        # Log the Logic App call
        log_precheck(practice_name, "OVERALL", "MOVE_FAILED_FILES", 
                     f"Logic App called for {len(failed_files)} file(s)", 
                     "PASS" if result["success"] else "FAIL",
                     time.time(),
                     error_message=result.get("error"),
                     details=f"Files: {', '.join([f['file_name'] for f in failed_files[:10]])}")
        
    except Exception as e:
        error_str = str(e)
        if "timed out" in error_str.lower() or "timeout" in error_str.lower():
            result["error"] = f"Request timed out after {LOGIC_APP_TIMEOUT_SECONDS} seconds"
            print(f"[ERROR] Logic App call timed out")
        else:
            result["error"] = error_str[:500]
            print(f"[ERROR] Logic App call failed: {result['error']}")
        log_precheck(practice_name, "OVERALL", "MOVE_FAILED_FILES", 
                     "Logic App call failed", "FAIL", time.time(),
                     error_message=result["error"])
    
    return result

# ==========================================================
# 2b) FUNCTION TO TRIGGER INGESTION ON SUCCESS
# ==========================================================
def trigger_ingestion_notebook(practice_name: str, file_type: str) -> dict:
    """
    Triggers the RAW_ADLS_TO_RAW_SNOWFLAKE notebook via stored procedure
    when precheck passes successfully.
    
    Returns:
        Dict with triggered, success, error, duration
    """
    result = {
        "triggered": False,
        "success": False,
        "error": None,
        "duration": 0
    }
    
    if not TRIGGER_INGESTION_ON_SUCCESS:
        print("[INFO] Ingestion trigger is disabled (TRIGGER_INGESTION_ON_SUCCESS=False)")
        return result
    
    trigger_start = time.time()
    
    try:
        print(f"\n[INFO] ========== TRIGGERING INGESTION NOTEBOOK ==========")
        print(f"[INFO] Calling stored procedure: {INGESTION_STORED_PROCEDURE}")
        print(f"[INFO] Parameters: practice={practice_name}, file_type={file_type}, parent_run_id={PARENT_RUN_ID}")
        
        # Call the stored procedure to execute the ingestion notebook
        # Pass PARENT_RUN_ID as 3rd parameter for end-to-end traceability
        call_sql = f"CALL {INGESTION_STORED_PROCEDURE}('{practice_name}', '{file_type}', '{PARENT_RUN_ID}')"
        print(f"[DEBUG] SQL: {call_sql}")
        
        sp_result = session.sql(call_sql).collect()
        
        trigger_end = time.time()
        result["duration"] = round(trigger_end - trigger_start, 3)
        result["triggered"] = True
        
        # Check the result
        if sp_result:
            sp_return = str(sp_result[0][0]) if sp_result[0] else ""
            print(f"[INFO] Stored procedure returned: {sp_return}")
            
            if "SUCCESS" in sp_return.upper() or "COMPLETE" in sp_return.upper():
                result["success"] = True
                print(f"[INFO] Ingestion notebook completed successfully")
            else:
                result["success"] = True  # SP returned, assume success
                print(f"[INFO] Ingestion notebook triggered (returned: {sp_return})")
        else:
            result["success"] = True
            print(f"[INFO] Ingestion notebook triggered")
        
        # Log the trigger
        log_precheck(practice_name, "OVERALL", "TRIGGER_INGESTION",
                     f"RAW_ADLS_TO_RAW_SNOWFLAKE triggered successfully",
                     "PASS", trigger_start, trigger_end,
                     details=f"Duration: {result['duration']}s")
        
    except Exception as e:
        trigger_end = time.time()
        result["duration"] = round(trigger_end - trigger_start, 3)
        result["error"] = str(e)[:500]
        print(f"[ERROR] Failed to trigger ingestion notebook: {result['error']}")
        
        log_precheck(practice_name, "OVERALL", "TRIGGER_INGESTION",
                     "Failed to trigger RAW_ADLS_TO_RAW_SNOWFLAKE",
                     "FAIL", trigger_start, trigger_end,
                     error_message=result["error"])
    
    return result

# ==========================================================
# 3) Load config
# ==========================================================
session.sql("CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG TYPE = JSON").collect()

precheck_start_time = time.time()
validation_errors = []
validation_warnings = []
files_checked = []

# Load config for this practice/file_type
cfg_sql = f"""
    SELECT
      LOWER(p.value:practice_name::string)                   AS practice_name,
      f.value:file_type::string                              AS file_type,
      f.value:source_type::string                            AS source_type,
      f.value:source:container::string                       AS src_container,
      f.value:source:stage_name::string                      AS src_stage_name,
      f.value:source:directory::string                       AS src_directory,
      f.value:source:file_pattern::string                    AS src_file_pattern,
      f.value:source:delimiter::string                       AS src_delimiter,
      f.value:snowflake:database::string                     AS target_db,
      f.value:snowflake:schema::string                       AS target_schema,
      f.value:snowflake:table::string                        AS target_table,
      f.value:precheck:expected_columns                      AS expected_columns,
      f.value:precheck:min_row_count::number                 AS min_row_count,
      f.value:precheck:max_row_count::number                 AS max_row_count,
      f.value:precheck:require_all_columns::boolean          AS require_all_columns,
      f.value:precheck:allow_extra_columns::boolean          AS allow_extra_columns,
      f.value:precheck:case_sensitive_headers::boolean       AS case_sensitive_headers,
      f.value:archive:container::string                      AS archive_container,
      f.value:archive:directory::string                      AS archive_directory,
      f.value:error:container::string                        AS err_container,
      f.value:error:directory::string                        AS err_directory
    FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
         LATERAL FLATTEN(input => cfg.$1:Practices) p,
         LATERAL FLATTEN(input => p.value:ingest) f
    WHERE LOWER(p.value:practice_name::string) = LOWER('{PRACTICE_NAME}')
      AND LOWER(f.value:file_type::string) = LOWER('{FILE_TYPE}')
"""

cfg_rows = session.sql(cfg_sql).collect()

# ==========================================================
# 3a) Handle missing config gracefully
# ==========================================================
if not cfg_rows:
    error_msg = f"No config found for practice={PRACTICE_NAME}, file_type={FILE_TYPE}"
    log_precheck(PRACTICE_NAME, "N/A", "CONFIG_NOT_FOUND", error_msg, "FAIL", precheck_start_time, 
                 error_message=error_msg,
                 details="Practice/file_type combination not recognized. Files will be moved to error location.")
    
    print(f"\n[ERROR] {error_msg}")
    print("[INFO] Attempting to move unrecognized files to error location...")
    
    # Use convention-based fallback paths
    # Source: {file_type}/{practice} (e.g., appointmentdata/isenberg)
    # Error: error/{practice}/{file_type} (e.g., error/isenberg/appointmentdata)
    fallback_src_directory = f"{FILE_TYPE.lower()}/{PRACTICE_NAME.lower()}"
    fallback_err_directory = f"error/{PRACTICE_NAME.lower()}/{FILE_TYPE.lower()}"
    fallback_stage_name = "STAGE_STULHETLDEV001_RAW"
    fallback_container = "raw"
    
    print(f"[INFO] Fallback source directory: {fallback_container}/{fallback_src_directory}")
    print(f"[INFO] Fallback error directory: {fallback_container}/{fallback_err_directory}")
    
    # Build fallback config dicts for Logic App call
    fallback_source_config = {
        "container": fallback_container,
        "directory": fallback_src_directory,
        "stage_name": fallback_stage_name
    }
    
    fallback_archive_config = {
        "container": fallback_container,
        "directory": f"archive/{PRACTICE_NAME.lower()}/{FILE_TYPE.lower()}"
    }
    
    fallback_error_config = {
        "container": fallback_container,
        "directory": fallback_err_directory
    }
    
    # Try to list files in the assumed source directory
    fallback_stage_path = f"@UPPERLINE.PUBLIC.{fallback_stage_name}/{fallback_src_directory}"
    fallback_failed_files = []
    
    try:
        print(f"[INFO] Listing files in: {fallback_stage_path}")
        list_sql = f"LIST {fallback_stage_path}"
        file_list_rows = session.sql(list_sql).collect()
        
        if file_list_rows:
            print(f"[INFO] Found {len(file_list_rows)} file(s) to move to error location")
            for row in file_list_rows:
                file_path = row["name"]
                file_name = file_path.split("/")[-1]
                fallback_failed_files.append({
                    "file_name": file_name,
                    "status": "CONFIG_NOT_FOUND",
                    "rows": 0,
                    "reason": f"No config found for practice={PRACTICE_NAME}, file_type={FILE_TYPE}"
                })
        else:
            print(f"[WARN] No files found in {fallback_stage_path}")
            
    except Exception as e:
        print(f"[WARN] Could not list files in fallback directory: {str(e)[:200]}")
        # Even if we can't list files, still try to notify via Logic App
        fallback_failed_files.append({
            "file_name": "UNKNOWN",
            "status": "CONFIG_NOT_FOUND",
            "rows": 0,
            "reason": f"No config found for practice={PRACTICE_NAME}, file_type={FILE_TYPE}. Could not list files: {str(e)[:100]}"
        })
    
    # Call Logic App to move files to error location and send Teams notification
    if fallback_failed_files:
        move_result = move_failed_files_to_error(
            practice_name=PRACTICE_NAME,
            file_type=FILE_TYPE,
            source_config=fallback_source_config,
            archive_config=fallback_archive_config,
            error_config=fallback_error_config,
            failed_files=fallback_failed_files
        )
        print(f"\n[INFO] File move result: {move_result}")
    
    # Log pipeline halted
    log_precheck(PRACTICE_NAME, "OVERALL", "PIPELINE_HALTED",
                 f"Precheck failed - config not found, ingestion NOT triggered",
                 "FAIL", precheck_start_time, time.time(),
                 error_message=f"{len(fallback_failed_files)} file(s) moved to error location. Manual review required.",
                 details=f"Practice/file_type not in config. Files moved to: {fallback_container}/{fallback_err_directory}")
    
    print("\n" + "=" * 60)
    print("[RESULT] PRECHECK FAILED - CONFIG NOT FOUND")
    print("[RESULT] Files moved to error location. Ingestion NOT triggered.")
    print("=" * 60)
    
    PRECHECK_RESULT = "FAIL"
    # Note: Final "[DONE]" print is at end of file, covers both branches

else:
    # Config found - proceed with normal flow
    cfg = cfg_rows[0]
    log_precheck(PRACTICE_NAME, "N/A", "CONFIG_LOAD", "Configuration loaded successfully", "PASS", precheck_start_time)
    
    # Extract config values
    source_type = (cfg["SOURCE_TYPE"] or "file").lower()
    src_container = cfg["SRC_CONTAINER"]
    src_stage_name = cfg["SRC_STAGE_NAME"]
    src_directory = (cfg["SRC_DIRECTORY"] or "").rstrip("/")
    src_file_pattern = cfg["SRC_FILE_PATTERN"] or ".*\\.csv$"
    src_delimiter = cfg["SRC_DELIMITER"] or ","
    target_db = cfg["TARGET_DB"]
    target_schema = cfg["TARGET_SCHEMA"]
    target_table = cfg["TARGET_TABLE"]
    expected_columns_raw = cfg["EXPECTED_COLUMNS"]
    min_row_count = cfg["MIN_ROW_COUNT"] if cfg["MIN_ROW_COUNT"] is not None else MIN_ROW_COUNT_DEFAULT
    max_row_count = cfg["MAX_ROW_COUNT"]  # Can be None (no max)
    require_all_columns = cfg["REQUIRE_ALL_COLUMNS"] if cfg["REQUIRE_ALL_COLUMNS"] is not None else True
    allow_extra_columns = cfg["ALLOW_EXTRA_COLUMNS"] if cfg["ALLOW_EXTRA_COLUMNS"] is not None else True
    case_sensitive_headers = cfg["CASE_SENSITIVE_HEADERS"] if cfg["CASE_SENSITIVE_HEADERS"] is not None else False
    archive_container = cfg["ARCHIVE_CONTAINER"]
    archive_directory = (cfg["ARCHIVE_DIRECTORY"] or "").rstrip("/")
    err_container = cfg["ERR_CONTAINER"]
    err_directory = (cfg["ERR_DIRECTORY"] or "").rstrip("/")
    
    # Build config dicts for Logic App calls
    source_config = {
        "container": src_container or "raw",
        "directory": src_directory,
        "stage_name": src_stage_name or ""
    }
    
    archive_config = {
        "container": archive_container or "raw",
        "directory": archive_directory
    }
    
    error_config = {
        "container": err_container or "raw",
        "directory": err_directory
    }
    
    # Track failed files for Logic App call
    failed_files_for_move = []
    
    print(f"\n[INFO] Source Configuration:")
    print(f"  Source Type     : {source_type}")
    print(f"  Stage Name      : {src_stage_name}")
    print(f"  Directory       : {src_directory}")
    print(f"  File Pattern    : {src_file_pattern}")
    print(f"  Delimiter       : '{src_delimiter}'")
    print(f"  Target Table    : {target_db}.{target_schema}.{target_table}")
    print(f"  Min Row Count   : {min_row_count}")
    print(f"  Max Row Count   : {max_row_count or 'N/A'}")
    print(f"  Error Location  : {err_container}/{err_directory}")
    
    # ==========================================================
    # 4) Parse expected columns from config
    # ==========================================================
    def ensure_list(raw_value):
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
            return raw_value
        if isinstance(raw_value, str):
            try:
                parsed = json.loads(raw_value)
                if isinstance(parsed, list):
                    return parsed
            except:
                pass
            return [raw_value]
        try:
            parsed = json.loads(str(raw_value))
            if isinstance(parsed, list):
                return parsed
        except:
            pass
        return []
    
    expected_columns = ensure_list(expected_columns_raw)
    
    if not expected_columns:
        # Try to get columns from target table schema
        print("[INFO] No expected_columns in config, fetching from target table schema...")
        try:
            schema_sql = f"""
                SELECT COLUMN_NAME 
                FROM {target_db}.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{target_schema}' 
                  AND TABLE_NAME = '{target_table}'
                  AND COLUMN_NAME NOT IN ('FILE_NAME', 'FILE_LOAD_TIME', 'PARENT_RUN_ID')
                ORDER BY ORDINAL_POSITION
            """
            schema_rows = session.sql(schema_sql).collect()
            expected_columns = [row["COLUMN_NAME"] for row in schema_rows]
            print(f"[INFO] Loaded {len(expected_columns)} columns from target table schema")
        except Exception as e:
            print(f"[WARN] Could not load columns from target table: {e}")
    
    print(f"[INFO] Expected columns ({len(expected_columns)}): {expected_columns[:10]}{'...' if len(expected_columns) > 10 else ''}")
    
    # ==========================================================
    # 5) Only process file-based sources
    # ==========================================================
    if source_type != "file":
        msg = f"Source type '{source_type}' is not file-based. Precheck only applies to file sources."
        log_precheck(PRACTICE_NAME, "N/A", "SOURCE_TYPE_CHECK", msg, "SKIP", precheck_start_time)
        print(f"\n[INFO] {msg}")
        print("[DONE] Precheck skipped for non-file source.")
        PRECHECK_RESULT = "SKIP"
    else:
        # ==========================================================
        # 6) List files in stage directory
        # ==========================================================
        list_start_time = time.time()
        
        stage_path = f"@UPPERLINE.PUBLIC.{src_stage_name.upper()}/{src_directory}"
        print(f"\n[INFO] Checking files in: {stage_path}")
        
        list_sql = f"LIST {stage_path}"
        try:
            file_list_rows = session.sql(list_sql).collect()
        except Exception as e:
            error_msg = f"Failed to list files in stage: {str(e)[:500]}"
            log_precheck(PRACTICE_NAME, stage_path, "FILE_LIST", error_msg, "FAIL", list_start_time,
                         error_message=error_msg)
            validation_errors.append(("FILE_LIST", "FILE_LIST", error_msg))
            file_list_rows = []
        
        if not file_list_rows:
            error_msg = f"No files found in {stage_path}"
            log_precheck(PRACTICE_NAME, stage_path, "FILE_LIST", error_msg, "WARN", list_start_time,
                         error_message=error_msg)
            validation_warnings.append(("FILE_LIST", "FILE_LIST", error_msg))
        else:
            log_precheck(PRACTICE_NAME, stage_path, "FILE_LIST", 
                         f"Found {len(file_list_rows)} file(s)", "PASS", list_start_time,
                         actual_value=str(len(file_list_rows)))
        
        # Filter files by pattern
        pattern_regex = re.compile(src_file_pattern, re.IGNORECASE)
        matching_files = []
        
        for row in file_list_rows:
            file_path = row["name"]
            file_name = file_path.split("/")[-1]
            if pattern_regex.search(file_name):
                # LIST command returns: name, size, md5, last_modified
                matching_files.append({
                    "path": file_path,
                    "name": file_name,
                    "size": row["size"]
                })
        
        print(f"[INFO] Files matching pattern '{src_file_pattern}': {len(matching_files)}")
        
        if not matching_files:
            error_msg = f"No files matching pattern '{src_file_pattern}' in {stage_path}"
            log_precheck(PRACTICE_NAME, stage_path, "FILE_PATTERN_MATCH", error_msg, "WARN", list_start_time,
                         error_message=error_msg, expected_value=src_file_pattern)
            validation_warnings.append(("FILE_PATTERN_MATCH", "FILE_PATTERN_MATCH", error_msg))
        
        # ==========================================================
        # 7) Validate each matching file
        # ==========================================================
        for file_info in matching_files:
            file_path = file_info["path"]
            file_name = file_info["name"]
            file_size = file_info["size"]
            
            print(f"\n{'='*60}")
            print(f"[INFO] Validating file: {file_name}")
            print(f"[INFO] Size: {file_size} bytes")
            print(f"{'='*60}")
            
            files_checked.append(file_name)
            file_start_time = time.time()
            file_errors = []
            file_warnings = []
            
            # ---------------------------------------------------------
            # 7a) Check file size
            # ---------------------------------------------------------
            if file_size == 0:
                error_msg = f"File is empty (0 bytes)"
                log_precheck(PRACTICE_NAME, file_name, "FILE_SIZE", error_msg, "FAIL", file_start_time,
                             error_message=error_msg, actual_value="0 bytes")
                file_errors.append(("FILE_SIZE", error_msg))
                continue  # Skip further checks for empty file
            else:
                log_precheck(PRACTICE_NAME, file_name, "FILE_SIZE", 
                             f"File size OK: {file_size} bytes", "PASS", file_start_time,
                             actual_value=f"{file_size} bytes")
            
            # ---------------------------------------------------------
            # 7b) Read file header and sample rows
            # ---------------------------------------------------------
            read_start_time = time.time()
            
            try:
                # Create temp file format to read raw lines (no field parsing)
                session.sql(f"""
                    CREATE OR REPLACE TEMPORARY FILE FORMAT FF_PRECHECK_RAW
                    TYPE = CSV
                    FIELD_DELIMITER = NONE
                    RECORD_DELIMITER = '\\n'
                    SKIP_HEADER = 0
                """).collect()
                
                # Read first row to get header
                header_sql = f"""
                    SELECT $1 AS RAW_LINE
                    FROM {stage_path}/{file_name} (FILE_FORMAT => 'FF_PRECHECK_RAW')
                    LIMIT 1
                """
                header_rows = session.sql(header_sql).collect()
                
                if not header_rows:
                    error_msg = "Could not read header row from file"
                    log_precheck(PRACTICE_NAME, file_name, "FILE_READ", error_msg, "FAIL", read_start_time,
                                 error_message=error_msg)
                    file_errors.append(("FILE_READ", error_msg))
                    continue
                
                raw_header = header_rows[0]["RAW_LINE"]
                
                # Read sample data rows (skip header)
                sample_sql = f"""
                    SELECT $1 AS RAW_LINE
                    FROM {stage_path}/{file_name} (FILE_FORMAT => 'FF_PRECHECK_RAW')
                    LIMIT 11
                """
                sample_rows = session.sql(sample_sql).collect()
                
                log_precheck(PRACTICE_NAME, file_name, "FILE_READ", 
                             f"Successfully read header and {len(sample_rows)-1} sample rows", "PASS", read_start_time,
                             actual_value=str(len(sample_rows)))
                             
            except Exception as e:
                error_msg = f"Failed to read file: {str(e)[:500]}"
                log_precheck(PRACTICE_NAME, file_name, "FILE_READ", error_msg, "FAIL", read_start_time,
                             error_message=error_msg)
                file_errors.append(("FILE_READ", error_msg))
                continue
            
            # ---------------------------------------------------------
            # 7c) Parse header row
            # ---------------------------------------------------------
            header_start_time = time.time()
            
            # Strip any trailing carriage return (Windows line endings)
            raw_header = raw_header.rstrip('\r\n') if raw_header else ""
            
            # Debug output
            print(f"[DEBUG] Raw header length: {len(raw_header)}")
            print(f"[DEBUG] Raw header first 100 chars: {repr(raw_header[:100])}")
            
            # Check for BOM
            bom_detected = None
            header_to_parse = raw_header
            
            # Check for various BOM representations
            bom_variants = [
                ('\ufeff', 'UTF-8 BOM (unicode)'),
                ('ï»¿', 'UTF-8 BOM (latin-1)'),
                ('\xef\xbb\xbf', 'UTF-8 BOM (bytes)'),
                ('\xff\xfe', 'UTF-16-LE BOM'),
                ('\xfe\xff', 'UTF-16-BE BOM'),
            ]
            
            for bom_str, bom_name in bom_variants:
                if raw_header.startswith(bom_str):
                    bom_detected = bom_name
                    header_to_parse = raw_header[len(bom_str):]
                    print(f"[DEBUG] Stripped BOM '{bom_name}' ({len(bom_str)} chars) from header")
                    break
            
            if bom_detected:
                warning_msg = f"BOM detected: {bom_detected}. First column name may include BOM characters."
                log_precheck(PRACTICE_NAME, file_name, "BOM_CHECK", warning_msg, "WARN", header_start_time,
                             error_message=warning_msg, actual_value=bom_detected,
                             details="BOM (Byte Order Mark) can cause issues with header parsing. Consider removing BOM from source files.")
                file_warnings.append(("BOM_CHECK", warning_msg))
            else:
                log_precheck(PRACTICE_NAME, file_name, "BOM_CHECK", "No BOM detected", "PASS", header_start_time)
            
            # Parse header columns using Python csv module for proper quote handling
            def parse_csv_line(line, delimiter):
                """Parse a CSV line handling quoted fields using Python's csv module."""
                try:
                    reader = csv.reader(StringIO(line), delimiter=delimiter, quotechar='"')
                    fields = next(reader, [])
                    return [f.strip() for f in fields]
                except Exception:
                    # Fallback to simple split
                    return [f.strip().strip('"') for f in line.split(delimiter)]
            
            actual_headers = parse_csv_line(header_to_parse, src_delimiter)
            
            # Clean headers (remove any remaining BOM artifacts)
            actual_headers = [h.lstrip('\ufeff').lstrip('ï»¿').strip() for h in actual_headers]
            
            print(f"[DEBUG] Header to parse (after BOM strip): {repr(header_to_parse[:100])}")
            print(f"[INFO] Parsed {len(actual_headers)} header columns")
            print(f"[INFO] First 5 headers: {actual_headers[:5]}")
            
            # ---------------------------------------------------------
            # 7d) Check for problematic characters in headers
            # ---------------------------------------------------------
            char_check_start = time.time()
            problematic_headers = []
            
            for i, header in enumerate(actual_headers):
                issues = []
                
                # Check for problematic characters
                for prob_char in PROBLEMATIC_HEADER_CHARS:
                    if prob_char in header:
                        issues.append(f"contains '{repr(prob_char)}'")
                
                # Check for leading/trailing whitespace
                if header != header.strip():
                    issues.append("has leading/trailing whitespace")
                
                # Check for empty header
                if not header or header.isspace():
                    issues.append("is empty or whitespace only")
                
                # Check header length
                if len(header) > MAX_HEADER_LENGTH:
                    issues.append(f"exceeds max length ({len(header)} > {MAX_HEADER_LENGTH})")
                
                # Check for non-printable characters
                non_printable = [c for c in header if ord(c) < 32 or ord(c) > 126]
                if non_printable:
                    issues.append(f"contains non-printable chars: {[hex(ord(c)) for c in non_printable]}")
                
                if issues:
                    problematic_headers.append((i, header, issues))
            
            if problematic_headers:
                error_details = []
                for idx, header, issues in problematic_headers:
                    error_details.append(f"Column {idx} '{header[:50]}': {', '.join(issues)}")
                
                error_msg = f"Found {len(problematic_headers)} header(s) with problematic characters"
                full_details = "\n".join(error_details)
                log_precheck(PRACTICE_NAME, file_name, "HEADER_CHARACTERS", error_msg, "FAIL", char_check_start,
                             error_message=error_msg, actual_value=str(len(problematic_headers)),
                             details=full_details[:4000])
                file_errors.append(("HEADER_CHARACTERS", f"{error_msg}: {full_details[:500]}"))
            else:
                log_precheck(PRACTICE_NAME, file_name, "HEADER_CHARACTERS", 
                             "All headers have valid characters", "PASS", char_check_start)
            
            # ---------------------------------------------------------
            # 7e) Validate header schema against expected columns
            # ---------------------------------------------------------
            schema_check_start = time.time()
            
            if expected_columns:
                # Normalize for comparison
                if case_sensitive_headers:
                    expected_set = set(expected_columns)
                    actual_set = set(actual_headers)
                    expected_list = expected_columns
                    actual_list = actual_headers
                else:
                    expected_set = set(c.upper() for c in expected_columns)
                    actual_set = set(c.upper() for c in actual_headers)
                    expected_list = [c.upper() for c in expected_columns]
                    actual_list = [c.upper() for c in actual_headers]
                
                missing_columns = expected_set - actual_set
                extra_columns = actual_set - expected_set
                
                # Check column count
                if len(actual_headers) != len(expected_columns):
                    if require_all_columns and missing_columns:
                        error_msg = f"Column count mismatch: expected {len(expected_columns)}, got {len(actual_headers)}"
                        log_precheck(PRACTICE_NAME, file_name, "COLUMN_COUNT", error_msg, "FAIL", schema_check_start,
                                     error_message=error_msg,
                                     expected_value=str(len(expected_columns)),
                                     actual_value=str(len(actual_headers)))
                        file_errors.append(("COLUMN_COUNT", error_msg))
                    else:
                        warning_msg = f"Column count differs: expected {len(expected_columns)}, got {len(actual_headers)}"
                        log_precheck(PRACTICE_NAME, file_name, "COLUMN_COUNT", warning_msg, "WARN", schema_check_start,
                                     error_message=warning_msg,
                                     expected_value=str(len(expected_columns)),
                                     actual_value=str(len(actual_headers)))
                        file_warnings.append(("COLUMN_COUNT", warning_msg))
                else:
                    log_precheck(PRACTICE_NAME, file_name, "COLUMN_COUNT", 
                                 f"Column count matches: {len(actual_headers)}", "PASS", schema_check_start,
                                 expected_value=str(len(expected_columns)),
                                 actual_value=str(len(actual_headers)))
                
                # Check for missing required columns
                if missing_columns:
                    missing_list = sorted(list(missing_columns))
                    if require_all_columns:
                        error_msg = f"Missing {len(missing_columns)} required column(s)"
                        log_precheck(PRACTICE_NAME, file_name, "MISSING_COLUMNS", error_msg, "FAIL", schema_check_start,
                                     error_message=error_msg,
                                     expected_value=", ".join(missing_list[:20]),
                                     details=f"Missing columns: {', '.join(missing_list)}")
                        file_errors.append(("MISSING_COLUMNS", f"{error_msg}: {', '.join(missing_list[:10])}"))
                    else:
                        warning_msg = f"Missing {len(missing_columns)} expected column(s)"
                        log_precheck(PRACTICE_NAME, file_name, "MISSING_COLUMNS", warning_msg, "WARN", schema_check_start,
                                     error_message=warning_msg,
                                     expected_value=", ".join(missing_list[:20]),
                                     details=f"Missing columns: {', '.join(missing_list)}")
                        file_warnings.append(("MISSING_COLUMNS", warning_msg))
                else:
                    log_precheck(PRACTICE_NAME, file_name, "MISSING_COLUMNS", 
                                 "All expected columns present", "PASS", schema_check_start)
                
                # Check for unexpected extra columns
                if extra_columns:
                    extra_list = sorted(list(extra_columns))
                    if not allow_extra_columns:
                        error_msg = f"Found {len(extra_columns)} unexpected column(s)"
                        log_precheck(PRACTICE_NAME, file_name, "EXTRA_COLUMNS", error_msg, "FAIL", schema_check_start,
                                     error_message=error_msg,
                                     actual_value=", ".join(extra_list[:20]),
                                     details=f"Extra columns: {', '.join(extra_list)}")
                        file_errors.append(("EXTRA_COLUMNS", f"{error_msg}: {', '.join(extra_list[:10])}"))
                    else:
                        info_msg = f"Found {len(extra_columns)} extra column(s) (allowed)"
                        log_precheck(PRACTICE_NAME, file_name, "EXTRA_COLUMNS", info_msg, "PASS", schema_check_start,
                                     actual_value=", ".join(extra_list[:20]),
                                     details=f"Extra columns: {', '.join(extra_list)}")
                
                # Check column order (if all columns present)
                if not missing_columns and len(actual_headers) == len(expected_columns):
                    order_matches = (actual_list == expected_list)
                    if not order_matches:
                        # Find mismatched positions
                        mismatches = []
                        for i, (exp, act) in enumerate(zip(expected_list, actual_list)):
                            if exp != act:
                                mismatches.append(f"Position {i}: expected '{expected_columns[i]}', got '{actual_headers[i]}'")
                        
                        warning_msg = f"Column order differs at {len(mismatches)} position(s)"
                        log_precheck(PRACTICE_NAME, file_name, "COLUMN_ORDER", warning_msg, "WARN", schema_check_start,
                                     error_message=warning_msg,
                                     details="\n".join(mismatches[:20]))
                        file_warnings.append(("COLUMN_ORDER", warning_msg))
                    else:
                        log_precheck(PRACTICE_NAME, file_name, "COLUMN_ORDER", 
                                     "Column order matches expected", "PASS", schema_check_start)
            else:
                log_precheck(PRACTICE_NAME, file_name, "SCHEMA_VALIDATION", 
                             "Skipped - no expected columns defined", "SKIP", schema_check_start)
            
            # ---------------------------------------------------------
            # 7f) Count rows and validate row count
            # ---------------------------------------------------------
            row_count_start = time.time()
            
            try:
                count_sql = f"""
                    SELECT COUNT(*) AS ROW_COUNT
                    FROM {stage_path}/{file_name} (FILE_FORMAT => 'FF_PRECHECK_RAW')
                """
                count_result = session.sql(count_sql).collect()
                total_rows = count_result[0]["ROW_COUNT"] if count_result else 0
                data_rows = total_rows - 1  # Subtract header row
                
                print(f"[INFO] Total rows in file: {total_rows} (header + {data_rows} data rows)")
                
                # Check minimum row count
                if data_rows < min_row_count:
                    error_msg = f"Row count {data_rows} is below minimum {min_row_count}"
                    log_precheck(PRACTICE_NAME, file_name, "MIN_ROW_COUNT", error_msg, "FAIL", row_count_start,
                                 error_message=error_msg,
                                 expected_value=f">= {min_row_count}",
                                 actual_value=str(data_rows))
                    file_errors.append(("MIN_ROW_COUNT", error_msg))
                else:
                    log_precheck(PRACTICE_NAME, file_name, "MIN_ROW_COUNT", 
                                 f"Row count OK: {data_rows} >= {min_row_count}", "PASS", row_count_start,
                                 expected_value=f">= {min_row_count}",
                                 actual_value=str(data_rows))
                
                # Check maximum row count (if defined)
                if max_row_count is not None and data_rows > max_row_count:
                    error_msg = f"Row count {data_rows} exceeds maximum {max_row_count}"
                    log_precheck(PRACTICE_NAME, file_name, "MAX_ROW_COUNT", error_msg, "FAIL", row_count_start,
                                 error_message=error_msg,
                                 expected_value=f"<= {max_row_count}",
                                 actual_value=str(data_rows))
                    file_errors.append(("MAX_ROW_COUNT", error_msg))
                elif max_row_count is not None:
                    log_precheck(PRACTICE_NAME, file_name, "MAX_ROW_COUNT", 
                                 f"Row count OK: {data_rows} <= {max_row_count}", "PASS", row_count_start,
                                 expected_value=f"<= {max_row_count}",
                                 actual_value=str(data_rows))
                                 
            except Exception as e:
                error_msg = f"Failed to count rows: {str(e)[:500]}"
                log_precheck(PRACTICE_NAME, file_name, "ROW_COUNT", error_msg, "FAIL", row_count_start,
                             error_message=error_msg)
                file_errors.append(("ROW_COUNT", error_msg))
            
            # ---------------------------------------------------------
            # 7g) Validate delimiter consistency
            # ---------------------------------------------------------
            delimiter_check_start = time.time()
            
            if len(sample_rows) > 1:
                header_field_count = len(actual_headers)
                inconsistent_rows = []
                
                # sample_rows[0] is header, so start from [1] for data rows
                for i, row in enumerate(sample_rows[1:6], start=2):  # Check first 5 data rows
                    row_line = row["RAW_LINE"].rstrip('\r\n') if row["RAW_LINE"] else ""
                    row_fields = parse_csv_line(row_line, src_delimiter)
                    if len(row_fields) != header_field_count:
                        inconsistent_rows.append((i, len(row_fields), header_field_count))
                
                if inconsistent_rows:
                    error_details = [f"Row {r[0]}: {r[1]} fields (expected {r[2]})" for r in inconsistent_rows]
                    error_msg = f"Found {len(inconsistent_rows)} row(s) with inconsistent field count"
                    log_precheck(PRACTICE_NAME, file_name, "DELIMITER_CONSISTENCY", error_msg, "FAIL", delimiter_check_start,
                                 error_message=error_msg,
                                 expected_value=f"Delimiter: '{src_delimiter}', Fields: {header_field_count}",
                                 actual_value=f"{len(inconsistent_rows)} inconsistent rows",
                                 details="\n".join(error_details))
                    file_errors.append(("DELIMITER_CONSISTENCY", f"{error_msg}: {error_details[0]}"))
                else:
                    log_precheck(PRACTICE_NAME, file_name, "DELIMITER_CONSISTENCY", 
                                 f"All sampled rows have consistent field count ({header_field_count})", "PASS", 
                                 delimiter_check_start,
                                 expected_value=f"Delimiter: '{src_delimiter}', Fields: {header_field_count}")
            
            # ---------------------------------------------------------
            # 7h) File validation summary
            # ---------------------------------------------------------
            file_end_time = time.time()
            
            if file_errors:
                validation_errors.extend([(file_name, e[0], e[1]) for e in file_errors])
                summary_msg = f"FAILED - {len(file_errors)} error(s), {len(file_warnings)} warning(s)"
                log_precheck(PRACTICE_NAME, file_name, "FILE_VALIDATION_SUMMARY", summary_msg, "FAIL", 
                             file_start_time, file_end_time,
                             error_message=f"Errors: {', '.join([e[0] for e in file_errors])}",
                             details=f"Errors:\n" + "\n".join([f"- {e[0]}: {e[1]}" for e in file_errors]))
                
                # Add to failed files list for Logic App call
                failed_files_for_move.append({
                    "file_name": file_name,
                    "status": "PRECHECK_FAILED",
                    "rows": 0,
                    "reason": "; ".join([f"{e[0]}: {e[1][:100]}" for e in file_errors[:3]])
                })
            else:
                if file_warnings:
                    validation_warnings.extend([(file_name, w[0], w[1]) for w in file_warnings])
                summary_msg = f"PASSED - 0 errors, {len(file_warnings)} warning(s)"
                log_precheck(PRACTICE_NAME, file_name, "FILE_VALIDATION_SUMMARY", summary_msg, "PASS", 
                             file_start_time, file_end_time,
                             details=f"Warnings:\n" + "\n".join([f"- {w[0]}: {w[1]}" for w in file_warnings]) if file_warnings else "No issues")
    
        # ==========================================================
        # 8) Overall Precheck Summary
        # ==========================================================
        precheck_end_time = time.time()
        precheck_duration = round(precheck_end_time - precheck_start_time, 3)
        
        print("\n" + "=" * 60)
        print("[PRECHECK SUMMARY]")
        print("=" * 60)
        print(f"  Parent Run ID    : {PARENT_RUN_ID}")
        print(f"  Practice         : {PRACTICE_NAME}")
        print(f"  File Type        : {FILE_TYPE}")
        print(f"  Files Checked    : {len(files_checked)}")
        print(f"  Total Errors     : {len(validation_errors)}")
        print(f"  Total Warnings   : {len(validation_warnings)}")
        print(f"  Duration         : {precheck_duration} seconds")
        print("-" * 60)
        
        if validation_errors:
            print("\n[ERRORS]")
            for file_name, check_name, error_msg in validation_errors[:20]:
                print(f"  ✗ [{file_name}] {check_name}: {error_msg[:100]}")
            if len(validation_errors) > 20:
                print(f"  ... and {len(validation_errors) - 20} more errors")
            
            # Log overall failure
            log_precheck(PRACTICE_NAME, "OVERALL", "PRECHECK_COMPLETE",
                         f"FAILED - {len(validation_errors)} error(s) in {len(files_checked)} file(s)", "FAIL",
                         precheck_start_time, precheck_end_time,
                         error_message=f"Files with errors: {', '.join(set(e[0] for e in validation_errors))}",
                         details=f"Error summary:\n" + "\n".join([f"- {e[1]}: {e[2][:100]}" for e in validation_errors[:20]]))
            
            # ==========================================================
            # 9a) Move failed files to error location via Logic App
            # ==========================================================
            if failed_files_for_move:
                move_result = move_failed_files_to_error(
                    practice_name=PRACTICE_NAME,
                    file_type=FILE_TYPE,
                    source_config=source_config,
                    archive_config=archive_config,
                    error_config=error_config,
                    failed_files=failed_files_for_move
                )
                print(f"\n[INFO] File move result: {move_result}")
            
            # ==========================================================
            # 9b) Log final pipeline halted status
            # ==========================================================
            files_moved_count = len(failed_files_for_move)
            log_precheck(PRACTICE_NAME, "OVERALL", "PIPELINE_HALTED",
                         f"Precheck failed - ingestion NOT triggered",
                         "FAIL", precheck_start_time, time.time(),
                         error_message=f"{files_moved_count} file(s) moved to error location. Manual review required.",
                         details=f"Failed files: {', '.join([f['file_name'] for f in failed_files_for_move[:10]])}")
            
            print("\n" + "=" * 60)
            print("[RESULT] PRECHECK FAILED - DO NOT PROCEED WITH INGESTION")
            print("=" * 60)
            
            # Return FAIL status
            PRECHECK_RESULT = "FAIL"
            
        else:
            if validation_warnings:
                print("\n[WARNINGS]")
                for file_name, check_name, warning_msg in validation_warnings[:10]:
                    print(f"  ⚠ [{file_name}] {check_name}: {warning_msg[:100]}")
            
            # Log overall success
            log_precheck(PRACTICE_NAME, "OVERALL", "PRECHECK_COMPLETE",
                         f"PASSED - {len(files_checked)} file(s) validated with {len(validation_warnings)} warning(s)", "PASS",
                         precheck_start_time, precheck_end_time,
                         details=f"Files validated: {', '.join(files_checked)}")
            
            print("\n" + "=" * 60)
            print("[RESULT] PRECHECK PASSED - OK TO PROCEED WITH INGESTION")
            print("=" * 60)
            
            # ==========================================================
            # 9b) Trigger ingestion notebook on success
            # ==========================================================
            ingestion_result = trigger_ingestion_notebook(PRACTICE_NAME, FILE_TYPE)
            
            if ingestion_result["triggered"]:
                print(f"\n[INFO] Ingestion trigger result:")
                print(f"  Triggered: {ingestion_result['triggered']}")
                print(f"  Success  : {ingestion_result['success']}")
                print(f"  Duration : {ingestion_result['duration']}s")
                if ingestion_result["error"]:
                    print(f"  Error    : {ingestion_result['error']}")
            
            # Return PASS status
            PRECHECK_RESULT = "PASS"

print(f"\n[DONE] Precheck complete. Result: {PRECHECK_RESULT}")
