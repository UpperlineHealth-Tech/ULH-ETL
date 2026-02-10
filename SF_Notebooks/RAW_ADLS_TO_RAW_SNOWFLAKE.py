# RAW_ADLS_TO_RAW_SNOWFLAKE.py
# ==========================================================
# Notebook Name: RAW_ADLS_TO_RAW_SNOWFLAKE
# Location: UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_ADLS_TO_RAW_SNOWFLAKE
# ==========================================================
#
# Multi-practice, multi-file_type ingestion using grouped JSON config in ADLS.
# SUPPORTS BOTH FILE-BASED AND QUERY-BASED SOURCES
#
# ==========================================================
# PARAMETER REQUIREMENT:
# ==========================================================
#
# REQUIRED:
#   This notebook requires BOTH practice and file_type to be provided.
#   If parameters are not provided, the notebook ends gracefully without processing.
#
# ==========================================================
# FILE_TYPE MATCHING:
# ==========================================================
# Logic App folder/file_type values are typically lower-case, such as "member_eligibility",
# while the config file_type may be cased, such as "Member_Eligibility".
#
# This notebook matches the incoming FILE_TYPE parameter against EITHER:
#   1) config file_type (case-insensitive), OR
#   2) the leaf folder name of source.directory (case-insensitive)
#

from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.exceptions import SnowparkSQLException

from datetime import datetime
import json
import re
import uuid
import time
import sys
import os
import urllib.request
import urllib.error

# ==========================================================
# 0) Session initialization
# ==========================================================
session = get_active_session()

# PARENT_RUN_ID will be set after parameter parsing
# If passed from upstream (PRECHECK), use that for end-to-end traceability
# Otherwise generate a new one
PARENT_RUN_ID = None  # Will be set after parsing parameters

# ==========================================================
# 1) PARAMETER HANDLING - Accept from EXECUTE NOTEBOOK
# ==========================================================
# When called via stored procedure -> EXECUTE NOTEBOOK:
#   The stored procedure passes PRACTICE, FILE_TYPE, and optionally PARENT_RUN_ID as arguments.
#
#   In a Snowflake Notebook, sys.argv contains only those argument strings:
#     sys.argv[0] = practice
#     sys.argv[1] = file_type
#     sys.argv[2] = parent_run_id (optional, for end-to-end traceability)
#
# Backward compatibility:
#   If a single argument contains 'practice|file_type|parent_run_id', it will be parsed as pipe-delimited.

def _strip_wrapping_quotes(s: str) -> str:
    """Remove one layer of matching single/double quotes around a string, if present."""
    if not s:
        return s
    s2 = s.strip()
    if len(s2) >= 2 and s2[0] == s2[-1] and s2[0] in ("'", '"'):
        return s2[1:-1]
    return s2

def parse_parameters():
    """
    Parse arguments passed via EXECUTE NOTEBOOK / scheduled runs.

    Supports:
      1) Three positional args: practice, file_type, parent_run_id
      2) Two positional args: practice, file_type
      3) Single pipe-delimited arg: "practice|file_type|parent_run_id"
      4) Single practice only
      5) Single named-arg string: "practice=... file_type=..."

    Returns:
      (practices_list, file_types_list, parent_run_id)
    """
    practices = []
    file_types = []
    parent_run_id = None

    print(f"[DEBUG] sys.argv = {sys.argv}")
    print(f"[DEBUG] Number of arguments: {len(sys.argv)}")

    # Normalize / sanitize arguments
    argv = []
    for a in (sys.argv or []):
        if a is None:
            continue
        s = _strip_wrapping_quotes(str(a).strip())
        if not s:
            continue
        # Ignore common flag-style args if running outside Snowflake.
        if s.startswith("-"):
            continue
        argv.append(s)

    if not argv:
        print("[INFO] No parameters provided")
        return practices, file_types, parent_run_id

    # Prefer a pipe-delimited argument if present.
    pipe_arg = next((a for a in argv if "|" in a), None)
    if pipe_arg is not None:
        parts = pipe_arg.split("|")
        practice_param = _strip_wrapping_quotes(parts[0].strip()) if len(parts) > 0 else ""
        file_type_param = _strip_wrapping_quotes(parts[1].strip()) if len(parts) > 1 else ""
        run_id_param = _strip_wrapping_quotes(parts[2].strip()) if len(parts) > 2 else ""

        if practice_param and practice_param.lower() not in ("null", "none", ""):
            practices = [practice_param]
            print(f"[INFO] Practice parameter received: '{practice_param}'")
        else:
            print("[INFO] Practice parameter is empty/null")

        if file_type_param and file_type_param.lower() not in ("null", "none", ""):
            file_types = [file_type_param]
            print(f"[INFO] File type parameter received: '{file_type_param}'")
        else:
            print("[INFO] File type parameter is empty/null")

        if run_id_param and run_id_param.lower() not in ("null", "none", ""):
            parent_run_id = run_id_param
            print(f"[INFO] Parent Run ID received from upstream: '{parent_run_id}'")

        return practices, file_types, parent_run_id

    # If three (or more) args are provided, treat as positional practice/file_type/parent_run_id.
    if len(argv) >= 3:
        practice_param = _strip_wrapping_quotes(argv[0].strip())
        file_type_param = _strip_wrapping_quotes(argv[1].strip())
        run_id_param = _strip_wrapping_quotes(argv[2].strip())

        if practice_param and practice_param.lower() not in ("null", "none", ""):
            practices = [practice_param]
            print(f"[INFO] Practice parameter received (positional): '{practice_param}'")

        if file_type_param and file_type_param.lower() not in ("null", "none", ""):
            file_types = [file_type_param]
            print(f"[INFO] File type parameter received (positional): '{file_type_param}'")

        if run_id_param and run_id_param.lower() not in ("null", "none", ""):
            parent_run_id = run_id_param
            print(f"[INFO] Parent Run ID received from upstream: '{parent_run_id}'")

        return practices, file_types, parent_run_id

    # If two args are provided, treat first two as positional practice/file_type.
    if len(argv) >= 2:
        practice_param = _strip_wrapping_quotes(argv[0].strip())
        file_type_param = _strip_wrapping_quotes(argv[1].strip())

        if practice_param and practice_param.lower() not in ("null", "none", ""):
            practices = [practice_param]
            print(f"[INFO] Practice parameter received (positional): '{practice_param}'")
        else:
            print("[INFO] Practice positional parameter is empty/null")

        if file_type_param and file_type_param.lower() not in ("null", "none", ""):
            file_types = [file_type_param]
            print(f"[INFO] File type parameter received (positional): '{file_type_param}'")
        else:
            print("[INFO] File type positional parameter is empty/null")

        return practices, file_types, parent_run_id

    # Single arg remaining: support "key=value" style, else treat as practice-only.
    single = argv[0]
    if "=" in single:
        kv = {}
        for token in single.split():
            if "=" not in token:
                continue
            k, v = token.split("=", 1)
            k = k.strip().lower()
            v = _strip_wrapping_quotes(v.strip())
            if k:
                kv[k] = v

        p_val = kv.get("practice") or kv.get("practicename") or kv.get("practice_name")
        ft_val = kv.get("file_type") or kv.get("filetype")

        if p_val and p_val.lower() not in ("null", "none", ""):
            practices = [p_val]
            print(f"[INFO] Practice parameter received (named): '{p_val}'")
        else:
            print("[INFO] Practice named parameter not provided")

        if ft_val and ft_val.lower() not in ("null", "none", ""):
            file_types = [ft_val]
            print(f"[INFO] File type parameter received (named): '{ft_val}'")
        else:
            print("[INFO] File type named parameter not provided")

        return practices, file_types, parent_run_id

    # Fallback: single value treated as practice
    practice_param = _strip_wrapping_quotes(single.strip())
    if practice_param and practice_param.lower() not in ("null", "none", ""):
        practices = [practice_param]
        print(f"[INFO] Single parameter received (practice): '{practice_param}'")
    else:
        print("[INFO] Single parameter is empty/null")

    return practices, file_types, parent_run_id


# Parse parameters from EXECUTE NOTEBOOK call
PRACTICES_TO_RUN, FILE_TYPES_TO_RUN, UPSTREAM_RUN_ID = parse_parameters()

# Set PARENT_RUN_ID: use upstream value if provided, otherwise generate new
if UPSTREAM_RUN_ID:
    PARENT_RUN_ID = UPSTREAM_RUN_ID
    print(f"[INFO] Using PARENT_RUN_ID from upstream: {PARENT_RUN_ID}")
else:
    PARENT_RUN_ID = str(uuid.uuid4())
    print(f"[INFO] Generated new PARENT_RUN_ID: {PARENT_RUN_ID}")

# ==========================================================
# 2) Parameter requirement enforcement
# ==========================================================
# This notebook requires both practice and file_type. If missing, end gracefully.
REQUIRE_PARAMETERS_TO_RUN = True

# Global toggle to truncate target tables before each load (for testing only)
TRUNCATE_BEFORE_LOAD = False

# If the notebook is triggered with explicit filters and no files/data are found,
# treat it as an error so the orchestration does not report a silent success.
FAIL_IF_NO_FILES_WHEN_FILTERED = True

# Default stage naming convention retained (not used by this notebook).
STAGE_BASE = "@stage_stulhetldev001_"

# Config file location (in "raw" container)
CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
CONFIG_FILENAME = "practice_ingest_config_v17.json"

# Log table for tracking ingests
LOG_TABLE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_INGEST_LOG"

# ==========================================================
# REFINED NOTEBOOK TRIGGER CONFIGURATION
# ==========================================================
# After successful raw ingest, optionally trigger the RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE notebook
# to apply column transformations and write to refined tables.
#
# The refined notebook is triggered only if the config has refined_database/refined_schema/refined_table defined.
# Set TRIGGER_REFINED_NOTEBOOK to False to disable automatic triggering.
TRIGGER_REFINED_NOTEBOOK = True
REFINED_NOTEBOOK_PATH = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE"
REFINED_NOTEBOOK_FAIL_ON_ERROR = False  # If True, raw ETL will fail if refined notebook fails

# ==========================================================
# ARCHIVE NOTIFICATION (Logic App -> Data Factory)
# ==========================================================
# After successful file-based ingests, optionally notify a "listener" Logic App over HTTP.
# The listener Logic App can trigger an Azure Data Factory pipeline to move processed files to their archive path.
#
# Configure by setting environment variable(s) on the Snowflake Notebook runtime (or your orchestrator):
#   ARCHIVE_NOTIFY_LOGIC_APP_URL          : The Logic App HTTP trigger URL (includes SAS token).
#   ARCHIVE_NOTIFY_TIMEOUT_SECS           : HTTP timeout (default 30).
#   ARCHIVE_NOTIFY_MAX_RETRIES            : Retry attempts (default 3).
#   ARCHIVE_NOTIFY_RETRY_BACKOFF_SECS     : Base backoff seconds (default 2).
#   ARCHIVE_NOTIFY_FAIL_ON_ERROR          : true/false (default false).
#
# If ARCHIVE_NOTIFY_LOGIC_APP_URL is not set, notifications are skipped and the ETL behaves exactly as before.
ARCHIVE_NOTIFY_LOGIC_APP_URL = (os.getenv('ARCHIVE_NOTIFY_LOGIC_APP_URL') or '').strip() or None
ARCHIVE_NOTIFY_TIMEOUT_SECS = int(os.getenv('ARCHIVE_NOTIFY_TIMEOUT_SECS', '30'))
ARCHIVE_NOTIFY_MAX_RETRIES = int(os.getenv('ARCHIVE_NOTIFY_MAX_RETRIES', '3'))
ARCHIVE_NOTIFY_RETRY_BACKOFF_SECS = float(os.getenv('ARCHIVE_NOTIFY_RETRY_BACKOFF_SECS', '2'))
ARCHIVE_NOTIFY_FAIL_ON_ERROR = (os.getenv('ARCHIVE_NOTIFY_FAIL_ON_ERROR', 'false').strip().lower() in ('1','true','yes','y'))

# ==========================================================
# ERROR FILE HANDLING (Logic App -> Data Factory + Teams)
# ==========================================================
# On ETL errors, call Logic App to:
#   1. Trigger ADF pipeline to move failed files to error location
#   2. Send Teams notification (Logic App checks for "fail"/"error" in status)
#
# This uses the same Logic App as PRECHECK notebook.
ERROR_LOGIC_APP_URL = "https://prod-28.northcentralus.logic.azure.com:443/workflows/5b7115640e194503a3eacf6fa004874b/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=Xe2zUAhAvu8zbw0EQU2KFZyH6NAaJFo32unA75A0u_E"

# Set to True to enable calling Logic App on error
ENABLE_ERROR_HANDLING = True

print(f"[INFO] ========== ETL RUN STARTED ==========")
print(f"[INFO] Parent Run ID: {PARENT_RUN_ID}")
print(f"[INFO] Practice filter: {PRACTICES_TO_RUN if PRACTICES_TO_RUN else 'NONE'}")
print(f"[INFO] File type filter: {FILE_TYPES_TO_RUN if FILE_TYPES_TO_RUN else 'NONE'}")
print(f"[INFO] Truncate before load: {TRUNCATE_BEFORE_LOAD}")
print(f"[INFO] Fail if no files when filtered: {FAIL_IF_NO_FILES_WHEN_FILTERED}")
print(f"[INFO] Reading config from: {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME}")
print(f"[INFO] Logging to: {LOG_TABLE}")

# ==========================================================
# GLOBAL LOGGING FUNCTION - Defined at module level
# ==========================================================
def generate_guid():
    """Generate a new GUID/UUID string."""
    return str(uuid.uuid4())

def log_ingest(practice_name: str, file_type: str, source_type: str,
               file_name: str, target_table: str, step_name: str,
               row_count: int, status: str, start_time: float,
               end_time: float = None, error_message: str = None,
               file_count: int = None):
    """
    Logs an ingest operation to the RAW_INGEST_LOG table with GUID and timing.
    """
    try:
        log_id = generate_guid()

        if end_time is None:
            end_time = time.time()

        duration_seconds = round(end_time - start_time, 3)

        # Convert timestamps to ISO format strings for Snowflake
        start_ts = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_ts = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Escape single quotes in strings
        practice_name_safe = (practice_name or '').replace("'", "''")
        file_type_safe = (file_type or '').replace("'", "''")
        source_type_safe = (source_type or '').replace("'", "''")
        file_name_safe = (file_name or '').replace("'", "''")
        target_table_safe = (target_table or '').replace("'", "''")
        step_name_safe = (step_name or '').replace("'", "''")
        error_message_safe = (error_message or '').replace("'", "''") if error_message else None

        error_value = f"'{error_message_safe}'" if error_message_safe else "NULL"
        row_count_value = row_count if row_count is not None else "NULL"
        file_count_value = file_count if file_count is not None else "NULL"

        insert_sql = f"""
            INSERT INTO {LOG_TABLE}
            (LOG_ID, PARENT_RUN_ID, PRACTICE_NAME, FILE_TYPE, SOURCE_TYPE, FILE_NAME,
             TARGET_TABLE, STEP_NAME, ROW_COUNT, FILE_COUNT, STATUS, ERROR_MESSAGE,
             START_TIME, END_TIME, DURATION_SECONDS)
            VALUES (
                '{log_id}',
                '{PARENT_RUN_ID}',
                '{practice_name_safe}',
                '{file_type_safe}',
                '{source_type_safe}',
                '{file_name_safe}',
                '{target_table_safe}',
                '{step_name_safe}',
                {row_count_value},
                {file_count_value},
                '{status}',
                {error_value},
                '{start_ts}',
                '{end_ts}',
                {duration_seconds}
            )
        """
        session.sql(insert_sql).collect()

        if file_count is not None:
            print(f"[LOG] {step_name}: {practice_name}/{file_type} - {file_name} - {status} ({file_count} files, {duration_seconds}s)")
        else:
            print(f"[LOG] {step_name}: {practice_name}/{file_type} - {file_name} - {status} ({row_count} rows, {duration_seconds}s)")

        return log_id
    except Exception as e:
        print(f"[WARN] Failed to write to log table: {e}")
        return None

# ==========================================================
# ERROR HANDLING FUNCTION (Logic App -> ADF + Teams)
# ==========================================================
def notify_error_logic_app(practice_name: str, file_type: str, 
                           source_config: dict, archive_config: dict, 
                           error_config: dict, failed_files: list,
                           error_message: str = None,
                           event_type: str = "etl_failure") -> dict:
    """
    Calls the Logic App to:
      1. Trigger ADF pipeline to move failed files to error location
      2. Send Teams notification (Logic App handles this when status contains "fail"/"error")
    
    This is the same Logic App used by PRECHECK notebook.
    """
    result = {
        "called": False,
        "success": False,
        "response": None,
        "error": None
    }
    
    if not ENABLE_ERROR_HANDLING:
        print("[INFO] Error handling is disabled (ENABLE_ERROR_HANDLING=False)")
        return result
    
    if not ERROR_LOGIC_APP_URL or ERROR_LOGIC_APP_URL.startswith("https://<"):
        print("[WARN] Error Logic App URL not configured. Skipping error handling.")
        return result
    
    error_start = time.time()
    
    try:
        # Build the payload for the Logic App
        # Status must contain "fail" or "error" to trigger Teams notification
        payload = {
            "event_type": event_type,
            "parent_run_id": PARENT_RUN_ID,
            "practice": practice_name,
            "file_type": file_type,
            "status": "ETL_FAILED",  # This triggers Teams notification in Logic App
            "completed_utc": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "total_rows": 0,
            "source": {
                "container": source_config.get("container", "raw"),
                "directory": source_config.get("directory", ""),
                "stage_name": source_config.get("stage_name", ""),
                "stage_suffix": source_config.get("stage_suffix", "")
            },
            "archive": {
                "container": archive_config.get("container", "raw"),
                "directory": archive_config.get("directory", "")
            },
            "error": {
                "container": error_config.get("container", "raw"),
                "directory": error_config.get("directory", "")
            },
            "files": failed_files or []
        }
        
        # Add error message to the first file's reason if present
        if error_message and failed_files:
            for f in failed_files:
                if "reason" not in f or not f["reason"]:
                    f["reason"] = error_message[:200]
        
        print(f"\n[INFO] Calling Error Logic App for {practice_name}/{file_type}...")
        print(f"[INFO] Event Type: {event_type}")
        print(f"[INFO] Status: ETL_FAILED (will trigger Teams notification)")
        print(f"[INFO] Error location: {error_config.get('container', 'raw')}/{error_config.get('directory', '')}")
        print(f"[INFO] Files to move: {len(failed_files) if failed_files else 0}")
        
        # Make the HTTP POST request to Logic App
        payload_bytes = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            ERROR_LOGIC_APP_URL,
            data=payload_bytes,
            headers={"Content-Type": "application/json"},
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=30) as response:
            status_code = response.getcode()
            response_body = response.read().decode('utf-8')
            
            result["called"] = True
            result["response"] = {
                "status_code": status_code,
                "body": response_body[:500] if response_body else None
            }
            
            if status_code in [200, 202]:
                result["success"] = True
                print(f"[INFO] Error Logic App call successful (HTTP {status_code})")
                if response_body:
                    print(f"[INFO] Response: {response_body[:200]}")
            else:
                result["error"] = f"HTTP {status_code}: {response_body[:200]}"
                print(f"[ERROR] Error Logic App call failed: {result['error']}")
        
        # Log the Logic App call
        error_end = time.time()
        log_ingest(practice_name, file_type, "error_handling", "ERROR_LOGIC_APP", "",
                   "NOTIFY_ERROR", 0, "SUCCESS" if result["success"] else "ERROR",
                   error_start, error_end,
                   error_message=result.get("error"),
                   file_count=len(failed_files) if failed_files else 0)
        
    except urllib.error.HTTPError as e:
        error_end = time.time()
        try:
            body = e.read().decode('utf-8') if hasattr(e, 'read') else ''
        except Exception:
            body = ''
        result["error"] = f"HTTPError {getattr(e, 'code', None)}: {str(e)}"
        if body:
            result["error"] += f' Body: {body[:200]}'
        print(f"[ERROR] Error Logic App call failed: {result['error']}")
        log_ingest(practice_name, file_type, "error_handling", "ERROR_LOGIC_APP", "",
                   "NOTIFY_ERROR", 0, "ERROR", error_start, error_end,
                   error_message=result["error"][:500])
    
    except Exception as e:
        error_end = time.time()
        error_str = str(e)
        if "timed out" in error_str.lower() or "timeout" in error_str.lower():
            result["error"] = "Request timed out after 30 seconds"
            print(f"[ERROR] Error Logic App call timed out")
        else:
            result["error"] = error_str[:500]
            print(f"[ERROR] Error Logic App call failed: {result['error']}")
        log_ingest(practice_name, file_type, "error_handling", "ERROR_LOGIC_APP", "",
                   "NOTIFY_ERROR", 0, "ERROR", error_start, error_end,
                   error_message=result["error"])
    
    return result

# ==========================================================
# MAIN PROCESSING
# ==========================================================
if REQUIRE_PARAMETERS_TO_RUN and (not PRACTICES_TO_RUN or not FILE_TYPES_TO_RUN):
    print("\n" + "-" * 60)
    print("[INFO] Parameters are required for this notebook to run.")
    print("[INFO] No processing will be performed.")
    print(f"[INFO] Received: practice={PRACTICES_TO_RUN or 'NONE'}, file_type={FILE_TYPES_TO_RUN or 'NONE'}")
    print("-" * 60)

    # Maintain the same output blocks so downstream callers have consistent output.
    NEXT_NOTEBOOK_JOBS = []
    FILES_PROCESSED = []
    ARCHIVE_NOTIFICATIONS = []  # Track archive notifications sent to the listener Logic App
    REFINED_NOTEBOOK_RESULTS = []  # Track refined notebook execution results
    overall = {"processed": 0, "skipped_no_files": 0, "errors": 0}

    print("\n[NEXT_NOTEBOOK_JOBS_JSON]")
    print(json.dumps(NEXT_NOTEBOOK_JOBS))

    print("\n[NEXT_NOTEBOOK_JOBS_LIST]")
    for job in NEXT_NOTEBOOK_JOBS:
        print(f"{job['payer']}|{job['file_type']}")

    print("\n[DONE] Run ended gracefully due to missing required parameters.")
    print(f"[INFO] Triggered by parameters: practice={PRACTICES_TO_RUN or 'NONE'}, file_type={FILE_TYPES_TO_RUN or 'NONE'}")
else:
    # ==========================================================
    # 3) Temp FILE FORMAT for JSON
    # ==========================================================
    session.sql("""
        CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG
        TYPE = JSON
    """).collect()

    # ==========================================================
    # 3.1) Optional: Load archive notify URL from config file
    # ==========================================================
    # If env var ARCHIVE_NOTIFY_LOGIC_APP_URL is not set, you can store the URL in practice_ingest_config_v12.json at:
    #   { "ArchiveNotification": { "logic_app_url": "<https://.../triggers/manual/...>" } }
    # This is safe to add because the ingest config is still read from cfg.$1:Practices as before.
    if not ARCHIVE_NOTIFY_LOGIC_APP_URL:
        try:
            url_row = session.sql(f"""
                SELECT cfg.$1:ArchiveNotification:logic_app_url::string AS URL
                FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') cfg
                LIMIT 1
            """).collect()
            if url_row and url_row[0]["URL"]:
                ARCHIVE_NOTIFY_LOGIC_APP_URL = url_row[0]["URL"]
                print("[INFO] Archive notify URL loaded from config file (ArchiveNotification.logic_app_url).")
        except Exception as e:
            print(f"[WARN] Could not load archive notify URL from config file: {e}")

    # ==========================================================
    # 4) Load & flatten config (Practices -> ingest)
    # ==========================================================
    # Adds file_type_lc and directory_leaf_lc to support robust file_type matching.
    # v2: Added refined_db/refined_schema/refined_table to determine if IS_NEW column is needed.
    cfg_sql = f"""
        SELECT
          LOWER(p.value:practice_name::string)                         AS practice_name,
          f.value:file_type::string                                    AS file_type,
          LOWER(f.value:file_type::string)                             AS file_type_lc,
          LOWER(REGEXP_SUBSTR(RTRIM(f.value:source:directory::string,'/'), '[^/]+$'))
                                                                      AS directory_leaf_lc,
          COALESCE(f.value:source_type::string, 'file')                AS source_type,
          f.value:source:container::string                             AS src_container,
          f.value:source:stage_suffix::string                          AS src_stage_suffix,
          f.value:source:stage_name::string                            AS src_stage_name,
          f.value:source:directory::string                             AS src_directory,
          f.value:source:file_pattern::string                          AS src_pattern,
          f.value:source:delimiter::string                             AS src_delimiter,
          f.value:source:query::string                                 AS src_query,
          f.value:archive:container::string                            AS arch_container,
          f.value:archive:directory::string                            AS arch_directory,
          f.value:error:container::string                              AS err_container,
          f.value:error:directory::string                              AS err_directory,
          f.value:snowflake:database::string                           AS target_db,
          f.value:snowflake:schema::string                             AS target_schema,
          f.value:snowflake:table::string                              AS target_table,
          LOWER(f.value:snowflake:load_mode::string)                   AS load_mode,
          f.value:snowflake:refined_database::string                   AS refined_db,
          f.value:snowflake:refined_schema::string                     AS refined_schema,
          f.value:snowflake:refined_table::string                      AS refined_table
        FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
             LATERAL FLATTEN(input => cfg.$1:Practices) p,
             LATERAL FLATTEN(input => p.value:ingest) f
    """
    cfg_df = session.sql(cfg_sql)

    # Apply optional filters.
    filtered = cfg_df

    if PRACTICES_TO_RUN:
        filtered = filtered.filter(
            col("practice_name").isin([p.lower() for p in PRACTICES_TO_RUN])
        )

    if FILE_TYPES_TO_RUN:
        ft_lc = [ft.lower() for ft in FILE_TYPES_TO_RUN]
        filtered = filtered.filter(
            col("file_type_lc").isin(ft_lc) | col("directory_leaf_lc").isin(ft_lc)
        )

    rows = filtered.collect()
    if not rows:
        error_msg = (
            f"No ingest matched filters. Requested: "
            f"practice={PRACTICES_TO_RUN or 'NONE'}, file_type={FILE_TYPES_TO_RUN or 'NONE'}"
        )
        print(f"[ERROR] {error_msg}")
        raise ValueError(error_msg)

    print("[INFO] Ingest entries found matching filters:")
    filtered.select(
        "practice_name",
        "file_type",
        "file_type_lc",
        "directory_leaf_lc",
        "source_type",
        "target_db",
        "target_schema",
        "target_table"
    ).show()

    # ==========================================================
    # Helper Functions
    # ==========================================================

    def _is_configured_value(v) -> bool:
        """True if v is a meaningful configured value (not None/empty/'N/A'/'NULL'/etc.)."""
        if v is None:
            return False
        s = str(v).strip()
        if not s:
            return False
        if s.lower() in ("none", "null", "n/a", "na"):
            return False
        return True

    def list_matching_files(src_stage_prefix: str, pattern: str):
        """
        Returns (count, rows_df) for files visible under src_stage_prefix that match PATTERN.
        Each row has columns: name, size, md5, last_modified.
        """
        print("\n[INFO] LIST (no filter):")
        session.sql(f"LIST {src_stage_prefix}").show()

        print("\n[INFO] LIST filtered by PATTERN:")
        df = session.sql(f"LIST {src_stage_prefix} PATTERN='{pattern}'")
        df.show()
        return df.count(), df

    def read_with_pattern(src_stage_prefix: str, pattern: str, delimiter: str) -> DataFrame:
        """
        Reads files from a stage prefix using Snowpark CSV reader and regex PATTERN,
        honoring the provided delimiter from config.
        """
        delim = delimiter or ","
        options = {
            "TYPE": "CSV",
            "FIELD_DELIMITER": delim,
            "FIELD_OPTIONALLY_ENCLOSED_BY": '"',
            "PARSE_HEADER": True,
            "SKIP_BLANK_LINES": True,
            "TRIM_SPACE": True,
            "ERROR_ON_COLUMN_COUNT_MISMATCH": False,
            "PATTERN": pattern
        }
        return session.read.options(options).csv(src_stage_prefix)

    def read_from_query(query: str) -> DataFrame:
        """Executes a SQL query and returns the result as a DataFrame."""
        print(f"\n[INFO] Executing query source:\n{query[:500]}..." if len(query) > 500 else f"\n[INFO] Executing query source:\n{query}")
        return session.sql(query)

    def qualify_table_name(db: str, schema: str, table: str) -> str:
        """Build fully qualified table name."""
        if db and schema and table:
            return f"{db}.{schema}.{table}"
        if schema and table:
            return f"{schema}.{table}"
        return table

    def get_row_count(fqtn: str) -> int:
        """Returns COUNT(*) for fqtn; -1 if table doesn't exist."""
        try:
            return session.sql(f"SELECT COUNT(*) AS C FROM {fqtn}").collect()[0]["C"]
        except Exception:
            return -1

    def truncate_table(fqtn: str):
        """Truncates the target table if it exists; no-op if it doesn't."""
        try:
            print(f"[INFO] Truncating table {fqtn} ...")
            session.sql(f"TRUNCATE TABLE {fqtn}").collect()
            print(f"[INFO] Table {fqtn} truncated.")
        except Exception as e:
            print(f"[WARN] TRUNCATE skipped for {fqtn} (likely not existing yet). Proceeding. Details: {e}")

    def write_to_table(df: DataFrame, fqtn: str, load_mode: str, src_count: int,
                       truncate_before: bool = False):
        """
        Writes df to fqtn, with optional TRUNCATE BEFORE LOAD.
        - If truncate_before=True: TRUNCATE TABLE and then APPEND the rows.
        - Else: use load_mode (append/overwrite), where 'overwrite' drops-recreates.
        """
        if truncate_before:
            truncate_table(fqtn)
            mode_to_use = "append"
        else:
            mode_to_use = "append" if load_mode not in ("overwrite", "truncate") else "overwrite"

        before_cnt = get_row_count(fqtn)
        if before_cnt >= 0:
            print(f"[INFO] Pre-load row count for {fqtn}: {before_cnt}")
        else:
            print(f"[INFO] Target table {fqtn} does not exist yet; it will be created (mode={mode_to_use}).")

        print(f"\n[INFO] Writing to {fqtn} (mode={mode_to_use})...")
        df.write.mode(mode_to_use).save_as_table(fqtn)

        after_cnt = get_row_count(fqtn)
        if after_cnt >= 0:
            print(f"[SUCCESS] Wrote {src_count} row(s) to {fqtn}.")
            print(f"[INFO] Post-load row count: {after_cnt}")
        else:
            print(f"[WARN] Could not verify post-load row count for {fqtn}.")

        print("\n[INFO] Preview of data in target table:")
        session.table(fqtn).show(10)

    def _basename_from_stage_path(path_value: str) -> str:
        """Returns just the file name portion from a stage path or URL-like string."""
        if not path_value:
            return path_value
        return path_value.split("/")[-1]

    def clean_error_message(error_msg: str) -> str:
        """Cleans Snowflake error messages by removing prefix containing error codes and query IDs."""
        if not error_msg:
            return error_msg
        pattern = r'^\(\d+\):\s*[a-f0-9-]+:\s*\d+\s*\([A-Z0-9]+\):\s*'
        cleaned = re.sub(pattern, '', error_msg, flags=re.IGNORECASE)
        return cleaned

    # ==========================================================
    # ARCHIVE NOTIFICATION HELPERS
    # ==========================================================
    def _http_post_json(url: str, payload: dict, timeout_secs: int):
        """POST JSON to a URL. Returns (status_code, response_text)."""
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
            status_code = getattr(resp, 'status', None) or resp.getcode()
            try:
                resp_body = resp.read()
                resp_text = resp_body.decode('utf-8') if resp_body else ''
            except Exception:
                resp_text = ''
            return status_code, resp_text

    def notify_archive_listener(
        practice_name: str,
        file_type: str,
        source_type: str,
        file_name_for_log: str,
        target_table: str,
        row_count: int,
        file_count: int,
        payload: dict
    ) -> bool:
        """Notify the listener Logic App that files are ready to be archived."""
        if not ARCHIVE_NOTIFY_LOGIC_APP_URL:
            print('[INFO] Archive notification skipped (ARCHIVE_NOTIFY_LOGIC_APP_URL is not set).')
            return True

        notify_start = time.time()
        last_error = None

        for attempt in range(1, ARCHIVE_NOTIFY_MAX_RETRIES + 1):
            try:
                status_code, resp_text = _http_post_json(
                    ARCHIVE_NOTIFY_LOGIC_APP_URL, payload, ARCHIVE_NOTIFY_TIMEOUT_SECS
                )

                if 200 <= int(status_code) < 300:
                    notify_end = time.time()
                    log_ingest(
                        practice_name, file_type, source_type, file_name_for_log, target_table,
                        'ARCHIVE_NOTIFY', row_count, 'SUCCESS', notify_start, notify_end,
                        file_count=file_count
                    )
                    print(f'[INFO] Archive listener notified successfully (HTTP {status_code}).')
                    if resp_text:
                        print(f"[INFO] Archive listener response (truncated): {resp_text[:500]}")
                    return True

                last_error = f'HTTP {status_code} returned by archive listener.'
                if resp_text:
                    last_error += f' Body: {resp_text[:500]}'
                print(f'[WARN] Archive notification attempt {attempt} failed: {last_error}')

            except urllib.error.HTTPError as e:
                try:
                    body = e.read().decode('utf-8') if hasattr(e, 'read') else ''
                except Exception:
                    body = ''
                last_error = f'HTTPError {getattr(e, "code", None)}: {str(e)}'
                if body:
                    last_error += f' Body: {body[:500]}'
                print(f'[WARN] Archive notification attempt {attempt} failed: {last_error}')

            except Exception as e:
                last_error = str(e)
                print(f'[WARN] Archive notification attempt {attempt} failed: {last_error}')

            if attempt < ARCHIVE_NOTIFY_MAX_RETRIES:
                time.sleep(ARCHIVE_NOTIFY_RETRY_BACKOFF_SECS * attempt)

        notify_end = time.time()
        err_msg = clean_error_message(last_error)[:500] if last_error else 'Unknown error'
        log_ingest(
            practice_name, file_type, source_type, file_name_for_log, target_table,
            'ARCHIVE_NOTIFY', row_count, 'ERROR', notify_start, notify_end,
            error_message=err_msg,
            file_count=file_count
        )

        if ARCHIVE_NOTIFY_FAIL_ON_ERROR:
            raise RuntimeError(f'Archive notification failed after {ARCHIVE_NOTIFY_MAX_RETRIES} attempt(s): {err_msg}')

        print(f'[WARN] Archive notification ultimately failed; continuing because ARCHIVE_NOTIFY_FAIL_ON_ERROR=false. Last error: {err_msg}')
        return False

    # ==========================================================
    # REFINED NOTEBOOK TRIGGER HELPER
    # ==========================================================
    def trigger_refined_notebook(practice_name: str, file_type: str, source_type: str,
                                  target_table: str, row_count: int) -> dict:
        """
        Trigger the RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE notebook to apply column transformations.
        
        Returns a dict with status information:
          {"triggered": bool, "status": str, "error": str or None, "duration": float}
        """
        result = {
            "triggered": False,
            "status": "SKIPPED",
            "error": None,
            "duration": 0.0
        }
        
        if not TRIGGER_REFINED_NOTEBOOK:
            print(f"[INFO] Refined notebook trigger is disabled (TRIGGER_REFINED_NOTEBOOK=False).")
            return result
        
        refined_start = time.time()
        
        try:
            # Pass practice, file_type, and parent_run_id to the refined notebook
            # Using pipe-delimited format for compatibility
            notebook_args = f"'{practice_name}|{file_type}|{PARENT_RUN_ID}'"
            
            # execute_sql = f"EXECUTE NOTEBOOK {REFINED_NOTEBOOK_PATH}({notebook_args})"
            execute_sql = f"CALL UPPERLINE.RAW_INBOUND_AFFILIATEDATA.SP_EXEC_REFINED_NOTEBOOK({notebook_args})"
            print(f"\n[INFO] Triggering refined notebook...")
            print(f"[INFO] Command: {execute_sql}")
            
            # Execute the notebook
            notebook_result = session.sql(execute_sql).collect()
            
            refined_end = time.time()
            result["duration"] = round(refined_end - refined_start, 3)
            result["triggered"] = True
            result["status"] = "SUCCESS"
            
            print(f"[INFO] Refined notebook completed successfully in {result['duration']}s")
            if notebook_result:
                print(f"[INFO] Notebook result: {notebook_result}")
            
            log_ingest(practice_name, file_type, source_type, "refined_notebook", target_table,
                       "TRIGGER_REFINED_NOTEBOOK", row_count, "SUCCESS", refined_start, refined_end)
            
        except Exception as e:
            refined_end = time.time()
            result["duration"] = round(refined_end - refined_start, 3)
            result["triggered"] = True
            result["status"] = "ERROR"
            result["error"] = clean_error_message(str(e))[:500]
            
            print(f"[ERROR] Refined notebook failed: {result['error']}")
            
            log_ingest(practice_name, file_type, source_type, "refined_notebook", target_table,
                       "TRIGGER_REFINED_NOTEBOOK", 0, "ERROR", refined_start, refined_end,
                       error_message=result["error"])
            
            if REFINED_NOTEBOOK_FAIL_ON_ERROR:
                raise RuntimeError(f"Refined notebook failed: {result['error']}")
        
        return result

    # ==========================================================
    # 5) Process each ingest
    # ==========================================================
    overall = {"processed": 0, "skipped_no_files": 0, "errors": 0}
    NEXT_NOTEBOOK_JOBS = []
    FILES_PROCESSED = []
    ARCHIVE_NOTIFICATIONS = []  # Track archive notifications sent to the listener Logic App
    REFINED_NOTEBOOK_RESULTS = []  # Track refined notebook execution results

    etl_start_time = time.time()

    for r in rows:
        practice_name    = r["PRACTICE_NAME"]    # payer
        file_type        = r["FILE_TYPE"]
        source_type      = (r["SOURCE_TYPE"] or "file").lower()

        src_container    = r["SRC_CONTAINER"]
        src_stage_suffix = r["SRC_STAGE_SUFFIX"]
        src_stage_name   = r["SRC_STAGE_NAME"]
        src_directory    = (r["SRC_DIRECTORY"] or "").rstrip("/")
        src_pattern      = r["SRC_PATTERN"]
        src_delimiter    = (r["SRC_DELIMITER"] or ",")
        src_query        = r["SRC_QUERY"]

        arch_container   = r["ARCH_CONTAINER"]
        arch_directory   = (r["ARCH_DIRECTORY"] or "").rstrip("/")
        err_container    = r["ERR_CONTAINER"]
        err_directory    = (r["ERR_DIRECTORY"] or "").rstrip("/")

        target_db        = r["TARGET_DB"]
        target_schema    = r["TARGET_SCHEMA"]
        target_table     = r["TARGET_TABLE"]
        load_mode        = (r["LOAD_MODE"] or "append").lower()

        # v2: Extract refined config to determine if IS_NEW column is needed
        refined_db       = r["REFINED_DB"]
        refined_sch      = r["REFINED_SCHEMA"]
        refined_tbl      = r["REFINED_TABLE"]
        has_refined = all(_is_configured_value(v) for v in (refined_db, refined_sch, refined_tbl))

        fq_table = qualify_table_name(target_db, target_schema, target_table)

        print("\n" + "=" * 72)
        print(f"[INFO] Ingest: {practice_name} :: {file_type}")
        print(f"  - source type:   {source_type}")
        print(f"  - has refined:   {has_refined}")

        # ------------------------------------------------------
        # Handle different source types
        # ------------------------------------------------------
        if source_type == "query":
            ingest_start_time = time.time()

            if not src_query:
                print(f"[ERROR] Missing required 'query' in config for query-based ingest: {practice_name} :: {file_type}")
                print("[INFO] This ingest will be skipped. Please update the config with 'source.query'.")
                overall["errors"] += 1
                log_ingest(practice_name, file_type, source_type, "query_source", fq_table,
                           "CONFIG_ERROR", 0, "ERROR", ingest_start_time,
                           error_message="Missing required 'query' in config")
                continue

            print(f"  - source query:  {src_query[:100]}..." if len(src_query) > 100 else f"  - source query:  {src_query}")
            print(f"  - target table:  {fq_table}")
            print(f"  - load mode:     {load_mode}")
            print("=" * 72)

            try:
                read_start_time = time.time()
                df_raw = read_from_query(src_query)

                print("\n[INFO] Schema from query:")
                df_raw.print_schema()

                print("\n[INFO] Sample rows (up to 10):")
                df_raw.show(10)

                src_count = df_raw.count()
                read_end_time = time.time()

                print(f"[INFO] Source row count: {src_count}")
                log_ingest(practice_name, file_type, source_type, "query_source", fq_table,
                           "READ_QUERY", src_count, "SUCCESS", read_start_time, read_end_time)

                if src_count == 0:
                    print(f"[INFO] Zero rows returned from query for {practice_name}/{file_type}. Skipping.")
                    overall["skipped_no_files"] += 1
                    log_ingest(practice_name, file_type, source_type, "query_source", fq_table,
                               "PROCESS_COMPLETE", 0, "SKIPPED", ingest_start_time,
                               error_message="Zero rows returned from query")
                    continue

                file_name_value = "query_source"
                # v2: Add metadata columns; IS_NEW only when refined pipeline exists
                df_with_meta = (
                    df_raw
                    .withColumn("file_name", lit(file_name_value))
                    .withColumn("file_load_time", current_timestamp())
                    .withColumn("PARENT_RUN_ID", lit(PARENT_RUN_ID))
                )
                if has_refined:
                    df_with_meta = df_with_meta.withColumn("IS_NEW", lit(1))

                write_start_time = time.time()
                write_to_table(
                    df_with_meta,
                    fq_table,
                    load_mode,
                    src_count,
                    truncate_before=TRUNCATE_BEFORE_LOAD
                )
                write_end_time = time.time()

                log_ingest(practice_name, file_type, source_type, "query_source", fq_table,
                           "WRITE_TABLE", src_count, "SUCCESS", write_start_time, write_end_time)

                FILES_PROCESSED.append({
                    "practice": practice_name,
                    "file_type": file_type,
                    "file_name": "query_source",
                    "status": "SUCCESS",
                    "rows": src_count,
                    "error": None
                })

                overall["processed"] += 1

                log_ingest(practice_name, file_type, source_type, "query_source", fq_table,
                           "PROCESS_COMPLETE", src_count, "SUCCESS", ingest_start_time)

                NEXT_NOTEBOOK_JOBS.append({
                    "payer": practice_name,
                    "file_type": file_type
                })

                # v2: Only trigger refined notebook when refined pipeline is configured
                if has_refined:
                    refined_result = trigger_refined_notebook(
                        practice_name=practice_name,
                        file_type=file_type,
                        source_type=source_type,
                        target_table=fq_table,
                        row_count=src_count
                    )
                    REFINED_NOTEBOOK_RESULTS.append({
                        "practice": practice_name,
                        "file_type": file_type,
                        **refined_result
                    })
                else:
                    print(f"[INFO] No refined config for {practice_name}/{file_type} - skipping refined notebook trigger.")

            except Exception as e:
                overall["errors"] += 1
                error_msg = clean_error_message(str(e))[:500]
                print(f"[ERROR] Query-based ingest failed for {practice_name}/{file_type}: {e}")
                log_ingest(practice_name, file_type, source_type, "query_source", fq_table,
                           "PROCESS_COMPLETE", 0, "ERROR", ingest_start_time, error_message=error_msg)

                FILES_PROCESSED.append({
                    "practice": practice_name,
                    "file_type": file_type,
                    "file_name": "query_source",
                    "status": "ERROR",
                    "rows": 0,
                    "error": error_msg
                })
                
                # Call Logic App to send Teams notification (no files to move for query-based)
                notify_error_logic_app(
                    practice_name=practice_name,
                    file_type=file_type,
                    source_config={"container": "", "directory": "", "stage_name": "", "stage_suffix": ""},
                    archive_config={"container": arch_container, "directory": arch_directory},
                    error_config={"container": err_container, "directory": err_directory},
                    failed_files=[{
                        "file_name": "query_source",
                        "status": "ETL_FAILED",
                        "rows": 0,
                        "reason": f"Query failed: {error_msg[:150]}"
                    }],
                    error_message=error_msg,
                    event_type="etl_query_failure"
                )

        else:
            # File-based source
            ingest_start_time = time.time()

            # Require stage_name; gracefully skip if missing
            if not src_stage_name:
                print(f"[ERROR] Missing required 'stage_name' in config for file-based ingest: {practice_name} :: {file_type}")
                print("[INFO] This ingest will be skipped. Please update the config with 'source.stage_name'.")
                overall["errors"] += 1
                log_ingest(practice_name, file_type, source_type, src_pattern or "*.csv", fq_table,
                           "CONFIG_ERROR", 0, "ERROR", ingest_start_time,
                           error_message="Missing required 'stage_name' in config")
                continue

            # Resolve stage from provided stage_name
            if src_stage_name.startswith("@"):
                src_stage = src_stage_name
            else:
                src_stage = f"@{src_stage_name}"

            if src_directory:
                src_stage_prefix = f"{src_stage}/{src_directory}/"
            else:
                src_stage_prefix = f"{src_stage}/"

            print(f"  - source stage:  {src_stage_prefix}")
            print(f"  - file pattern:  {src_pattern}")
            print(f"  - delimiter:     {src_delimiter}")
            print(f"  - archive path:  {arch_container}/{arch_directory}")
            print(f"  - error path:    {err_container}/{err_directory}")
            print(f"  - target table:  {fq_table}")
            print(f"  - load mode:     {load_mode}")
            print("=" * 72)

            # Step 1: List files
            list_start_time = time.time()
            match_count, list_df = list_matching_files(src_stage_prefix, src_pattern)
            list_end_time = time.time()

            log_ingest(practice_name, file_type, source_type, src_pattern or "*.csv", fq_table,
                       "LIST_FILES", None, "SUCCESS", list_start_time, list_end_time, file_count=match_count)

            if match_count == 0:
                msg = f"No files found for {practice_name}/{file_type} at {src_stage_prefix} PATTERN={src_pattern}"
                print(f"[WARN] {msg}")

                # If this is a filtered run with both practice and file_type provided, fail fast.
                if FAIL_IF_NO_FILES_WHEN_FILTERED and PRACTICES_TO_RUN and FILE_TYPES_TO_RUN:
                    overall["errors"] += 1
                    log_ingest(practice_name, file_type, source_type, src_pattern or "*.csv", fq_table,
                               "PROCESS_COMPLETE", 0, "ERROR", ingest_start_time, error_message=msg)
                    raise ValueError(msg)

                overall["skipped_no_files"] += 1
                log_ingest(practice_name, file_type, source_type, src_pattern or "*.csv", fq_table,
                           "PROCESS_COMPLETE", 0, "SKIPPED", ingest_start_time, error_message=msg)
                continue

            file_rows = list_df.collect()
            current_file = src_pattern or "*.csv"

            try:
                first_file = True
                total_src_count = 0
                files_for_archive = []  # Files to be moved to archive (and/or error) by Data Factory

                for file_row in file_rows:
                    full_path = file_row["name"]
                    basename = _basename_from_stage_path(full_path)
                    current_file = basename

                    escaped_basename = re.escape(basename)
                    single_pattern = f".*{escaped_basename}$"

                    print(f"\n[INFO] Processing file: {basename}")
                    print(f"[INFO] Using PATTERN: {single_pattern}")

                    read_start_time = time.time()
                    df_raw = read_with_pattern(src_stage_prefix, single_pattern, src_delimiter)

                    print("\n[INFO] Schema inferred by Snowpark for this file:")
                    df_raw.print_schema()

                    print("\n[INFO] Sample rows from this file (up to 10):")
                    df_raw.show(10)

                    src_count = df_raw.count()
                    read_end_time = time.time()

                    print(f"[INFO] Source row count for file {basename}: {src_count}")

                    log_ingest(practice_name, file_type, source_type, basename, fq_table,
                               "READ_FILE", src_count, "SUCCESS", read_start_time, read_end_time)

                    if src_count == 0:
                        print(f"[INFO] Zero rows read for file {basename}. Skipping this file.")
                        log_ingest(practice_name, file_type, source_type, basename, fq_table,
                                   "PROCESS_FILE", 0, "SKIPPED", read_start_time,
                                   error_message="Zero rows in file")
                        files_for_archive.append({
                            "file_name": basename,
                            "status": "SKIPPED",
                            "rows": 0,
                            "reason": "Zero rows in file"
                        })
                        continue

                    total_src_count += src_count

                    # v2: Add metadata columns; IS_NEW only when refined pipeline exists
                    df_with_meta = (
                        df_raw
                        .withColumn("file_name", lit(basename))
                        .withColumn("file_load_time", current_timestamp())
                        .withColumn("PARENT_RUN_ID", lit(PARENT_RUN_ID))
                    )
                    if has_refined:
                        df_with_meta = df_with_meta.withColumn("IS_NEW", lit(1))

                    write_start_time = time.time()
                    write_to_table(
                        df_with_meta,
                        fq_table,
                        load_mode,
                        src_count,
                        truncate_before=(TRUNCATE_BEFORE_LOAD and first_file)
                    )
                    write_end_time = time.time()

                    log_ingest(practice_name, file_type, source_type, basename, fq_table,
                               "WRITE_TABLE", src_count, "SUCCESS", write_start_time, write_end_time)

                    FILES_PROCESSED.append({
                        "practice": practice_name,
                        "file_type": file_type,
                        "file_name": basename,
                        "status": "SUCCESS",
                        "rows": src_count,
                        "error": None
                    })
                    files_for_archive.append({
                        "file_name": basename,
                        "status": "SUCCESS",
                        "rows": src_count
                    })

                    first_file = False

                if total_src_count == 0:
                    msg = f"No non-empty files were loaded for {practice_name}/{file_type}."
                    print(f"[WARN] {msg}")

                    # If this is a filtered run with both practice and file_type provided, fail fast.
                    if FAIL_IF_NO_FILES_WHEN_FILTERED and PRACTICES_TO_RUN and FILE_TYPES_TO_RUN:
                        overall["errors"] += 1
                        log_ingest(practice_name, file_type, source_type, current_file, fq_table,
                                   "PROCESS_COMPLETE", 0, "ERROR", ingest_start_time, error_message=msg)
                        raise ValueError(msg)


                    # Notify listener Logic App so Data Factory can archive the processed file(s).
                    if files_for_archive:
                        archive_payload = {
                            "event_type": "practice_file_ingest_complete",
                            "parent_run_id": PARENT_RUN_ID,
                            "practice": practice_name,
                            "file_type": file_type,
                            "source": {
                                "container": src_container,
                                "directory": src_directory,
                                "stage_name": src_stage_name,
                                "stage_suffix": src_stage_suffix
                            },
                            "archive": {"container": arch_container, "directory": arch_directory},
                            "error": {"container": err_container, "directory": err_directory},
                            "files": files_for_archive,
                            "total_rows": total_src_count,
                            "status": "SKIPPED_NO_ROWS",
                            "completed_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                        }

                        notify_ok = notify_archive_listener(
                            practice_name=practice_name,
                            file_type=file_type,
                            source_type=source_type,
                            file_name_for_log=(src_pattern or current_file),
                            target_table=fq_table,
                            row_count=total_src_count,
                            file_count=len(files_for_archive),
                            payload=archive_payload
                        )

                        ARCHIVE_NOTIFICATIONS.append({
                            "practice": practice_name,
                            "file_type": file_type,
                            "status": "SUCCESS" if notify_ok else "ERROR",
                            "files": len(files_for_archive)
                        })
                    else:
                        print("[INFO] No files eligible for archive notification in this ingest.")

                    overall["skipped_no_files"] += 1
                    log_ingest(practice_name, file_type, source_type, src_pattern or "*.csv", fq_table,
                               "PROCESS_COMPLETE", 0, "SKIPPED", ingest_start_time, error_message=msg)
                    continue


                # Notify listener Logic App so Data Factory can archive the processed file(s).
                if files_for_archive:
                    archive_payload = {
                        "event_type": "practice_file_ingest_complete",
                        "parent_run_id": PARENT_RUN_ID,
                        "practice": practice_name,
                        "file_type": file_type,
                        "source": {
                            "container": src_container,
                            "directory": src_directory,
                            "stage_name": src_stage_name,
                            "stage_suffix": src_stage_suffix
                        },
                        "archive": {"container": arch_container, "directory": arch_directory},
                        "error": {"container": err_container, "directory": err_directory},
                        "files": files_for_archive,
                        "total_rows": total_src_count,
                        "status": "SUCCESS",
                        "completed_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    }

                    notify_ok = notify_archive_listener(
                        practice_name=practice_name,
                        file_type=file_type,
                        source_type=source_type,
                        file_name_for_log=current_file,
                        target_table=fq_table,
                        row_count=total_src_count,
                        file_count=len(files_for_archive),
                        payload=archive_payload
                    )

                    ARCHIVE_NOTIFICATIONS.append({
                        "practice": practice_name,
                        "file_type": file_type,
                        "status": "SUCCESS" if notify_ok else "ERROR",
                        "files": len(files_for_archive)
                    })
                else:
                    print("[INFO] No files eligible for archive notification in this ingest.")

                overall["processed"] += 1

                log_ingest(practice_name, file_type, source_type, current_file, fq_table,
                           "PROCESS_COMPLETE", total_src_count, "SUCCESS", ingest_start_time)

                NEXT_NOTEBOOK_JOBS.append({
                    "payer": practice_name,
                    "file_type": file_type
                })

                # v2: Only trigger refined notebook when refined pipeline is configured
                if has_refined:
                    refined_result = trigger_refined_notebook(
                        practice_name=practice_name,
                        file_type=file_type,
                        source_type=source_type,
                        target_table=fq_table,
                        row_count=total_src_count
                    )
                    REFINED_NOTEBOOK_RESULTS.append({
                        "practice": practice_name,
                        "file_type": file_type,
                        **refined_result
                    })
                else:
                    print(f"[INFO] No refined config for {practice_name}/{file_type} - skipping refined notebook trigger.")

            except Exception as e:
                overall["errors"] += 1
                error_msg = clean_error_message(str(e))[:500]
                print(f"[ERROR] File-based ingest failed for {practice_name}/{file_type}: {e}")
                log_ingest(practice_name, file_type, source_type, current_file, fq_table,
                           "PROCESS_COMPLETE", 0, "ERROR", ingest_start_time, error_message=error_msg)

                FILES_PROCESSED.append({
                    "practice": practice_name,
                    "file_type": file_type,
                    "file_name": current_file,
                    "status": "ERROR",
                    "rows": 0,
                    "error": error_msg
                })
                
                # Call Logic App to move files to error location and send Teams notification
                failed_files_for_error = [{
                    "file_name": current_file,
                    "status": "ETL_FAILED",
                    "rows": 0,
                    "reason": error_msg[:200]
                }]
                
                source_config = {
                    "container": src_container,
                    "directory": src_directory,
                    "stage_name": src_stage_name,
                    "stage_suffix": src_stage_suffix
                }
                archive_config = {"container": arch_container, "directory": arch_directory}
                error_config = {"container": err_container, "directory": err_directory}
                
                notify_error_logic_app(
                    practice_name=practice_name,
                    file_type=file_type,
                    source_config=source_config,
                    archive_config=archive_config,
                    error_config=error_config,
                    failed_files=failed_files_for_error,
                    error_message=error_msg,
                    event_type="etl_failure"
                )

    # ==========================================================
    # 6) Summary
    # ==========================================================
    etl_end_time = time.time()
    etl_duration = round(etl_end_time - etl_start_time, 3)

    files_success = sum(1 for f in FILES_PROCESSED if f["status"] == "SUCCESS")
    files_error = sum(1 for f in FILES_PROCESSED if f["status"] == "ERROR")
    total_rows_loaded = sum(f["rows"] for f in FILES_PROCESSED if f["status"] == "SUCCESS")

    # Refined notebook stats
    refined_triggered = sum(1 for r in REFINED_NOTEBOOK_RESULTS if r.get("triggered"))
    refined_success = sum(1 for r in REFINED_NOTEBOOK_RESULTS if r.get("status") == "SUCCESS")
    refined_error = sum(1 for r in REFINED_NOTEBOOK_RESULTS if r.get("status") == "ERROR")
    refined_skipped = sum(1 for r in REFINED_NOTEBOOK_RESULTS if r.get("status") == "SKIPPED")

    print("\n" + "-" * 60)
    print("[SUMMARY]")
    print(f"  Parent Run ID                 : {PARENT_RUN_ID}")
    print(f"  Parameters Received           : practice={PRACTICES_TO_RUN or 'NONE'}, file_type={FILE_TYPES_TO_RUN or 'NONE'}")
    print(f"  Total ETL Duration            : {etl_duration} seconds")
    print(f"  Ingest processed successfully : {overall['processed']}")
    print(f"  Ingest skipped (no files/data): {overall['skipped_no_files']}")
    print(f"  Ingest with errors            : {overall['errors']}")
    print("-" * 60)
    print(f"  Files processed successfully  : {files_success}")
    print(f"  Files with errors             : {files_error}")
    print(f"  Total rows loaded             : {total_rows_loaded}")
    print("-" * 60)
    print(f"  Refined notebooks triggered   : {refined_triggered}")
    print(f"  Refined notebooks succeeded   : {refined_success}")
    print(f"  Refined notebooks failed      : {refined_error}")
    print(f"  Refined notebooks skipped     : {refined_skipped}")
    print("-" * 60)

    if FILES_PROCESSED:
        print("\n[FILES PROCESSED]")
        print(f"{'PRACTICE':<15} {'FILE_TYPE':<20} {'FILE_NAME':<50} {'STATUS':<10} {'ROWS':<10} {'ERROR'}")
        print("-" * 140)
        for f in FILES_PROCESSED:
            error_display = (f["error"][:40] + "...") if f["error"] and len(f["error"]) > 40 else (f["error"] or "")
            print(f"{f['practice']:<15} {f['file_type']:<20} {f['file_name']:<50} {f['status']:<10} {f['rows']:<10} {error_display}")
        print("-" * 140)

    if REFINED_NOTEBOOK_RESULTS:
        print("\n[REFINED NOTEBOOK EXECUTIONS]")
        print(f"{'PRACTICE':<15} {'FILE_TYPE':<20} {'TRIGGERED':<10} {'STATUS':<10} {'DURATION':<12} {'ERROR'}")
        print("-" * 120)
        for r in REFINED_NOTEBOOK_RESULTS:
            error_display = (r["error"][:40] + "...") if r.get("error") and len(r["error"]) > 40 else (r.get("error") or "")
            print(f"{r['practice']:<15} {r['file_type']:<20} {str(r.get('triggered', False)):<10} {r.get('status', 'N/A'):<10} {str(r.get('duration', 0)) + 's':<12} {error_display}")
        print("-" * 120)

    # ==========================================================
    # 7) Exit values for downstream notebook(s)
    # ==========================================================
    print("\n[NEXT_NOTEBOOK_JOBS_JSON]")
    print(json.dumps(NEXT_NOTEBOOK_JOBS))

    print("\n[NEXT_NOTEBOOK_JOBS_LIST]")
    for job in NEXT_NOTEBOOK_JOBS:
        print(f"{job['payer']}|{job['file_type']}")

    print("\n[DONE] Multi-practice ingest completed.")
    print(f"[INFO] Triggered by parameters: practice={PRACTICES_TO_RUN or 'NONE'}, file_type={FILE_TYPES_TO_RUN or 'NONE'}")
