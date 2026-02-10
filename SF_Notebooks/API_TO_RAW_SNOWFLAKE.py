# API_TO_RAW_SNOWFLAKE.py
# ==========================================================
# Notebook for loading HealthJump API data into Snowflake
# Uses practice_ingest_config JSON for table configuration
# ==========================================================
#
# PARAMETER REQUIREMENT:
# ==========================================================
# This notebook requires BOTH practice and file_type to be provided.
#
# Supports:
#   1) Two positional args: practice, file_type
#   2) Single pipe-delimited arg: "practice|file_type"
#   3) Single named-arg string: "practice=... file_type=..."
#
# Example scheduled run:
#   EXECUTE NOTEBOOK ...('healthjump', 'AppointmentData')
#   EXECUTE NOTEBOOK ...('healthjump|AppointmentData')
#
# ==========================================================
# API REFRESH:
# ==========================================================
# Before pulling data, this notebook sends a POST request to refresh
# the query results, then waits for the refresh to complete before
# fetching the data.
#
# Refresh endpoint: POST /api/queries/<query_id>/refresh
# Results endpoint: GET /api/queries/<query_id>/results.json
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
import re
from datetime import datetime

# ==========================================================
# 0) Session initialization
# ==========================================================
session = get_active_session()

PARENT_RUN_ID = str(uuid.uuid4())
LOG_TABLE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_INGEST_LOG"

# Refresh wait time in seconds
REFRESH_WAIT_SECONDS = 120

# ==========================================================
# 1) PARAMETER HANDLING - Accept from EXECUTE NOTEBOOK
# ==========================================================
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
      1) Two positional args: practice, file_type
      2) Single pipe-delimited arg: "practice|file_type"
      3) Single practice only
      4) Single named-arg string: "practice=... file_type=..."

    Returns:
      (practices_list, file_types_list)
    """
    practices = ["healthjump"]
    file_types = ["AppointmentData"]

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
        return practices, file_types

    # Prefer a pipe-delimited argument if present.
    pipe_arg = next((a for a in argv if "|" in a), None)
    if pipe_arg is not None:
        practice_param, file_type_param = (pipe_arg.split("|", 1) + [""])[:2]
        practice_param = _strip_wrapping_quotes(practice_param.strip())
        file_type_param = _strip_wrapping_quotes(file_type_param.strip())

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

        return practices, file_types

    # If two (or more) args are provided, treat first two as positional practice/file_type.
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

        return practices, file_types

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

        return practices, file_types

    # Fallback: single value treated as practice
    practice_param = _strip_wrapping_quotes(single.strip())
    if practice_param and practice_param.lower() not in ("null", "none", ""):
        practices = [practice_param]
        print(f"[INFO] Single parameter received (practice): '{practice_param}'")
    else:
        print("[INFO] Single parameter is empty/null")

    return practices, file_types


# Parse parameters from EXECUTE NOTEBOOK call
PRACTICES_TO_RUN, FILE_TYPES_TO_RUN = parse_parameters()

# Extract single values for this notebook
PRACTICE_NAME = PRACTICES_TO_RUN[0] if PRACTICES_TO_RUN else None
FILE_TYPE = FILE_TYPES_TO_RUN[0] if FILE_TYPES_TO_RUN else None

print(f"[INFO] ========== API INGEST STARTED ==========")
print(f"[INFO] Parent Run ID: {PARENT_RUN_ID}")
print(f"[INFO] Practice: {PRACTICE_NAME or 'NONE'}")
print(f"[INFO] File Type: {FILE_TYPE or 'NONE'}")

# ==========================================================
# 2) Parameter requirement enforcement
# ==========================================================
if not PRACTICE_NAME or not FILE_TYPE:
    print("\n" + "-" * 60)
    print("[INFO] Parameters are required for this notebook to run.")
    print("[INFO] No processing will be performed.")
    print(f"[INFO] Received: practice={PRACTICE_NAME or 'NONE'}, file_type={FILE_TYPE or 'NONE'}")
    print("-" * 60)
    print("\n[DONE] Run ended gracefully due to missing required parameters.")
else:
    # ==========================================================
    # 3) Load config from JSON
    # ==========================================================
    CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
    CONFIG_FILENAME = "practice_ingest_config_v14.json"

    session.sql("""
        CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG
        TYPE = JSON
    """).collect()

    cfg_sql = f"""
        SELECT
          LOWER(p.value:practice_name::string)                         AS practice_name,
          f.value:file_type::string                                    AS file_type,
          COALESCE(f.value:source_type::string, 'file')                AS source_type,
          f.value:source:api_url::string                               AS api_url,
          f.value:source:method::string                                AS api_method,
          f.value:source:queries:api_key::string                       AS api_key,
          f.value:source:refresh:api_key::string                       AS refresh_api_key,
          f.value:source:rows_path::string                             AS rows_path,
          f.value:source:columns_path::string                          AS columns_path,
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
    print(f"[INFO] Target table: {config['TARGET_DB']}.{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}")

    # ==========================================================
    # 4) Helper functions
    # ==========================================================
    def generate_guid():
        return str(uuid.uuid4())

    def log_ingest(practice_name, file_type, source_type, file_name, target_table, 
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
                    '{source_type}', '{file_name}', '{target_table}', '{step_name}',
                    {row_count}, '{status}', {error_value}, '{start_ts}', '{end_ts}', {duration_seconds}
                )
            """
            session.sql(insert_sql).collect()
            print(f"[LOG] {step_name}: {practice_name}/{file_type} - {status} ({row_count} rows, {duration_seconds}s)")
        except Exception as e:
            print(f"[WARN] Failed to write to log table: {e}")

    def get_nested_value(data, path):
        """Navigate nested dict using dot-notation path like 'query_result.data.rows'"""
        keys = path.split('.')
        for key in keys:
            data = data[key]
        return data

    def extract_query_id(api_url):
        """
        Extract query_id from API URL.
        Example: https://dbx.healthjump.com/api/queries/3977/results.json -> 3977
        """
        match = re.search(r'/api/queries/(\d+)/', api_url)
        if match:
            return match.group(1)
        return None

    def build_refresh_url(api_url, query_id):
        """
        Build the refresh URL from the results URL.
        Example: https://dbx.healthjump.com/api/queries/3977/results.json
              -> https://dbx.healthjump.com/api/queries/3977/refresh
        """
        # Extract base URL up to /api/queries/
        match = re.match(r'(https?://[^/]+)/api/queries/\d+/', api_url)
        if match:
            base_url = match.group(1)
            return f"{base_url}/api/queries/{query_id}/refresh"
        return None

    # ==========================================================
    # 5) Extract API configuration
    # ==========================================================
    api_url = config['API_URL']
    api_key = config['API_KEY']
    refresh_api_key = config['REFRESH_API_KEY']
    rows_path = config['ROWS_PATH']
    load_mode = config['LOAD_MODE'] or 'append'
    fq_table = f"{config['TARGET_DB']}.{config['TARGET_SCHEMA']}.{config['TARGET_TABLE']}"

    # Validate refresh API key
    if not refresh_api_key:
        raise ValueError("Missing 'refresh.api_key' in config. Required for API refresh endpoint.")

    # Extract query_id and build refresh URL
    query_id = extract_query_id(api_url)
    if not query_id:
        raise ValueError(f"Could not extract query_id from API URL: {api_url}")
    
    refresh_url = build_refresh_url(api_url, query_id)
    if not refresh_url:
        raise ValueError(f"Could not build refresh URL from API URL: {api_url}")

    print(f"[INFO] API URL: {api_url}")
    print(f"[INFO] Query ID: {query_id}")
    print(f"[INFO] Refresh URL: {refresh_url}")
    print(f"[INFO] Refresh API Key: {refresh_api_key[:10]}...{refresh_api_key[-4:]}" if refresh_api_key and len(refresh_api_key) > 14 else "[INFO] Refresh API Key: configured")

    # ==========================================================
    # 6) Refresh the API query
    # ==========================================================
    refresh_start_time = time.time()

    try:
        print(f"\n[INFO] Sending POST request to refresh query {query_id}...")
        
        # Use refresh API key as query parameter
        refresh_response = requests.post(
            refresh_url, 
            params={"api_key": refresh_api_key},
            timeout=60
        )
        
        if refresh_response.status_code not in (200, 201, 202):
            raise Exception(f"Refresh API returned status code {refresh_response.status_code}: {refresh_response.text[:500]}")
        
        refresh_end_time = time.time()
        print(f"[INFO] Refresh request successful (status={refresh_response.status_code})")
        
        # Log the refresh response if available
        try:
            refresh_json = refresh_response.json()
            print(f"[INFO] Refresh response: {json.dumps(refresh_json)[:500]}")
        except:
            print(f"[INFO] Refresh response: {refresh_response.text[:500]}")
        
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", refresh_url, fq_table, 
                   "API_REFRESH", 0, "SUCCESS", refresh_start_time, refresh_end_time)
        
        # Wait for refresh to complete
        print(f"\n[INFO] Waiting {REFRESH_WAIT_SECONDS} seconds for query refresh to complete...")
        time.sleep(REFRESH_WAIT_SECONDS)
        print(f"[INFO] Wait complete. Proceeding to fetch data...")
        
    except Exception as e:
        refresh_end_time = time.time()
        error_msg = str(e)[:500]
        print(f"[ERROR] API refresh failed: {error_msg}")
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", refresh_url, fq_table, 
                   "API_REFRESH", 0, "ERROR", refresh_start_time, refresh_end_time, error_msg)
        raise

    # ==========================================================
    # 7) Call API and load into DataFrame
    # ==========================================================
    print(f"\n[INFO] Calling API to fetch data: {api_url}")
    api_start_time = time.time()

    try:
        response = requests.get(api_url, params={"api_key": api_key}, timeout=120)
        api_end_time = time.time()
        
        if response.status_code != 200:
            raise Exception(f"API returned status code {response.status_code}: {response.text[:500]}")
        
        print(f"[INFO] API call successful (status={response.status_code})")
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", api_url, fq_table, 
                   "API_CALL", 0, "SUCCESS", api_start_time, api_end_time)
        
        # Parse JSON response
        response_json = response.json()
        
        # Extract rows using configured path (e.g., "query_result.data.rows")
        rows = get_nested_value(response_json, rows_path)
        
        print(f"[INFO] Extracted {len(rows)} rows from API response")
        
        # Convert to pandas DataFrame
        df_pandas = pd.DataFrame(rows)
        
        print(f"[INFO] DataFrame created with {len(df_pandas)} rows and {len(df_pandas.columns)} columns")
        print(f"[INFO] Columns: {df_pandas.columns.tolist()}")
        print("\n[INFO] Sample data (first 10 rows):")
        print(df_pandas.head(10).to_string())
        
    except Exception as e:
        api_end_time = time.time()
        error_msg = str(e)[:500]
        print(f"[ERROR] API call failed: {error_msg}")
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", api_url, fq_table, 
                   "API_CALL", 0, "ERROR", api_start_time, api_end_time, error_msg)
        raise

    # ==========================================================
    # 8) Convert to Snowpark DataFrame and add metadata
    # ==========================================================
    write_start_time = time.time()

    try:
        # Convert pandas DataFrame to Snowpark DataFrame
        df_snowpark = session.create_dataframe(df_pandas)
        
        # Add metadata columns
        df_with_meta = (
            df_snowpark
            .withColumn("FILE_NAME", lit(f"api_{PRACTICE_NAME}_{FILE_TYPE}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
            .withColumn("FILE_LOAD_TIME", current_timestamp())
        )
        
        src_count = df_with_meta.count()
        print(f"[INFO] Source row count: {src_count}")
        
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "api_response", fq_table, 
                   "READ_API", src_count, "SUCCESS", api_start_time)
        
        # ==========================================================
        # 9) Write to Snowflake table
        # ==========================================================
        print(f"\n[INFO] Writing to {fq_table} (mode={load_mode})...")
        
        # Check pre-load count
        try:
            before_cnt = session.sql(f"SELECT COUNT(*) AS C FROM {fq_table}").collect()[0]["C"]
            print(f"[INFO] Pre-load row count: {before_cnt}")
        except:
            print(f"[INFO] Target table {fq_table} does not exist yet; it will be created.")
            before_cnt = -1
        
        # Write data
        df_with_meta.write.mode(load_mode).save_as_table(fq_table)
        
        write_end_time = time.time()
        
        # Check post-load count
        after_cnt = session.sql(f"SELECT COUNT(*) AS C FROM {fq_table}").collect()[0]["C"]
        print(f"[SUCCESS] Wrote {src_count} row(s) to {fq_table}")
        print(f"[INFO] Post-load row count: {after_cnt}")
        
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "api_response", fq_table, 
                   "WRITE_TABLE", src_count, "SUCCESS", write_start_time, write_end_time)
        
        # Preview data in table
        print("\n[INFO] Preview of data in target table:")
        session.table(fq_table).show(10)
        
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "api_response", fq_table, 
                   "PROCESS_COMPLETE", src_count, "SUCCESS", refresh_start_time)
        
    except Exception as e:
        write_end_time = time.time()
        error_msg = str(e)[:500]
        print(f"[ERROR] Write failed: {error_msg}")
        log_ingest(PRACTICE_NAME, FILE_TYPE, "api", "api_response", fq_table, 
                   "PROCESS_COMPLETE", 0, "ERROR", refresh_start_time, write_end_time, error_msg)
        raise

    # ==========================================================
    # 10) Summary
    # ==========================================================
    etl_end_time = time.time()
    etl_duration = round(etl_end_time - refresh_start_time, 3)

    print("\n" + "-" * 60)
    print("[SUMMARY]")
    print(f"  Parent Run ID     : {PARENT_RUN_ID}")
    print(f"  Practice          : {PRACTICE_NAME}")
    print(f"  File Type         : {FILE_TYPE}")
    print(f"  Query ID          : {query_id}")
    print(f"  Target Table      : {fq_table}")
    print(f"  Rows Loaded       : {src_count}")
    print(f"  Refresh Wait      : {REFRESH_WAIT_SECONDS} seconds")
    print(f"  Total Duration    : {etl_duration} seconds")
    print("-" * 60)
    print("\n[DONE] API ingest completed.")
