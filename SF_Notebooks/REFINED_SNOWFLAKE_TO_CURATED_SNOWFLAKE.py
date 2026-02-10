# REFINED_SNOWFLAKE_TO_CURATED_SNOWFLAKE.py 
# ==========================================================
# Curated data loading script with Dataverse sync
#
# Features:
# - Writes to curated Snowflake table (receipt/audit)
# - Syncs to Dataverse using $batch API (up to 1000 ops per request)
# - Patient Demographics: Uses PATCH with alternate key (cr063_medicarenum) - TRUE UPSERT
# - Appointments: Uses PATCH by GUID for updates, POST for creates
# - OFFICE MAPPING: Resolves location names to assigned office names via configurable lookup
# - SOURCE FILTER: Config-driven row filtering (only Eligible='Y' records)
# - DUMMY MBI: Records without MBI use NOMBI_{PatientNumber} format
# - TOKEN REFRESH: Refreshes token before each major sync phase
# - TRANSFORM MAPPING: Supports value transformation in field mappings
# - Handles NEW (create) vs UPDATE operations
# - Sets delete flag for canceled appointments
# - Comprehensive logging for all Dataverse operations
# - Error handling with Teams notifications
# ==========================================================

import json
import sys
import re
import uuid
import time
import requests
from datetime import datetime
from snowflake.snowpark.context import get_active_session

# ==========================================================
# Secrets function (Snowflake Secret objects via Notebook st.secrets)
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
# 0) Session & config
# ==========================================================
session = get_active_session()
print(session)

_FALLBACK_RUN_ID = str(uuid.uuid4())

CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
CONFIG_FILENAME = "practice_ingest_config_v17.json"

CURATED_LOG_TABLE = "UPPERLINE_CURATED.AFFILIATEDATA.CURATED_INGEST_LOG"

# ==========================================================
# DATAVERSE DRY RUN MODE
# ==========================================================
DATAVERSE_DRY_RUN = False

# ==========================================================
# DATAVERSE TARGET ENVIRONMENT
# ==========================================================
DATAVERSE_TARGET = "sandbox"

if DATAVERSE_DRY_RUN:
    print("=" * 70)
    print("*** DATAVERSE_DRY_RUN = True ***")
    print("*** Dataverse API calls will be SKIPPED (dev/test mode) ***")
    print("*** Change to False for production ***")
    print("=" * 70)

print(f"[CONFIG] DATAVERSE_TARGET = {DATAVERSE_TARGET}")

TEAMS_WEBHOOK_URL = "https://prod-28.northcentralus.logic.azure.com:443/workflows/5b7115640e194503a3eacf6fa004874b/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=Xe2zUAhAvu8zbw0EQU2KFZyH6NAaJFo32unA75A0u_E"

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
    refined_run_id = None

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
        return practice_name, file_type, refined_run_id

    pipe_arg = next((a for a in argv if "|" in a), None)
    if pipe_arg is not None:
        parts = pipe_arg.split("|")
        if len(parts) >= 1:
            practice_name = _strip_wrapping_quotes(parts[0].strip()) or None
        if len(parts) >= 2:
            file_type = _strip_wrapping_quotes(parts[1].strip()) or None
        if len(parts) >= 3:
            refined_run_id = _strip_wrapping_quotes(parts[2].strip()) or None
        return practice_name, file_type, refined_run_id

    if len(argv) >= 1:
        practice_name = _strip_wrapping_quotes(argv[0].strip()) or None
    if len(argv) >= 2:
        file_type = _strip_wrapping_quotes(argv[1].strip()) or None
    if len(argv) >= 3:
        refined_run_id = _strip_wrapping_quotes(argv[2].strip()) or None

    return practice_name, file_type, refined_run_id

PRACTICE_NAME, FILE_TYPE, REFINED_RUN_ID = parse_parameters()

if REFINED_RUN_ID:
    PARENT_RUN_ID = REFINED_RUN_ID
    print(f"[INFO] Using upstream PARENT_RUN_ID for end-to-end tracking: {PARENT_RUN_ID}")
else:
    PARENT_RUN_ID = _FALLBACK_RUN_ID
    print(f"[INFO] Generated new PARENT_RUN_ID (standalone run): {PARENT_RUN_ID}")

DEFAULT_PRACTICE_FOR_TESTING = None
DEFAULT_FILE_TYPE_FOR_TESTING = None

if not PRACTICE_NAME and DEFAULT_PRACTICE_FOR_TESTING:
    PRACTICE_NAME = DEFAULT_PRACTICE_FOR_TESTING
    print(f"[INFO] Using default practice for testing: {PRACTICE_NAME}")

if not FILE_TYPE and DEFAULT_FILE_TYPE_FOR_TESTING:
    FILE_TYPE = DEFAULT_FILE_TYPE_FOR_TESTING
    print(f"[INFO] Using default file_type for testing: {FILE_TYPE}")

if not PRACTICE_NAME or not FILE_TYPE:
    raise ValueError("Missing required parameters: practice and file_type")

print(f"\n[INFO] ========== CURATED ETL RUN STARTED ==========")
print(f"[INFO] Parent Run ID: {PARENT_RUN_ID}")
print(f"[INFO] Practice: {PRACTICE_NAME}")
print(f"[INFO] File Type: {FILE_TYPE}")

# ==========================================================
# 2) LOGGING FUNCTION
# ==========================================================
def log_curated(practice_name: str, source_table: str, target_table: str,
                step_name: str, row_count: int, status: str,
                start_time: float, end_time: float = None, error_message: str = None,
                force_commit: bool = False):
    try:
        log_id = str(uuid.uuid4())
        if end_time is None:
            end_time = time.time()

        duration_seconds = round(end_time - start_time, 3)
        start_ts = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        end_ts = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        practice_name_safe = (practice_name or "").replace("'", "''")
        source_table_safe = (source_table or "").replace("'", "''")
        target_table_safe = (target_table or "").replace("'", "''")
        step_name_safe = (step_name or "").replace("'", "''")
        error_message_safe = (error_message or "").replace("'", "''") if error_message else None

        error_value = f"'{error_message_safe}'" if error_message_safe else "NULL"
        row_count_value = row_count if row_count is not None else "NULL"

        insert_sql = f"""
            INSERT INTO {CURATED_LOG_TABLE}
            (LOG_ID, PARENT_RUN_ID, PRACTICE_NAME, SOURCE_TABLE, TARGET_TABLE,
             STEP_NAME, ROW_COUNT, STATUS, ERROR_MESSAGE, START_TIME, END_TIME, DURATION_SECONDS)
            VALUES (
                '{log_id}', '{PARENT_RUN_ID}', '{practice_name_safe}', '{source_table_safe}',
                '{target_table_safe}', '{step_name_safe}', {row_count_value}, '{status}',
                {error_value}, '{start_ts}', '{end_ts}', {duration_seconds}
            )
        """
        session.sql(insert_sql).collect()
        print(f"[LOG] {step_name}: {practice_name} - {status} ({row_count} rows, {duration_seconds}s)")
        return log_id
    except Exception as e:
        print(f"[WARN] Failed to write to log table for {step_name}: {e}")
        return None

def log_dataverse(step_name: str, row_count: int, status: str, 
                  start_time: float, end_time: float = None, 
                  error_message: str = None, target_entity: str = "DATAVERSE"):
    try:
        log_id = str(uuid.uuid4())
        if end_time is None:
            end_time = time.time()

        duration_seconds = round(end_time - start_time, 3)
        start_ts = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        end_ts = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        practice_name_safe = (PRACTICE_NAME or "").replace("'", "''")
        step_name_safe = (step_name or "").replace("'", "''")
        target_entity_safe = (target_entity or "").replace("'", "''")
        error_message_safe = (error_message or "").replace("'", "''")[:500] if error_message else None

        error_value = f"'{error_message_safe}'" if error_message_safe else "NULL"
        row_count_value = row_count if row_count is not None else 0

        insert_sql = f"""
            INSERT INTO {CURATED_LOG_TABLE}
            (LOG_ID, PARENT_RUN_ID, PRACTICE_NAME, SOURCE_TABLE, TARGET_TABLE,
             STEP_NAME, ROW_COUNT, STATUS, ERROR_MESSAGE, START_TIME, END_TIME, DURATION_SECONDS)
            VALUES (
                '{log_id}', 
                '{PARENT_RUN_ID}', 
                '{practice_name_safe}',
                'DATAVERSE_SYNC',
                '{target_entity_safe}', 
                '{step_name_safe}', 
                {row_count_value}, 
                '{status}',
                {error_value}, 
                '{start_ts}', 
                '{end_ts}', 
                {duration_seconds}
            )
        """
        
        result = session.sql(insert_sql).collect()
        print(f"[LOG-DV] {step_name}: {status} ({row_count} rows, {duration_seconds}s)")
        return log_id
        
    except Exception as e:
        print(f"[ERROR] Failed to write Dataverse log for {step_name}: {e}")
        print(f"[ERROR] SQL attempted: {insert_sql[:500]}...")
        return None

def send_teams_notification(title: str, message: str, is_error: bool = True):
    try:
        color = "ff0000" if is_error else "00ff00"
        payload = {
            "title": title,
            "text": message,
            "themeColor": color
        }
        response = requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"[INFO] Teams notification sent: {title}")
        else:
            print(f"[WARN] Teams notification failed: {response.status_code}")
    except Exception as e:
        print(f"[WARN] Failed to send Teams notification: {e}")

# ==========================================================
# 3) Functions
# ==========================================================
def _is_configured_value(v) -> bool:
    if v is None:
        return False
    s = str(v).strip()
    if not s:
        return False
    if s.lower() in ("none", "null", "n/a", "na"):
        return False
    return True

def ensure_list_of_dicts(raw_value):
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return raw_value
    if isinstance(raw_value, dict):
        return [raw_value]
    try:
        parsed = json.loads(str(raw_value))
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except Exception:
        pass
    return []

def _norm_name(name: str) -> str:
    if name is None:
        return ""
    return "".join(ch for ch in str(name) if ch.isalnum()).lower()

def _unquote_ident(ident: str) -> str:
    if ident is None:
        return ""
    s = str(ident).strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].replace('""', '"')
    return s

def _quote_ident(ident: str) -> str:
    if ident is None:
        return '""'
    s = _unquote_ident(str(ident))
    return '"' + s.replace('"', '""') + '"'

def _clean_error_message(msg: str) -> str:
    if not msg:
        return msg
    pattern = r"^\(\d+\):\s*[a-f0-9-]+:\s*\d+\s*\([A-Z0-9]+\):\s*"
    return re.sub(pattern, "", msg, flags=re.IGNORECASE)

def build_sql_column_expr(col_config, source_cols_map: dict) -> str:
    if isinstance(col_config, str):
        actual_col = source_cols_map.get(col_config.upper(), col_config)
        return _quote_ident(actual_col)
    elif isinstance(col_config, dict) and "concat" in col_config:
        columns = col_config["concat"]
        separator = col_config.get("separator", " ")
        parts = []
        for col in columns:
            actual_col = source_cols_map.get(col.upper(), col)
            parts.append(_quote_ident(actual_col))
        if len(parts) == 1:
            return parts[0]
        concat_parts = []
        for i, part in enumerate(parts):
            concat_parts.append(part)
            if i < len(parts) - 1:
                concat_parts.append(f"'{separator}'")
        return f"CONCAT({', '.join(concat_parts)})"
    else:
        return _quote_ident(str(col_config))

def build_lookup_key_value(col_config, row: dict, source_cols_map: dict) -> str:
    if isinstance(col_config, str):
        actual_col = source_cols_map.get(col_config.upper(), col_config)
        return str(row.get(actual_col, "") or "").strip()
    elif isinstance(col_config, dict) and "concat" in col_config:
        columns = col_config["concat"]
        separator = col_config.get("separator", " ")
        values = []
        for col in columns:
            actual_col = source_cols_map.get(col.upper(), col)
            val = str(row.get(actual_col, "") or "").strip()
            values.append(val)
        return separator.join(values)
    else:
        return str(col_config)

# ==========================================================
# 4) DATAVERSE HELPER FUNCTIONS
# ==========================================================
def get_dataverse_credentials(secret_alias: str) -> dict:
    try:
        secret_string = get_generic_string_secret(secret_alias)
        credentials = json.loads(secret_string)
        
        required_fields = ["tenant_id", "client_id", "client_secret"]
        missing = [f for f in required_fields if f not in credentials]
        if missing:
            raise ValueError(f"Secret '{secret_alias}' is missing required fields: {missing}")
        
        return credentials
    except Exception as e:
        print(f"[ERROR] Failed to retrieve Dataverse credentials from secret '{secret_alias}': {e}")
        raise

def get_dataverse_token(credentials: dict, environment_url: str) -> str:
    tenant_id = credentials["tenant_id"]
    client_id = credentials["client_id"]
    client_secret = credentials["client_secret"]
    
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": f"{environment_url}/.default"
    }
    
    response = requests.post(token_url, data=token_data, timeout=30)
    response.raise_for_status()
    
    token_json = response.json()
    return token_json["access_token"]

def build_field_value(field_config, row: dict, source_cols_map: dict) -> str:
    """
    Build field value from config - handles simple mapping, concat, coalesce, prefix, suffix, and transform
    """
    if isinstance(field_config, str):
        col_name = field_config.upper()
        actual_col = source_cols_map.get(col_name, col_name)
        return str(row.get(actual_col, "") or "")
    
    elif isinstance(field_config, dict):
        if "concat" in field_config:
            parts = []
            separator = field_config.get("separator", " ")
            for col in field_config["concat"]:
                col_name = col.upper()
                actual_col = source_cols_map.get(col_name, col_name)
                val = str(row.get(actual_col, "") or "")
                parts.append(val)
            return separator.join(parts).strip()
        
        elif "coalesce" in field_config:
            for col in field_config["coalesce"]:
                col_name = col.upper()
                actual_col = source_cols_map.get(col_name, col_name)
                val = row.get(actual_col)
                if val:
                    return str(val)
            return ""
        
        elif "source" in field_config:
            col_name = field_config["source"].upper()
            actual_col = source_cols_map.get(col_name, col_name)
            val = str(row.get(actual_col, "") or "")
            
            if "transform" in field_config and val:
                transform_map = field_config["transform"]
                if val in transform_map:
                    val = transform_map[val]
                elif val.lower() in transform_map:
                    val = transform_map[val.lower()]
                else:
                    val_lower = val.lower()
                    for key, mapped_val in transform_map.items():
                        if key.lower() == val_lower:
                            val = mapped_val
                            break
            
            prefix = field_config.get("prefix", "")
            suffix = field_config.get("suffix", "")
            if val:
                return prefix + val + suffix
            return val
    
    return ""

def dataverse_batch_request(
    environment_url: str,
    access_token: str,
    requests_list: list,
    batch_size: int = 1000,
    entity_name: str = "unknown"
) -> dict:
    """
    Send batch requests to Dataverse WITHOUT changesets.

    
    Each operation is independent within the batch:
    - If one operation fails, others still succeed
    - No transactional rollback
    - "Entity Does Not Exist" errors only affect the specific record, not the whole batch
    """
    results = {
        "total": len(requests_list),
        "success": 0, 
        "failed": 0, 
        "creates": 0,
        "updates": 0,
        "errors": [],
        "batches_sent": 0,
        "batches_succeeded": 0,
        "batches_failed": 0,
        "dry_run": DATAVERSE_DRY_RUN
    }
    
    if not requests_list:
        print(f"[INFO] No requests to send for {entity_name}")
        return results
    
    results["creates"] = sum(1 for r in requests_list if r.get("method", "POST") == "POST")
    results["updates"] = sum(1 for r in requests_list if r.get("method") == "PATCH")
    
    print(f"[BATCH-API] {entity_name}: Processing {results['total']} requests ({results['creates']} CREATE, {results['updates']} UPDATE)")
    print(f"[BATCH-API] {entity_name}: Using $batch API (V8 - no changesets) with batch_size={batch_size}")
    
    if DATAVERSE_DRY_RUN:
        print(f"[DRY RUN] {entity_name}: *** DRY RUN MODE - Skipping actual Dataverse API calls ***")
        print(f"[DRY RUN] {entity_name}: Would have sent {results['total']} requests in {(results['total'] + batch_size - 1) // batch_size} batches")
        results["success"] = results["total"]
        results["failed"] = 0
        results["batches_sent"] = (results['total'] + batch_size - 1) // batch_size
        results["batches_succeeded"] = results["batches_sent"]
        results["errors"].append("DRY RUN - No actual API calls made")
        print(f"[DRY RUN] {entity_name}: Simulated success for all {results['total']} records")
        return results
    
    batch_url = f"{environment_url}/api/data/v9.2/$batch"
    start_time = time.time()
    
    log_dataverse(f"{entity_name} Batch Processing Started", results['total'], "IN_PROGRESS", start_time,
                  error_message=f"Creates:{results['creates']}, Updates:{results['updates']}, BatchSize:{batch_size}")
    
    total_batches = (len(requests_list) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        batch_start_idx = batch_num * batch_size
        batch_end_idx = min(batch_start_idx + batch_size, len(requests_list))
        batch_requests = requests_list[batch_start_idx:batch_end_idx]
        
        batch_id = f"{entity_name.lower()}_{batch_num}_{int(time.time())}"
        results["batches_sent"] += 1
        
        batch_boundary = f"batch_{batch_id}"
        
        lines = []
        
        for op_idx, req in enumerate(batch_requests):
            method = req.get("method", "POST")
            entity = req["entity"]
            data = req["data"]
            record_id = req.get("record_id")
            alternate_key = req.get("alternate_key")
            
            # Start of individual request (no changeset boundary)
            lines.append(f"--{batch_boundary}")
            lines.append("Content-Type: application/http")
            lines.append("Content-Transfer-Encoding: binary")
            lines.append(f"Content-ID: {op_idx + 1}")
            lines.append("")
            
            # HTTP request line and headers
            if alternate_key:
                lines.append(f"PATCH {entity}({alternate_key}) HTTP/1.1")
                lines.append("Content-Type: application/json; charset=utf-8")
            elif method == "PATCH" and record_id:
                lines.append(f"PATCH {entity}({record_id}) HTTP/1.1")
                lines.append("Content-Type: application/json; charset=utf-8")
                lines.append("If-Match: *")
            else:
                lines.append(f"POST {entity} HTTP/1.1")
                lines.append("Content-Type: application/json; charset=utf-8")
            
            # Empty line before body
            lines.append("")
            # JSON body
            lines.append(json.dumps(data))
        
        # End of batch (no changeset end boundary)
        lines.append(f"--{batch_boundary}--")
        
        body = "\r\n".join(lines)
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/mixed; boundary={batch_boundary}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Accept": "application/json",
            "Prefer": "odata.continue-on-error"
        }
        
        try:
            resp = requests.post(batch_url, headers=headers, data=body, timeout=300)
            
            if resp.status_code in (200, 202):
                response_text = resp.text
                success_count = (
                    response_text.count("HTTP/1.1 204") + 
                    response_text.count("HTTP/1.1 201") + 
                    response_text.count("HTTP/1.1 200")
                )
                
                error_codes = re.findall(r'HTTP/1\.1 ([45]\d\d)', response_text)
                fail_count = len(error_codes)
                
                if success_count + fail_count != len(batch_requests):
                    success_count = len(batch_requests) - fail_count
                
                results["success"] += success_count
                results["failed"] += fail_count
                
                if fail_count == 0:
                    results["batches_succeeded"] += 1
                else:
                    results["batches_failed"] += 1
                    if fail_count > 0 and len(results["errors"]) < 5:
                        error_msgs = re.findall(r'"message"\s*:\s*"([^"]{1,200})"', response_text)
                        for em in error_msgs[:2]:
                            results["errors"].append(f"Batch {batch_num + 1}: {em}")
            else:
                results["failed"] += len(batch_requests)
                results["batches_failed"] += 1
                try:
                    err_msg = resp.json().get("error", {}).get("message", resp.text[:300])
                except:
                    err_msg = resp.text[:300]
                results["errors"].append(f"Batch {batch_num + 1} failed: HTTP {resp.status_code} - {err_msg}")
                
        except requests.exceptions.Timeout:
            results["failed"] += len(batch_requests)
            results["batches_failed"] += 1
            results["errors"].append(f"Batch {batch_num + 1} timed out (300s)")
            
        except Exception as e:
            results["failed"] += len(batch_requests)
            results["batches_failed"] += 1
            results["errors"].append(f"Batch {batch_num + 1} error: {str(e)[:150]}")
        
        elapsed = round(time.time() - start_time, 1)
        rate = round(batch_end_idx / elapsed, 1) if elapsed > 0 else 0
        print(f"[BATCH-API] {entity_name}: Batch {batch_num + 1}/{total_batches} - {results['success']}/{batch_end_idx} ok ({rate} rec/sec)")
        
        if (batch_num + 1) % 10 == 0:
            log_dataverse(f"{entity_name} Progress", batch_end_idx, "IN_PROGRESS", start_time,
                          error_message=f"Batches:{batch_num + 1}/{total_batches}, Success:{results['success']}, Failed:{results['failed']}")
    
    duration = round(time.time() - start_time, 2)
    
    print(f"[BATCH-API] {entity_name}: Completed in {duration}s")
    print(f"[BATCH-API] {entity_name}: {results['success']} success, {results['failed']} failed")
    print(f"[BATCH-API] {entity_name}: Batches: {results['batches_sent']} sent, {results['batches_succeeded']} ok, {results['batches_failed']} fail")
    
    return results

# ==========================================================
# 5) DATAVERSE SYNC FUNCTION
# ==========================================================
def sync_to_dataverse(
    dataverse_config: dict,
    source_data: list,
    source_cols_map: dict,
    patient_lookup_results: dict,
    appt_lookup_results: dict
) -> dict:
    results = {
        "patient_demo": {
            "total": 0, "success": 0, "failed": 0, 
            "creates": 0, "updates": 0, "skipped": 0,
            "batches_sent": 0, "batches_succeeded": 0, "batches_failed": 0,
            "errors": []
        },
        "appointment": {
            "total": 0, "success": 0, "failed": 0,
            "creates": 0, "updates": 0, "skipped": 0, "canceled": 0,
            "batches_sent": 0, "batches_succeeded": 0, "batches_failed": 0,
            "errors": []
        }
    }
    
    if not dataverse_config.get("enabled", False):
        print("[INFO] Dataverse sync is disabled in config")
        log_curated(PRACTICE_NAME, "REFINED", "DATAVERSE", "Dataverse Sync Disabled",
                    0, "SKIPPED", time.time(), error_message="Dataverse sync disabled in config")
        return results
    
    secret_name = dataverse_config.get("secret_name")
    batch_size = dataverse_config.get("batch_size", 1000)
    patient_config = dataverse_config.get("patient_demographics", {})
    appt_config = dataverse_config.get("appointment_data", {})
    
    environments = dataverse_config.get("environments", {})
    environment_url = environments.get(DATAVERSE_TARGET)
    
    if not environment_url:
        environment_url = dataverse_config.get("environment_url")
    
    if not environment_url:
        error_msg = f"No environment URL found for target '{DATAVERSE_TARGET}'. Check dataverse_sync.environments in config."
        print(f"[ERROR] {error_msg}")
        log_curated(PRACTICE_NAME, "REFINED", "DATAVERSE", "Dataverse Config Error",
                    0, "ERROR", time.time(), error_message=error_msg)
        results["patient_demo"]["errors"].append(error_msg)
        results["appointment"]["errors"].append(error_msg)
        return results
    
    print("\n" + "=" * 70)
    print("[DATAVERSE] ========== DATAVERSE SYNC STARTED (V8) ==========")
    print("[DATAVERSE] V8 Fix: No changesets - each operation independent")
    print("[DATAVERSE] V7 Features: Dummy MBI support, transform mapping, token refresh")
    print(f"[DATAVERSE] Target Environment: {DATAVERSE_TARGET}")
    if DATAVERSE_DRY_RUN:
        print("[DATAVERSE] *** DRY RUN MODE ENABLED ***")
    print("=" * 70)
    
    sync_start_time = time.time()
    auth_start_time = time.time()
    
    log_dataverse("Dataverse Sync Started", len(source_data), "IN_PROGRESS", sync_start_time, 
                  target_entity="DATAVERSE")
    
    try:
        print(f"\n[DATAVERSE] Step 1: Authenticating to Dataverse...")
        print(f"[DATAVERSE]   Secret: {secret_name}")
        print(f"[DATAVERSE]   Environment: {environment_url} ({DATAVERSE_TARGET})")
        
        credentials = get_dataverse_credentials(secret_name)
        
        access_token = get_dataverse_token(credentials, environment_url)
        auth_duration = round(time.time() - auth_start_time, 2)
        
        print(f"[DATAVERSE]   ✓ Authentication successful ({auth_duration}s)")
        log_dataverse("Dataverse Authentication", 1, "SUCCESS", auth_start_time,
                      target_entity=environment_url)
        
    except Exception as e:
        auth_duration = round(time.time() - auth_start_time, 2)
        error_msg = f"Authentication failed: {str(e)[:300]}"
        print(f"[DATAVERSE]   ✗ {error_msg}")
        
        results["patient_demo"]["errors"].append(error_msg)
        results["appointment"]["errors"].append(error_msg)
        
        log_dataverse("Dataverse Authentication", 0, "ERROR", auth_start_time,
                      error_message=error_msg, target_entity="DATAVERSE")
        
        send_teams_notification(
            f"Dataverse Sync Failed - {PRACTICE_NAME}/{FILE_TYPE}",
            f"Authentication failed after {auth_duration}s: {error_msg}"
        )
        return results
    
    # STEP 2: Patient Demographics Sync
    if patient_config:
        print(f"\n[DATAVERSE] Step 2: Patient Demographics Sync")
        print(f"[DATAVERSE]   " + "-" * 50)
        
        patient_start_time = time.time()
        patient_entity = patient_config.get("entity", "cr063_spotpatientdemographicses")
        patient_field_mapping = patient_config.get("field_mapping", {})
        patient_source_key = patient_config.get("source_key", "PRIMARYINSURANCESUBSCRIBERID").upper()
        
        print(f"[DATAVERSE]   Entity: {patient_entity}")
        print(f"[DATAVERSE]   Source Key: {patient_source_key}")
        print(f"[DATAVERSE]   Field Mappings: {len(patient_field_mapping)}")
        
        patient_requests = []
        skipped_no_mbi = 0
        dummy_mbi_count = 0
        create_count = 0
        update_count = 0
        
        for row in source_data:
            mbi_col = source_cols_map.get(patient_source_key, patient_source_key)
            mbi = str(row.get(mbi_col, "") or "").strip().upper()
            
            if not mbi:
                patient_number_col = source_cols_map.get("PATIENTNUMBER", "PATIENTNUMBER")
                patient_number = str(row.get(patient_number_col, "") or "").strip()
                if patient_number:
                    mbi = f"NOMBI_{patient_number}"
                    dummy_mbi_count += 1
                else:
                    skipped_no_mbi += 1
                    continue
            
            data = {}
            for dv_field, source_field in patient_field_mapping.items():
                value = build_field_value(source_field, row, source_cols_map)
                if value:
                    data[dv_field] = value
            
            record_id = patient_lookup_results.get(mbi)
            is_update = record_id is not None
            
            if is_update:
                update_count += 1
            else:
                create_count += 1
            
            patient_requests.append({
                "method": "PATCH",
                "entity": patient_entity,
                "data": data,
                "alternate_key": f"cr063_medicarenum='{mbi}'",
                "is_update": is_update
            })
        
        results["patient_demo"]["skipped"] = skipped_no_mbi
        results["patient_demo"]["dummy_mbi"] = dummy_mbi_count
        
        print(f"[DATAVERSE]   Records to process: {len(patient_requests)}")
        print(f"[DATAVERSE]     - NEW (CREATE): {create_count}")
        print(f"[DATAVERSE]     - EXISTING (UPDATE): {update_count}")
        print(f"[DATAVERSE]     - Using dummy MBI (NOMBI_*): {dummy_mbi_count}")
        print(f"[DATAVERSE]     - Skipped (no MBI or PatientNumber): {skipped_no_mbi}")
        
        # ========== DEBUG: Show sample payload ==========
        if patient_requests:
            sample_payload = patient_requests[0]["data"]
            print(f"\n[DEBUG] ========== SAMPLE PATIENT PAYLOAD ==========")
            print(f"[DEBUG] Alternate Key: {patient_requests[0]['alternate_key']}")
            print(f"[DEBUG] Payload fields ({len(sample_payload)} fields):")
            for k, v in list(sample_payload.items())[:10]:
                print(f"[DEBUG]   {k}: '{str(v)[:50]}'")
            if len(sample_payload) > 10:
                print(f"[DEBUG]   ... and {len(sample_payload) - 10} more fields")
            print(f"[DEBUG] ================================================\n")
        # ========== END DEBUG ==========
        
        if patient_requests:
            patient_result = dataverse_batch_request(
                environment_url, access_token, patient_requests, batch_size,
                entity_name="PatientDemographics"
            )
            
            results["patient_demo"].update(patient_result)
            results["patient_demo"]["creates"] = create_count
            results["patient_demo"]["updates"] = update_count
            
            patient_duration = round(time.time() - patient_start_time, 2)
            patient_end_time = time.time()
            
            if patient_result["failed"] == 0:
                status = "SUCCESS"
                status_icon = "✓"
            elif patient_result["success"] > 0:
                status = "PARTIAL"
                status_icon = "⚠"
            else:
                status = "ERROR"
                status_icon = "✗"
            
            print(f"\n[DATAVERSE]   {status_icon} Patient Demographics Complete ({patient_duration}s)")
            print(f"[DATAVERSE]     - Total Sent: {patient_result['total']}")
            print(f"[DATAVERSE]     - Success: {patient_result['success']}")
            print(f"[DATAVERSE]     - Failed: {patient_result['failed']}")
            
            patient_summary = f"Total:{patient_result['total']}, Creates:{create_count}, Updates:{update_count}, Failed:{patient_result['failed']}"
            if patient_result["errors"]:
                patient_summary += f" | Errors: {'; '.join(patient_result['errors'][:2])}"
            if DATAVERSE_DRY_RUN:
                patient_summary += " | DRY RUN"
            
            log_dataverse("Patient Demographics Sync", patient_result["total"], status, patient_start_time,
                          patient_end_time, target_entity=patient_entity,
                          error_message=patient_summary)
            
        else:
            print(f"[DATAVERSE]   ⚠ No patient demographic records to sync")
            log_dataverse("Patient Demographics Sync", 0, "SKIPPED", patient_start_time,
                          target_entity=patient_entity, error_message="No records with valid MBI")
    else:
        print(f"\n[DATAVERSE] Step 2: Patient Demographics - SKIPPED (not configured)")
    
    # STEP 3: Appointment Data Sync
    if appt_config:
        print(f"\n[DATAVERSE] Step 3: Appointment Data Sync")
        print(f"[DATAVERSE]   " + "-" * 50)
        
        print(f"[DATAVERSE]   Refreshing OAuth token before appointments...")
        try:
            access_token = get_dataverse_token(credentials, environment_url)
            print(f"[DATAVERSE]   ✓ Token refreshed successfully")
        except Exception as e:
            print(f"[DATAVERSE]   ✗ Token refresh failed: {str(e)[:100]}")
            print(f"[DATAVERSE]   Continuing with existing token...")
        
        appt_start_time = time.time()
        appt_entity = appt_config.get("entity", "crddb_vaaffliatedatas")
        appt_field_mapping = appt_config.get("field_mapping", {})
        canceled_check = appt_config.get("canceled_check", {})
        
        canceled_source_field = canceled_check.get("source_field", "APPOINTMENTSTATUS").upper()
        canceled_flag_field = canceled_check.get("flag_field", "crddb_deleteflagfromhj")
        canceled_flag_value = canceled_check.get("flag_value", "Y")
        
        canceled_contains = canceled_check.get("contains")
        canceled_values = canceled_check.get("values", [])
        
        lookup_keys = appt_config.get("lookup_keys", {})
        
        if isinstance(lookup_keys, list):
            lookup_key_configs = lookup_keys
        else:
            patient_id_cfg = lookup_keys.get("patient_id", {})
            appointment_date_cfg = lookup_keys.get("appointment_date", {})
            lookup_key_configs = [
                {"source": patient_id_cfg.get("source", "PATIENTNUMBER"), "lookup": patient_id_cfg.get("lookup")},
                {"source": appointment_date_cfg.get("source", "APPOINTMENTDATE"), "lookup": appointment_date_cfg.get("lookup")}
            ]
        
        print(f"[DATAVERSE]   Entity: {appt_entity}")
        print(f"[DATAVERSE]   Field Mappings: {len(appt_field_mapping)}")
        
        appt_requests = []
        skipped_no_key = 0
        appt_dummy_mbi_count = 0
        create_count = 0
        update_count = 0
        canceled_count = 0
        
        for row in source_data:
            key_values = []
            skip_row = False
            
            for key_cfg in lookup_key_configs:
                src_col_cfg = key_cfg.get("source")
                val = build_lookup_key_value(src_col_cfg, row, source_cols_map)
                if not val:
                    skip_row = True
                    break
                key_values.append(val)
            
            if skip_row:
                skipped_no_key += 1
                continue
            
            lookup_key = tuple(key_values)
            
            data = {}
            for dv_field, source_field in appt_field_mapping.items():
                value = build_field_value(source_field, row, source_cols_map)
                if value:
                    data[dv_field] = value
            
            if not data.get("crddb_medicarenum"):
                patient_number_col = source_cols_map.get("PATIENTNUMBER", "PATIENTNUMBER")
                patient_number = str(row.get(patient_number_col, "") or "").strip()
                if patient_number:
                    data["crddb_medicarenum"] = f"NOMBI_{patient_number}"
                    appt_dummy_mbi_count += 1
            
            status_col = source_cols_map.get(canceled_source_field, canceled_source_field)
            status_value = str(row.get(status_col, "") or "")
            
            is_canceled = False
            if canceled_values:
                is_canceled = status_value.upper() in [v.upper() for v in canceled_values]
            elif canceled_contains:
                is_canceled = canceled_contains.lower() in status_value.lower()
            
            if is_canceled:
                data[canceled_flag_field] = canceled_flag_value
                canceled_count += 1
            
            existing_record_id = appt_lookup_results.get(lookup_key)
            
            if existing_record_id:
                update_count += 1
                appt_requests.append({
                    "method": "PATCH",
                    "entity": appt_entity,
                    "data": data,
                    "record_id": existing_record_id
                })
            else:
                create_count += 1
                appt_requests.append({
                    "method": "POST",
                    "entity": appt_entity,
                    "data": data
                })
        
        results["appointment"]["skipped"] = skipped_no_key
        results["appointment"]["canceled"] = canceled_count
        results["appointment"]["dummy_mbi"] = appt_dummy_mbi_count
        
        print(f"[DATAVERSE]   Records to process: {len(appt_requests)}")
        print(f"[DATAVERSE]     - NEW (CREATE): {create_count}")
        print(f"[DATAVERSE]     - EXISTING (UPDATE): {update_count}")
        print(f"[DATAVERSE]     - Canceled (with delete flag): {canceled_count}")
        
        # ========== DEBUG: Show sample appointment payload ==========
        if appt_requests:
            sample_payload = appt_requests[0]["data"]
            print(f"\n[DEBUG] ========== SAMPLE APPOINTMENT PAYLOAD ==========")
            print(f"[DEBUG] Method: {appt_requests[0]['method']}")
            if 'record_id' in appt_requests[0]:
                print(f"[DEBUG] Record ID: {appt_requests[0]['record_id']}")
            print(f"[DEBUG] Payload fields ({len(sample_payload)} fields):")
            for k, v in list(sample_payload.items())[:15]:
                print(f"[DEBUG]   {k}: '{str(v)[:50]}'")
            if len(sample_payload) > 15:
                print(f"[DEBUG]   ... and {len(sample_payload) - 15} more fields")
            print(f"[DEBUG] ====================================================\n")
        # ========== END DEBUG ==========
        
        if appt_requests:
            appt_result = dataverse_batch_request(
                environment_url, access_token, appt_requests, batch_size,
                entity_name="Appointments"
            )
            
            results["appointment"].update(appt_result)
            results["appointment"]["creates"] = create_count
            results["appointment"]["updates"] = update_count
            
            appt_duration = round(time.time() - appt_start_time, 2)
            appt_end_time = time.time()
            
            if appt_result["failed"] == 0:
                status = "SUCCESS"
                status_icon = "✓"
            elif appt_result["success"] > 0:
                status = "PARTIAL"
                status_icon = "⚠"
            else:
                status = "ERROR"
                status_icon = "✗"
            
            print(f"\n[DATAVERSE]   {status_icon} Appointment Data Complete ({appt_duration}s)")
            print(f"[DATAVERSE]     - Total Sent: {appt_result['total']}")
            print(f"[DATAVERSE]     - Success: {appt_result['success']}")
            print(f"[DATAVERSE]     - Failed: {appt_result['failed']}")
            
            appt_summary = f"Total:{appt_result['total']}, Creates:{create_count}, Updates:{update_count}, Canceled:{canceled_count}, Failed:{appt_result['failed']}"
            if appt_result["errors"]:
                appt_summary += f" | Errors: {'; '.join(appt_result['errors'][:2])}"
            if DATAVERSE_DRY_RUN:
                appt_summary += " | DRY RUN"
            
            log_dataverse("Appointment Data Sync", appt_result["total"], status, appt_start_time,
                          appt_end_time, target_entity=appt_entity,
                          error_message=appt_summary)
            
        else:
            print(f"[DATAVERSE]   ⚠ No appointment records to sync")
            log_dataverse("Appointment Data Sync", 0, "SKIPPED", appt_start_time,
                          target_entity=appt_entity, error_message="No records with valid lookup keys")
    else:
        print(f"\n[DATAVERSE] Step 3: Appointment Data - SKIPPED (not configured)")
    
    # STEP 4: Summary
    sync_duration = round(time.time() - sync_start_time, 2)
    
    total_sent = results["patient_demo"]["total"] + results["appointment"]["total"]
    total_success = results["patient_demo"]["success"] + results["appointment"]["success"]
    total_failed = results["patient_demo"]["failed"] + results["appointment"]["failed"]
    total_creates = results["patient_demo"]["creates"] + results["appointment"]["creates"]
    total_updates = results["patient_demo"]["updates"] + results["appointment"]["updates"]
    
    print(f"\n[DATAVERSE] " + "=" * 60)
    print(f"[DATAVERSE] DATAVERSE SYNC SUMMARY")
    print(f"[DATAVERSE] " + "=" * 60)
    print(f"[DATAVERSE]   Total Duration: {sync_duration}s")
    print(f"[DATAVERSE]   Records Sent: {total_sent}")
    print(f"[DATAVERSE]   Records Success: {total_success}")
    print(f"[DATAVERSE]   Records Failed: {total_failed}")
    print(f"[DATAVERSE] " + "=" * 60)
    
    if total_failed == 0 and total_success > 0:
        overall_status = "SUCCESS"
    elif total_success > 0:
        overall_status = "PARTIAL"
    elif total_sent == 0:
        overall_status = "SKIPPED"
    else:
        overall_status = "ERROR"
    
    log_curated(PRACTICE_NAME, "REFINED", "DATAVERSE", "Dataverse Sync Complete",
                total_success, overall_status, sync_start_time,
                error_message=f"Total: {total_sent}, Success: {total_success}, Failed: {total_failed}, Creates: {total_creates}, Updates: {total_updates}")
    
    if total_failed > 0:
        all_errors = results["patient_demo"]["errors"] + results["appointment"]["errors"]
        error_summary = "; ".join(all_errors[:3]) if all_errors else "See logs for details"
        send_teams_notification(
            f"Dataverse Sync Partial Failure - {PRACTICE_NAME}/{FILE_TYPE}",
            f"Duration: {sync_duration}s\n"
            f"Success: {total_success}, Failed: {total_failed}\n"
            f"Creates: {total_creates}, Updates: {total_updates}\n"
            f"Errors: {error_summary}",
            is_error=True
        )
    elif total_success > 0:
        print(f"[DATAVERSE] ✓ All records synced successfully")
    
    return results

# ==========================================================
# 6) Load config
# ==========================================================
session.sql("CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG TYPE = JSON").collect()
etl_start_time = time.time()

cfg_sql = f"""
    SELECT
      LOWER(p.value:practice_name::string)                   AS practice_name,
      f.value:snowflake:refined_database::string             AS refined_db,
      f.value:snowflake:refined_schema::string               AS refined_schema,
      f.value:snowflake:refined_table::string                AS refined_table,
      f.value:snowflake:curated_database::string             AS curated_db,
      f.value:snowflake:curated_schema::string               AS curated_schema,
      f.value:snowflake:curated_table::string                AS curated_table,
      f.value:snowflake:curated_column_mapping               AS curated_column_mapping,
      f.value:snowflake:curated_lookup:lookup_table::string  AS lookup_table,
      f.value:snowflake:curated_lookup:source_key::string    AS lookup_source_key,
      f.value:snowflake:curated_lookup:lookup_key::string    AS lookup_lookup_key,
      f.value:snowflake:curated_lookup:result_column::string AS lookup_result_column,
      f.value:snowflake:curated_lookup:match_value::string   AS lookup_match_value,
      f.value:snowflake:curated_lookup:no_match_value::string AS lookup_no_match_value,
      f.value:snowflake:dataverse_sync                       AS dataverse_sync_config,
      f.value:snowflake:source_filter                         AS source_filter_config
    FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
         LATERAL FLATTEN(input => cfg.$1:Practices) p,
         LATERAL FLATTEN(input => p.value:ingest) f
    WHERE LOWER(p.value:practice_name::string) = LOWER('{PRACTICE_NAME}')
      AND LOWER(f.value:file_type::string) = LOWER('{FILE_TYPE}')
"""

cfg_rows = session.sql(cfg_sql).collect()
if not cfg_rows:
    err = f"No config found for practice={PRACTICE_NAME}, file_type={FILE_TYPE}"
    log_curated(PRACTICE_NAME, "N/A", "N/A", "Load Configuration", 0, "ERROR", etl_start_time, error_message=err)
    raise ValueError(err)

cfg = cfg_rows[0]

refined_db = cfg["REFINED_DB"]
refined_sch = cfg["REFINED_SCHEMA"]
refined_tbl = cfg["REFINED_TABLE"]
curated_db = cfg["CURATED_DB"]
curated_sch = cfg["CURATED_SCHEMA"]
curated_tbl = cfg["CURATED_TABLE"]
column_mapping_raw = cfg["CURATED_COLUMN_MAPPING"]

lookup_table = cfg["LOOKUP_TABLE"]
lookup_source_key = cfg["LOOKUP_SOURCE_KEY"]
lookup_lookup_key = cfg["LOOKUP_LOOKUP_KEY"]
lookup_result_column = cfg["LOOKUP_RESULT_COLUMN"]
lookup_match_value = cfg["LOOKUP_MATCH_VALUE"]
lookup_no_match_value = cfg["LOOKUP_NO_MATCH_VALUE"]

dataverse_sync_config_raw = cfg["DATAVERSE_SYNC_CONFIG"]
dataverse_sync_config = None
if dataverse_sync_config_raw:
    try:
        if isinstance(dataverse_sync_config_raw, str):
            dataverse_sync_config = json.loads(dataverse_sync_config_raw)
        else:
            dataverse_sync_config = dict(dataverse_sync_config_raw)
    except Exception as e:
        print(f"[WARN] Could not parse dataverse_sync config: {e}")

# ==========================================================
# 6b) Parse source_filter config
# ==========================================================
source_filter_config_raw = cfg["SOURCE_FILTER_CONFIG"]
source_filter_list = []
if source_filter_config_raw:
    try:
        source_filter_list = ensure_list_of_dicts(source_filter_config_raw)
        if source_filter_list:
            print(f"[INFO] Source filter configured: {len(source_filter_list)} condition(s)")
            for sf in source_filter_list:
                print(f"[INFO]   - {sf.get('column')} {sf.get('operator', '=')} '{sf.get('value')}'")
        else:
            print(f"[INFO] No source filter configured (all records pass)")
    except Exception as e:
        print(f"[WARN] Could not parse source_filter config: {e}")
        source_filter_list = []

has_curated_lookup = all([
    _is_configured_value(lookup_table),
    _is_configured_value(lookup_source_key),
    _is_configured_value(lookup_lookup_key),
    _is_configured_value(lookup_result_column),
    _is_configured_value(lookup_match_value),
    _is_configured_value(lookup_no_match_value)
])

if not (_is_configured_value(refined_db) and _is_configured_value(refined_sch) and _is_configured_value(refined_tbl)):
    err = "Refined source table configuration is incomplete (refined_database/refined_schema/refined_table)."
    log_curated(PRACTICE_NAME, "N/A", "N/A", "Load Configuration", 0, "ERROR", etl_start_time, error_message=err)
    raise ValueError(err)

if not (_is_configured_value(curated_db) and _is_configured_value(curated_sch) and _is_configured_value(curated_tbl)):
    print("[INFO] No curated table configured. Skipping.")
    log_curated(
        PRACTICE_NAME,
        "N/A",
        "N/A",
        "Pipeline Complete",
        0,
        "SKIPPED",
        etl_start_time,
        error_message="No curated_database/curated_schema/curated_table configured"
    )
else:
    source_table = f"{refined_db}.{refined_sch}.{refined_tbl}"
    target_table = f"{curated_db}.{curated_sch}.{curated_tbl}"

    print(f"[INFO] Source (refined): {source_table}")
    print(f"[INFO] Target (curated): {target_table}")
    
    if has_curated_lookup:
        print(f"[INFO] Lookup table: {lookup_table}")
        print(f"[INFO] Lookup join: {lookup_source_key} = {lookup_lookup_key}")
    else:
        print(f"[INFO] No lookup configured - will write directly to curated table")
    
    if dataverse_sync_config and dataverse_sync_config.get("enabled"):
        print(f"[INFO] Dataverse sync: ENABLED")
    else:
        print(f"[INFO] Dataverse sync: DISABLED")

    # ==========================================================
    # 6a) Parse office_mapping config
    # ==========================================================
    office_mapping_config = None
    if dataverse_sync_config:
        office_mapping_config = dataverse_sync_config.get("office_mapping")
    
    if office_mapping_config:
        print(f"[INFO] Office mapping: ENABLED")
        print(f"[INFO]   Lookup table: {office_mapping_config.get('lookup_table')}")
        print(f"[INFO]   {office_mapping_config.get('source_column')} -> {office_mapping_config.get('result_column')} AS {office_mapping_config.get('target_alias')}")
    else:
        print(f"[INFO] Office mapping: NOT CONFIGURED (practice uses native office column)")

    # ==========================================================
    # 7) Parse column mappings
    # ==========================================================
    column_mappings = ensure_list_of_dicts(column_mapping_raw)

    if not column_mappings:
        err = "No curated_column_mapping defined in config."
        log_curated(PRACTICE_NAME, source_table, target_table, "Load Configuration", 0, "ERROR", etl_start_time, error_message=err)
        raise ValueError(err)

    print(f"[INFO] Column mappings loaded: {len(column_mappings)}")

    # ==========================================================
    # 8) Build SELECT expression
    # ==========================================================
    source_cols_df = session.sql(f"SELECT * FROM {source_table} LIMIT 0")
    actual_source_cols = [_unquote_ident(c) for c in list(source_cols_df.columns)]

    norm_to_actual_source = {}
    for c in actual_source_cols:
        k = _norm_name(c)
        if k and k not in norm_to_actual_source:
            norm_to_actual_source[k] = c

    source_cols_map = {c.upper(): c for c in actual_source_cols}

    print(f"[INFO] Source columns (actual): {actual_source_cols}")

    # ==========================================================
    # 8b) Build source_filter SQL clauses
    # ==========================================================
    source_filter_sql = ""
    source_filter_clauses = []
    if source_filter_list:
        for sf in source_filter_list:
            col_name = (sf.get("column") or "").upper()
            operator = sf.get("operator", "=")
            value = sf.get("value", "")
            
            actual_col = source_cols_map.get(col_name, col_name)
            safe_value = str(value).replace("'", "''")
            
            if operator.upper() in ("=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE"):
                clause = f"src.{_quote_ident(actual_col)} {operator} '{safe_value}'"
                source_filter_clauses.append(clause)
            elif operator.upper() == "IN":
                values = [v.strip().replace("'", "''") for v in str(value).split(",")]
                in_list = ", ".join(f"'{v}'" for v in values)
                clause = f"src.{_quote_ident(actual_col)} IN ({in_list})"
                source_filter_clauses.append(clause)
            elif operator.upper() == "IS NULL":
                clause = f"src.{_quote_ident(actual_col)} IS NULL"
                source_filter_clauses.append(clause)
            elif operator.upper() == "IS NOT NULL":
                clause = f"src.{_quote_ident(actual_col)} IS NOT NULL"
                source_filter_clauses.append(clause)
            else:
                print(f"[WARN] Unknown source_filter operator '{operator}', skipping")
        
        if source_filter_clauses:
            source_filter_sql = " AND " + " AND ".join(source_filter_clauses)
            print(f"[INFO] Source filter SQL: {source_filter_sql}")
    else:
        print(f"[INFO] No source filter - all IS_VALID=1 records will be processed")

    def resolve_source_col(configured_name: str):
        if not configured_name:
            return None
        cfg_clean = _unquote_ident(configured_name.strip())
        if cfg_clean in actual_source_cols:
            return cfg_clean
        return norm_to_actual_source.get(_norm_name(cfg_clean))

    select_parts = []
    insert_target_cols = []
    unresolved_sources = []

    for mapping in column_mappings:
        if not isinstance(mapping, dict):
            try:
                mapping = json.loads(str(mapping))
            except Exception:
                continue

        target_col = (mapping.get("target") or "").strip()
        source_col_cfg = (mapping.get("source") or "").strip()
        concat_with_cfg = (mapping.get("concat_with") or "").strip()
        concat_sep = mapping.get("concat_separator", " ")
        fallback_cfg = (mapping.get("fallback") or "").strip()

        if not target_col or not source_col_cfg:
            continue

        target_col_sql = target_col.upper()
        insert_target_cols.append(target_col_sql)

        actual_source = resolve_source_col(source_col_cfg)
        if not actual_source:
            print(f"[WARN] Source column '{source_col_cfg}' not found (case-insensitive). Using NULL for '{target_col_sql}'.")
            unresolved_sources.append(source_col_cfg)
            select_parts.append(f"NULL AS {target_col_sql}")
            continue

        src_expr = f"src.{_quote_ident(actual_source)}"

        actual_concat = resolve_source_col(concat_with_cfg) if concat_with_cfg else None
        actual_fallback = resolve_source_col(fallback_cfg) if fallback_cfg else None

        if actual_concat:
            concat_expr = f"src.{_quote_ident(actual_concat)}"
            concat_sep_sql = str(concat_sep).replace("'", "''")
            expr = (
                f"CONCAT(COALESCE({src_expr}, ''), "
                f"'{concat_sep_sql}', "
                f"COALESCE({concat_expr}, '')) AS {target_col_sql}"
            )
        elif actual_fallback:
            fallback_expr = f"src.{_quote_ident(actual_fallback)}"
            expr = f"COALESCE({src_expr}, {fallback_expr}) AS {target_col_sql}"
        else:
            expr = f"{src_expr} AS {target_col_sql}"

        select_parts.append(expr)

    if not select_parts or not insert_target_cols:
        err = "No valid curated mappings produced a SELECT clause."
        log_curated(PRACTICE_NAME, source_table, target_table, "Build SQL Query", 0, "ERROR", etl_start_time, error_message=err)
        raise ValueError(err)

    if unresolved_sources:
        print(f"[WARN] Unresolved mapped source columns (count={len(unresolved_sources)}): {sorted(set(unresolved_sources))}")

    # ==========================================================
    # 9) Add metadata columns
    # ==========================================================
    target_cols_df = session.sql(f"SELECT * FROM {target_table} LIMIT 0")
    actual_target_cols = [_unquote_ident(c) for c in list(target_cols_df.columns)]
    actual_target_cols_upper = {c.upper() for c in actual_target_cols}

    if "SOURCE_PRACTICE" in actual_target_cols_upper:
        select_parts.append(f"'{PRACTICE_NAME.upper()}' AS SOURCE_PRACTICE")
        insert_target_cols.append("SOURCE_PRACTICE")

    if "SOURCE_TABLE" in actual_target_cols_upper:
        select_parts.append(f"'{source_table}' AS SOURCE_TABLE")
        insert_target_cols.append("SOURCE_TABLE")

    if "FILE_LOAD_TIME" in actual_target_cols_upper:
        select_parts.append("CURRENT_TIMESTAMP() AS FILE_LOAD_TIME")
        insert_target_cols.append("FILE_LOAD_TIME")

    if "PARENT_RUN_ID" in actual_target_cols_upper:
        select_parts.append(f"'{PARENT_RUN_ID}' AS PARENT_RUN_ID")
        insert_target_cols.append("PARENT_RUN_ID")

    if "CREATED_DATE" in actual_target_cols_upper:
        select_parts.append(f"'{datetime.now().strftime('%Y-%m-%d')}' AS CREATED_DATE")
        insert_target_cols.append("CREATED_DATE")

    if "RECORD_TYPE" in actual_target_cols_upper:
        if has_curated_lookup:
            record_type_expr = f"""CASE 
                WHEN lookup.{_quote_ident(lookup_lookup_key)} IS NOT NULL THEN '{lookup_match_value}'
                ELSE '{lookup_no_match_value}'
            END AS RECORD_TYPE"""
            select_parts.append(record_type_expr)
            insert_target_cols.append("RECORD_TYPE")
        else:
            select_parts.append("'NEW' AS RECORD_TYPE")
            insert_target_cols.append("RECORD_TYPE")

    select_clause = ",\n       ".join(select_parts)
    insert_cols = ", ".join(insert_target_cols)

    # ==========================================================
    # 10) Build and execute INSERT
    # ==========================================================
    if has_curated_lookup:
        insert_sql = f"""
            INSERT INTO {target_table} ({insert_cols})
            SELECT {select_clause}
            FROM {source_table} src
            LEFT JOIN {lookup_table} lookup
                ON src.{_quote_ident(lookup_source_key)} = lookup.{_quote_ident(lookup_lookup_key)}
            WHERE src.IS_VALID = 1{source_filter_sql}
        """
        print(f"\\n[INFO] Executing INSERT with LEFT JOIN lookup (filtering WHERE IS_VALID = 1{source_filter_sql})...")
    else:
        insert_sql = f"""
            INSERT INTO {target_table} ({insert_cols})
            SELECT {select_clause}
            FROM {source_table} src
            WHERE src.IS_VALID = 1{source_filter_sql}
        """
        print(f"\\n[INFO] Executing INSERT directly (no lookup, filtering WHERE IS_VALID = 1{source_filter_sql})...")
    
    print(f"[DEBUG] SQL:\n{insert_sql}")

    write_start_time = time.time()
    new_count = 0
    update_count = 0

    try:
        count_result = session.sql(f"SELECT COUNT(*) AS CNT FROM {source_table} src WHERE src.IS_VALID = 1{source_filter_sql}").collect()
        row_count = count_result[0]["CNT"]
        print(f"[INFO] Rows to insert (IS_VALID=1): {row_count}")

        if row_count == 0:
            print("[INFO] No valid records to process. Skipping INSERT.")
            log_curated(PRACTICE_NAME, source_table, target_table, "Write to Curated Table",
                        0, "SKIPPED", write_start_time, time.time(), 
                        error_message="No records with IS_VALID=1")
        else:
            session.sql(insert_sql).collect()

            write_end_time = time.time()
            log_curated(PRACTICE_NAME, source_table, target_table, "Write to Curated Table",
                        row_count, "SUCCESS", write_start_time, write_end_time)

            print(f"\n[SUCCESS] Inserted {row_count} rows into {target_table}")

            # ==========================================================
            # 11) Log RECORD_TYPE distribution
            # ==========================================================
            if has_curated_lookup:
                try:
                    record_type_sql = f"""
                        SELECT 
                            CASE 
                                WHEN lookup.{_quote_ident(lookup_lookup_key)} IS NOT NULL THEN '{lookup_match_value}'
                                ELSE '{lookup_no_match_value}'
                            END AS RECORD_TYPE,
                            COUNT(*) AS RECORD_COUNT
                        FROM {source_table} src
                        LEFT JOIN {lookup_table} lookup
                            ON src.{_quote_ident(lookup_source_key)} = lookup.{_quote_ident(lookup_lookup_key)}
                        WHERE src.IS_VALID = 1{source_filter_sql}
                        GROUP BY 1
                    """
                    record_type_counts = session.sql(record_type_sql).collect()
                    
                    new_count = 0
                    update_count = 0
                    for row in record_type_counts:
                        if row["RECORD_TYPE"] == lookup_no_match_value:
                            new_count = row["RECORD_COUNT"]
                        elif row["RECORD_TYPE"] == lookup_match_value:
                            update_count = row["RECORD_COUNT"]
                    
                    print(f"\n[INFO] RECORD_TYPE Distribution:")
                    print(f"  - {lookup_no_match_value}: {new_count} records")
                    print(f"  - {lookup_match_value}: {update_count} records")
                    
                    log_curated(PRACTICE_NAME, source_table, target_table, "New Records Identified",
                                new_count, "SUCCESS", write_start_time, write_end_time)
                    log_curated(PRACTICE_NAME, source_table, target_table, "Update Records Identified",
                                update_count, "SUCCESS", write_start_time, write_end_time)
                                
                except Exception as e:
                    print(f"[WARN] Could not get RECORD_TYPE distribution: {e}")

            # ==========================================================
            # 12) DATAVERSE SYNC (if enabled)
            # ==========================================================
            dataverse_results = None
            if dataverse_sync_config and dataverse_sync_config.get("enabled"):
                try:
                    print("\n[INFO] Fetching source data for Dataverse sync...")
                    fetch_start = time.time()
                    
                    mbi_col_quoted = _quote_ident(source_cols_map.get(lookup_source_key, lookup_source_key))
                    
                    count_sql = f"""
                        SELECT 
                            COUNT(*) as TOTAL_RECORDS,
                            COUNT(CASE WHEN {mbi_col_quoted} IS NOT NULL AND TRIM({mbi_col_quoted}) != '' THEN 1 END) as WITH_MBI,
                            COUNT(CASE WHEN {mbi_col_quoted} IS NULL OR TRIM({mbi_col_quoted}) = '' THEN 1 END) as WITHOUT_MBI
                        FROM {source_table} src
                        WHERE src.IS_VALID = 1{source_filter_sql}
                    """
                    count_result = session.sql(count_sql).collect()[0]
                    total_records = count_result["TOTAL_RECORDS"]
                    records_with_mbi = count_result["WITH_MBI"]
                    records_without_mbi = count_result["WITHOUT_MBI"]
                    
                    print(f"[INFO] Source data summary:")
                    print(f"[INFO]   - Total records (IS_VALID=1): {total_records}")
                    print(f"[INFO]   - Records WITH MBI ({lookup_source_key}): {records_with_mbi}")
                    print(f"[INFO]   - Records WITHOUT MBI (will use dummy NOMBI_*): {records_without_mbi}")
                    
                    if records_without_mbi > 0:
                        log_curated(PRACTICE_NAME, source_table, "DATAVERSE", "Records Using Dummy MBI",
                                    records_without_mbi, "INFO", fetch_start,
                                    error_message=f"{records_without_mbi} records without {lookup_source_key} will use dummy MBI (NOMBI_PatientNumber)")
                    
                    # Build WHERE clauses with src alias for JOIN compatibility
                    where_clauses = ["src.IS_VALID = 1"]
                    
                    # Apply source_filter (Eligible = 'Y')
                    if source_filter_clauses:
                        where_clauses.extend(source_filter_clauses)
                        print(f"[INFO] Source filter applied to Dataverse fetch: {source_filter_clauses}")
                    
                    future_filter = dataverse_sync_config.get("future_only_filter", {})
                    if future_filter.get("enabled"):
                        dt_columns = future_filter.get("datetime_columns", [])
                        separator = future_filter.get("separator", " ")
                        
                        if dt_columns:
                            if len(dt_columns) == 1:
                                dt_expr = f"src.{_quote_ident(source_cols_map.get(dt_columns[0].upper(), dt_columns[0]))}"
                            else:
                                col_parts = [f"src.{_quote_ident(source_cols_map.get(c.upper(), c))}" for c in dt_columns]
                                concat_parts = []
                                for i, part in enumerate(col_parts):
                                    concat_parts.append(part)
                                    if i < len(col_parts) - 1:
                                        concat_parts.append(f"'{separator}'")
                                dt_expr = f"CONCAT({', '.join(concat_parts)})"
                            
                            where_clauses.append(f"TRY_TO_TIMESTAMP({dt_expr}) > CURRENT_TIMESTAMP()")
                            print(f"[INFO] Future-only filter enabled: {dt_expr} > CURRENT_TIMESTAMP()")
                    
                    # ==============================================================
                    # Office mapping - LEFT JOIN to resolve location -> office name
                    # ==============================================================
                    office_join_sql = ""
                    office_select_col = ""
                    
                    if office_mapping_config:
                        om_lookup_table = office_mapping_config.get("lookup_table")
                        om_source_column = office_mapping_config.get("source_column", "").upper()
                        om_lookup_column = office_mapping_config.get("lookup_column")
                        om_result_column = office_mapping_config.get("result_column")
                        om_target_alias = office_mapping_config.get("target_alias", "OFFICENAME")
                        
                        if all([om_lookup_table, om_source_column, om_lookup_column, om_result_column]):
                            om_src_actual = source_cols_map.get(om_source_column, om_source_column)
                            office_join_sql = (
                                f"\n    LEFT JOIN {om_lookup_table} omap"
                                f"\n        ON src.{_quote_ident(om_src_actual)} = omap.{_quote_ident(om_lookup_column)}"
                            )
                            office_select_col = f", omap.{_quote_ident(om_result_column)} AS {_quote_ident(om_target_alias)}"
                            
                            print(f"[INFO] Office mapping: src.{om_src_actual} -> omap.{om_result_column} AS {om_target_alias}")
                            print(f"[INFO]   JOIN: {om_lookup_table} ON {om_src_actual} = {om_lookup_column}")
                        else:
                            missing = [k for k, v in {
                                "lookup_table": om_lookup_table,
                                "source_column": om_source_column,
                                "lookup_column": om_lookup_column,
                                "result_column": om_result_column
                            }.items() if not v]
                            print(f"[WARN] Office mapping config incomplete, missing: {missing}. Skipping office mapping.")
                    
                    source_data_sql = f"SELECT src.*{office_select_col} FROM {source_table} src{office_join_sql} WHERE {' AND '.join(where_clauses)}"
                    
                    print(f"[DEBUG] Source data SQL: {source_data_sql[:500]}")
                    
                    source_data_rows = session.sql(source_data_sql).collect()
                    source_data = [dict(row.as_dict()) for row in source_data_rows]
                    fetch_duration = round(time.time() - fetch_start, 2)
                    
                    print(f"[INFO] Fetched {len(source_data)} records for Dataverse sync ({fetch_duration}s)")
                    
                    # Add office alias to source_cols_map so field mappings can reference it
                    if office_mapping_config and source_data:
                        om_target_alias = office_mapping_config.get("target_alias", "OFFICENAME")
                        source_cols_map[om_target_alias.upper()] = om_target_alias
                        print(f"[INFO] Office mapping: Added '{om_target_alias}' to source_cols_map")
                        
                        # Log office mapping coverage
                        mapped_count = sum(1 for r in source_data if r.get(om_target_alias))
                        unmapped_count = len(source_data) - mapped_count
                        print(f"[INFO] Office mapping coverage: {mapped_count} mapped, {unmapped_count} unmapped")
                        if unmapped_count > 0:
                            om_source_column = office_mapping_config.get("source_column", "").upper()
                            om_src_actual = source_cols_map.get(om_source_column, om_source_column)
                            unmapped_locations = set()
                            for r in source_data:
                                if not r.get(om_target_alias):
                                    loc = r.get(om_src_actual, "")
                                    if loc:
                                        unmapped_locations.add(str(loc))
                            if unmapped_locations:
                                sample = sorted(unmapped_locations)[:10]
                                print(f"[WARN] Unmapped locations (sample): {sample}")
                                log_curated(PRACTICE_NAME, source_table, "DATAVERSE", 
                                            "Office Mapping - Unmapped Locations",
                                            unmapped_count, "WARNING", fetch_start,
                                            error_message=f"{unmapped_count} records with unmapped location_name. Sample: {', '.join(sample[:5])}")
                    
                    log_curated(PRACTICE_NAME, source_table, "DATAVERSE", "Fetch Source Data for Dataverse",
                                len(source_data), "SUCCESS", fetch_start,
                                error_message=f"Fetched {len(source_data)} total records ({records_with_mbi} with MBI, {records_without_mbi} will use dummy MBI)")
                    
                    # Build patient lookup results
                    print("[INFO] Building patient lookup results...")
                    patient_lookup_start = time.time()
                    patient_lookup_results = {}
                    if has_curated_lookup:
                        patient_lookup_sql = f"""
                            SELECT 
                                UPPER(src.{_quote_ident(lookup_source_key)}) AS MBI,
                                lookup.CR063_SPOTPATIENTDEMOGRAPHICSID AS RECORD_ID
                            FROM {source_table} src
                            LEFT JOIN {lookup_table} lookup
                                ON src.{_quote_ident(lookup_source_key)} = lookup.{_quote_ident(lookup_lookup_key)}
                            WHERE src.IS_VALID = 1{source_filter_sql}
                              AND src.{_quote_ident(lookup_source_key)} IS NOT NULL
                              AND TRIM(src.{_quote_ident(lookup_source_key)}) != ''
                        """
                        patient_lookup_rows = session.sql(patient_lookup_sql).collect()
                        existing_count = 0
                        new_count_dv = 0
                        for row in patient_lookup_rows:
                            mbi = row["MBI"]
                            if mbi:
                                record_id = row["RECORD_ID"]
                                patient_lookup_results[mbi.upper()] = record_id
                                if record_id:
                                    existing_count += 1
                                else:
                                    new_count_dv += 1
                        
                        patient_lookup_duration = round(time.time() - patient_lookup_start, 2)
                        print(f"[INFO] Patient lookup: {existing_count} existing, {new_count_dv} new ({patient_lookup_duration}s)")
                        log_curated(PRACTICE_NAME, lookup_table, "PATIENT_LOOKUP", "Patient Lookup in Curated",
                                    len(patient_lookup_results), "SUCCESS", patient_lookup_start,
                                    error_message=f"Existing: {existing_count}, New: {new_count_dv}")
                    
                    # Build appointment lookup results
                    print("[INFO] Building appointment lookup results...")
                    appt_lookup_start = time.time()
                    appt_lookup_results = {}
                    
                    appt_config = dataverse_sync_config.get("appointment_data", {})
                    appt_lookup_keys = appt_config.get("lookup_keys", {})
                    appt_entity = appt_config.get("entity", "crddb_vaaffliatedatas")
                    
                    if appt_lookup_keys and has_curated_lookup:
                        if isinstance(appt_lookup_keys, list):
                            lookup_key_configs = appt_lookup_keys
                        else:
                            patient_id_cfg = appt_lookup_keys.get("patient_id", {})
                            appointment_date_cfg = appt_lookup_keys.get("appointment_date", {})
                            lookup_key_configs = [
                                {"source": patient_id_cfg.get("source", "PATIENTNUMBER"), "lookup": patient_id_cfg.get("lookup")},
                                {"source": appointment_date_cfg.get("source", "APPOINTMENTDATE"), "lookup": appointment_date_cfg.get("lookup")}
                            ]
                        
                        dv_lookup_columns = []
                        src_lookup_columns = []
                        for key_cfg in lookup_key_configs:
                            src_col = key_cfg.get("source")
                            lkp_col = key_cfg.get("lookup")
                            if src_col and lkp_col:
                                src_expr = build_sql_column_expr(src_col, source_cols_map)
                                dv_lookup_columns.append(lkp_col)
                                src_lookup_columns.append(src_expr)
                        
                        if dv_lookup_columns:
                            environments = dataverse_sync_config.get("environments", {})
                            environment_url = environments.get(DATAVERSE_TARGET)
                            if not environment_url:
                                environment_url = dataverse_sync_config.get("environment_url")
                            
                            secret_name = dataverse_sync_config.get("secret_name")
                            
                            try:
                                credentials = get_dataverse_credentials(secret_name)
                                access_token = get_dataverse_token(credentials, environment_url)
                                
                                select_fields = ",".join(dv_lookup_columns + [f"{appt_entity}id"])
                                
                                appt_query_url = f"{environment_url}/api/data/v9.2/{appt_entity}?$select={select_fields}&$top=5000"
                                
                                headers = {
                                    "Authorization": f"Bearer {access_token}",
                                    "OData-MaxVersion": "4.0",
                                    "OData-Version": "4.0",
                                    "Accept": "application/json",
                                    "Prefer": "odata.maxpagesize=5000"
                                }
                                
                                all_appt_records = []
                                next_link = appt_query_url
                                page_count = 0
                                
                                while next_link:
                                    resp = requests.get(next_link, headers=headers, timeout=60)
                                    if resp.status_code == 200:
                                        data = resp.json()
                                        records = data.get("value", [])
                                        all_appt_records.extend(records)
                                        next_link = data.get("@odata.nextLink")
                                        page_count += 1
                                        if page_count % 10 == 0:
                                            print(f"[INFO] Appointment lookup: {len(all_appt_records)} records fetched ({page_count} pages)...")
                                    else:
                                        print(f"[WARN] Appointment lookup query failed: HTTP {resp.status_code}")
                                        next_link = None
                                
                                for record in all_appt_records:
                                    key_values = []
                                    for lkp_col in dv_lookup_columns:
                                        val = str(record.get(lkp_col, "") or "").strip()
                                        key_values.append(val)
                                    if all(key_values):
                                        lookup_key = tuple(key_values)
                                        record_id = record.get(f"{appt_entity}id")
                                        if record_id:
                                            appt_lookup_results[lookup_key] = record_id
                                
                                appt_lookup_duration = round(time.time() - appt_lookup_start, 2)
                                print(f"[INFO] Appointment lookup: {len(appt_lookup_results)} existing records loaded ({appt_lookup_duration}s)")
                                log_curated(PRACTICE_NAME, appt_entity, "APPT_LOOKUP", "Appointment Lookup in Dataverse",
                                            len(appt_lookup_results), "SUCCESS", appt_lookup_start,
                                            error_message=f"Total records: {len(all_appt_records)}, Unique keys: {len(appt_lookup_results)}")
                                
                            except Exception as e:
                                print(f"[WARN] Appointment lookup failed: {str(e)[:200]}")
                                print("[WARN] Continuing without appointment lookup - all records will be treated as NEW")
                                log_curated(PRACTICE_NAME, appt_entity, "APPT_LOOKUP", "Appointment Lookup Failed",
                                            0, "WARNING", appt_lookup_start,
                                            error_message=f"Lookup failed: {str(e)[:200]}. All records will be POSTed as new.")
                    
                    # Execute Dataverse sync
                    dataverse_results = sync_to_dataverse(
                        dataverse_sync_config,
                        source_data,
                        source_cols_map,
                        patient_lookup_results,
                        appt_lookup_results
                    )
                    
                except Exception as e:
                    error_msg = f"Dataverse sync error: {str(e)[:500]}"
                    print(f"[ERROR] {error_msg}")
                    import traceback
                    traceback.print_exc()
                    log_curated(PRACTICE_NAME, source_table, "DATAVERSE", "Dataverse Sync Error",
                                0, "ERROR", etl_start_time, error_message=error_msg)
                    send_teams_notification(
                        f"Dataverse Sync Error - {PRACTICE_NAME}/{FILE_TYPE}",
                        error_msg
                    )

            # ==========================================================
            # 13) Mark refined records as processed (SET IS_VALID = 0)
            # ==========================================================
            mark_start_time = time.time()
            try:
                mark_sql = f"UPDATE {source_table} SET IS_VALID = 0 WHERE IS_VALID = 1"
                session.sql(mark_sql).collect()
                
                mark_end_time = time.time()
                log_curated(PRACTICE_NAME, source_table, target_table, "Mark Refined Records Processed",
                            row_count, "SUCCESS", mark_start_time, mark_end_time)
                print(f"\n[SUCCESS] Marked {row_count} refined records as processed (IS_VALID = 0)")
                
            except Exception as e:
                error_msg = f"Failed to mark refined records: {str(e)[:300]}"
                print(f"[ERROR] {error_msg}")
                log_curated(PRACTICE_NAME, source_table, target_table, "Mark Refined Records Processed",
                            0, "ERROR", mark_start_time, error_message=error_msg)

    except Exception as e:
        error_msg = f"ETL pipeline error: {str(e)[:500]}"
        print(f"\n[ERROR] {error_msg}")
        import traceback
        traceback.print_exc()
        log_curated(PRACTICE_NAME, source_table, target_table, "Pipeline Error",
                    0, "ERROR", etl_start_time, error_message=error_msg)
        send_teams_notification(
            f"Curated ETL Failed - {PRACTICE_NAME}/{FILE_TYPE}",
            f"Run ID: {PARENT_RUN_ID}\nError: {error_msg}"
        )
        raise

    # ==========================================================
    # 14) Pipeline complete
    # ==========================================================
    total_duration = round(time.time() - etl_start_time, 2)
    
    summary_parts = [
        f"Duration: {total_duration}s",
        f"Curated rows: {row_count}",
    ]
    
    if has_curated_lookup:
        summary_parts.append(f"New: {new_count}, Update: {update_count}")
    
    if dataverse_results:
        pd = dataverse_results.get("patient_demo", {})
        ap = dataverse_results.get("appointment", {})
        summary_parts.append(
            f"DV Patient: {pd.get('success', 0)}/{pd.get('total', 0)}, "
            f"DV Appt: {ap.get('success', 0)}/{ap.get('total', 0)}"
        )
    
    summary = " | ".join(summary_parts)
    
    log_curated(PRACTICE_NAME, source_table, target_table, "Pipeline Complete",
                row_count, "SUCCESS", etl_start_time, error_message=summary)
    
    print(f"\n{'=' * 70}")
    print(f"[COMPLETE] Curated ETL Pipeline Finished")
    print(f"[COMPLETE] {summary}")
    print(f"{'=' * 70}")
