# RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE.py
# ==========================================================
# Notebook Name: RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE
# Location: UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE
# ==========================================================
#
# Generic column transformation script driven by JSON config in Snowflake stage.
# For the given practice/file_type:
# - Applies column_regex_replace rules (regex-based substitutions)
# - Applies column_reformat rules (split/reorder operations)
# - Preserves original column order
# - Appends to (or creates) refined target table per config
# - Logs all operations to REFINED_INGEST_LOG table
#
# ==========================================================
# PARAMETER REQUIREMENT:
# ==========================================================
# This notebook is intended to be triggered from the RAW_ADLS_TO_RAW_SNOWFLAKE notebook
# via EXECUTE NOTEBOOK:
#
#   EXECUTE NOTEBOOK UPPERLINE.RAW_INBOUND_AFFILIATEDATA.RAW_SNOWFLAKE_TO_REFINED_SNOWFLAKE('practice|file_type|raw_run_id')
#
# Parameters:
#   - practice: The practice name (e.g., 'werter', 'fastpace')
#   - file_type: The file type (e.g., 'AppointmentData')
#   - raw_run_id: The PARENT_RUN_ID from the raw ingest (optional, for linking logs)
#
# ==========================================================
# CHANGE LOG:
# ==========================================================
# v2 - 2026-02-03: Added 3-hour timeout (10800 seconds) before CURATED notebook call
# ==========================================================

import json
import sys
import re
import uuid
import time
from datetime import datetime
from snowflake.snowpark.functions import col, when, regexp_replace, split, trim, concat_ws, lit
from snowflake.snowpark.context import get_active_session

# ==========================================================
# 0) Session & config locations
# ==========================================================
session = get_active_session()
print(session)

# PARENT_RUN_ID will be set after parameter parsing
# If passed from upstream (RAW ETL), use that for end-to-end traceability
# Otherwise generate a new one
PARENT_RUN_ID = None  # Will be set after parsing parameters

CONFIG_STAGE_PREFIX = "@UPPERLINE.PUBLIC.STAGE_STULHETLDEV001_RAW/util/config/"
CONFIG_FILENAME = "practice_ingest_config_v17.json"

# Log table for tracking refined ingests
REFINED_LOG_TABLE = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.REFINED_INGEST_LOG"

# ==========================================================
# 1) PARAMETER HANDLING
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
    Parse arguments passed via EXECUTE NOTEBOOK.
    
    Supports:
      1) Single pipe-delimited arg: "practice|file_type|raw_run_id"
      2) Two positional args: practice, file_type
      3) Three positional args: practice, file_type, raw_run_id
    
    Returns:
      (practice_name, file_type, raw_run_id)
    """
    practice_name = None
    file_type = None
    raw_run_id = None
    
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
        if s.startswith("-"):
            continue
        argv.append(s)
    
    if not argv:
        print("[INFO] No parameters provided")
        return practice_name, file_type, raw_run_id
    
    # Prefer a pipe-delimited argument if present
    pipe_arg = next((a for a in argv if "|" in a), None)
    if pipe_arg is not None:
        parts = pipe_arg.split("|")
        if len(parts) >= 1:
            practice_name = _strip_wrapping_quotes(parts[0].strip()) or None
        if len(parts) >= 2:
            file_type = _strip_wrapping_quotes(parts[1].strip()) or None
        if len(parts) >= 3:
            raw_run_id = _strip_wrapping_quotes(parts[2].strip()) or None
        
        print(f"[INFO] Parsed from pipe-delimited: practice={practice_name}, file_type={file_type}, raw_run_id={raw_run_id}")
        return practice_name, file_type, raw_run_id
    
    # Positional arguments
    if len(argv) >= 1:
        practice_name = _strip_wrapping_quotes(argv[0].strip()) or None
    if len(argv) >= 2:
        file_type = _strip_wrapping_quotes(argv[1].strip()) or None
    if len(argv) >= 3:
        raw_run_id = _strip_wrapping_quotes(argv[2].strip()) or None
    
    print(f"[INFO] Parsed from positional: practice={practice_name}, file_type={file_type}, raw_run_id={raw_run_id}")
    return practice_name, file_type, raw_run_id

# Parse parameters
PRACTICE_NAME, FILE_TYPE, RAW_RUN_ID = parse_parameters()

# Set PARENT_RUN_ID: use upstream RAW_RUN_ID if provided for end-to-end traceability
# Otherwise generate a new one for standalone runs
if RAW_RUN_ID:
    PARENT_RUN_ID = RAW_RUN_ID
    print(f"[INFO] Using upstream PARENT_RUN_ID for end-to-end tracking: {PARENT_RUN_ID}")
else:
    PARENT_RUN_ID = str(uuid.uuid4())
    print(f"[INFO] Generated new PARENT_RUN_ID (standalone run): {PARENT_RUN_ID}")

# Validate required parameters
if not PRACTICE_NAME or not FILE_TYPE:
    print("\n" + "-" * 60)
    print("[ERROR] Both practice and file_type are required parameters.")
    print(f"[INFO] Received: practice={PRACTICE_NAME or 'NONE'}, file_type={FILE_TYPE or 'NONE'}")
    print("-" * 60)
    raise ValueError("Missing required parameters: practice and file_type")

print(f"\n[INFO] ========== REFINED ETL RUN STARTED ==========")
print(f"[INFO] Parent Run ID: {PARENT_RUN_ID}")
print(f"[INFO] Practice: {PRACTICE_NAME}")
print(f"[INFO] File Type: {FILE_TYPE}")
print(f"[INFO] Using config from: {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME}")
print(f"[INFO] Logging to: {REFINED_LOG_TABLE}")

# ==========================================================
# 2) LOGGING FUNCTION
# ==========================================================
def generate_guid():
    """Generate a new GUID/UUID string."""
    return str(uuid.uuid4())

def clean_error_message(error_msg: str) -> str:
    """Cleans Snowflake error messages by removing prefix containing error codes and query IDs."""
    if not error_msg:
        return error_msg
    pattern = r'^\(\d+\):\s*[a-f0-9-]+:\s*\d+\s*\([A-Z0-9]+\):\s*'
    cleaned = re.sub(pattern, '', error_msg, flags=re.IGNORECASE)
    return cleaned

def log_refined(practice_name: str, file_type: str, source_table: str,
                target_table: str, step_name: str, row_count: int, 
                status: str, start_time: float, end_time: float = None,
                error_message: str = None, columns_transformed: int = None,
                transformations_applied: str = None):
    """
    Logs a refined ingest operation to the REFINED_INGEST_LOG table.
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
        source_table_safe = (source_table or '').replace("'", "''")
        target_table_safe = (target_table or '').replace("'", "''")
        step_name_safe = (step_name or '').replace("'", "''")
        error_message_safe = (error_message or '').replace("'", "''") if error_message else None
        transformations_safe = (transformations_applied or '').replace("'", "''") if transformations_applied else None
        
        error_value = f"'{error_message_safe}'" if error_message_safe else "NULL"
        row_count_value = row_count if row_count is not None else "NULL"
        cols_transformed_value = columns_transformed if columns_transformed is not None else "NULL"
        transformations_value = f"'{transformations_safe}'" if transformations_safe else "NULL"
        raw_run_id_value = f"'{RAW_RUN_ID}'" if RAW_RUN_ID else "NULL"
        
        insert_sql = f"""
            INSERT INTO {REFINED_LOG_TABLE}
            (LOG_ID, PARENT_RUN_ID, RAW_RUN_ID, PRACTICE_NAME, FILE_TYPE, SOURCE_TABLE,
             TARGET_TABLE, STEP_NAME, ROW_COUNT, COLUMNS_TRANSFORMED, STATUS, ERROR_MESSAGE,
             TRANSFORMATIONS_APPLIED, START_TIME, END_TIME, DURATION_SECONDS)
            VALUES (
                '{log_id}',
                '{PARENT_RUN_ID}',
                {raw_run_id_value},
                '{practice_name_safe}',
                '{file_type_safe}',
                '{source_table_safe}',
                '{target_table_safe}',
                '{step_name_safe}',
                {row_count_value},
                {cols_transformed_value},
                '{status}',
                {error_value},
                {transformations_value},
                '{start_ts}',
                '{end_ts}',
                {duration_seconds}
            )
        """
        session.sql(insert_sql).collect()
        
        print(f"[LOG] {step_name}: {practice_name}/{file_type} - {status} ({row_count} rows, {duration_seconds}s)")
        return log_id
    except Exception as e:
        print(f"[WARN] Failed to write to refined log table: {e}")
        return None

# ==========================================================
# 3) Temp FILE FORMAT for JSON
# ==========================================================
session.sql("""
    CREATE TEMPORARY FILE FORMAT IF NOT EXISTS FF_JSON_CFG
    TYPE = JSON
""").collect()

# ==========================================================
# 4) Read Snowflake config for practice/file_type
# ==========================================================
etl_start_time = time.time()

cfg_sql = f"""
    SELECT
      LOWER(p.value:practice_name::string)             AS practice_name,
      f.value:file_type::string                        AS file_type,
      f.value:snowflake:database::string               AS src_db,
      f.value:snowflake:schema::string                 AS src_schema,
      f.value:snowflake:table::string                  AS src_table,
      f.value:snowflake:refined_database::string       AS refined_db,
      f.value:snowflake:refined_schema::string         AS refined_schema,
      f.value:snowflake:refined_table::string          AS refined_table,
      f.value:snowflake:column_regex_replace           AS column_regex_replace,
      f.value:snowflake:column_reformat                AS column_reformat,
      f.value:snowflake:column_strip                   AS column_strip,
      f.value:snowflake:curated_database::string       AS curated_db,
      f.value:snowflake:curated_schema::string         AS curated_schema,
      f.value:snowflake:curated_table::string          AS curated_table
    FROM {CONFIG_STAGE_PREFIX}{CONFIG_FILENAME} (FILE_FORMAT => 'FF_JSON_CFG') AS cfg,
         LATERAL FLATTEN(input => cfg.$1:Practices) p,
         LATERAL FLATTEN(input => p.value:ingest) f
    WHERE LOWER(p.value:practice_name::string) = LOWER('{PRACTICE_NAME}')
      AND LOWER(f.value:file_type::string) = LOWER('{FILE_TYPE}')
"""

cfg_rows = session.sql(cfg_sql).collect()
if not cfg_rows:
    error_msg = f"No config found for practice={PRACTICE_NAME}, file_type={FILE_TYPE}"
    log_refined(PRACTICE_NAME, FILE_TYPE, "N/A", "N/A", "CONFIG_LOAD", 0, "ERROR", 
                etl_start_time, error_message=error_msg)
    raise ValueError(error_msg)

cfg = cfg_rows[0]

src_db  = cfg["SRC_DB"]
src_sch = cfg["SRC_SCHEMA"]
src_tbl = cfg["SRC_TABLE"]

ref_db  = cfg["REFINED_DB"]
ref_sch = cfg["REFINED_SCHEMA"]
ref_tbl = cfg["REFINED_TABLE"]

column_regex_raw = cfg["COLUMN_REGEX_REPLACE"]
column_reformat_raw = cfg["COLUMN_REFORMAT"]
column_strip_raw = cfg["COLUMN_STRIP"]

# Curated config (for triggering next notebook)
curated_db = cfg["CURATED_DB"]
curated_sch = cfg["CURATED_SCHEMA"]
curated_tbl = cfg["CURATED_TABLE"]

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


# Check if refinement config exists
if not ref_db or not ref_sch or not ref_tbl:
    print("[INFO] No refined_database/refined_schema/refined_table configured for this practice/file_type.")
    print("[INFO] Skipping refinement - no transformation needed.")
    log_refined(PRACTICE_NAME, FILE_TYPE, f"{src_db}.{src_sch}.{src_tbl}" if src_db else "N/A", "N/A", 
                "PROCESS_COMPLETE", 0, "SKIPPED", etl_start_time,
                error_message="No refined table configuration")
    print("\n[DONE] Refinement skipped - no target configured.")
else:
    if not src_db or not src_sch or not src_tbl:
        error_msg = "Source Snowflake table configuration is incomplete."
        log_refined(PRACTICE_NAME, FILE_TYPE, "N/A", "N/A", "CONFIG_LOAD", 0, "ERROR",
                    etl_start_time, error_message=error_msg)
        raise ValueError(error_msg)

    source_table = f"{src_db}.{src_sch}.{src_tbl}"
    target_table = f"{ref_db}.{ref_sch}.{ref_tbl}"

    print(f"[INFO] Source table : {source_table}")
    print(f"[INFO] Target table : {target_table}")

    log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "CONFIG_LOAD", 
                0, "SUCCESS", etl_start_time)

    # ==========================================================
    # 4a) Normalize config values to Python list[dict]
    # ==========================================================
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

    column_regex_configs = ensure_list_of_dicts(column_regex_raw)
    column_reformat_configs = ensure_list_of_dicts(column_reformat_raw)
    column_strip_configs = ensure_list_of_dicts(column_strip_raw)

    # Track transformations applied
    transformations_applied = []
    columns_transformed = 0

    # ==========================================================
    # 5) Read source table (filtered by IS_NEW = 1)
    # ==========================================================
    read_start_time = time.time()
    
    # CRITICAL: Only read records that haven't been processed yet (IS_NEW = 1)
    # This prevents duplicate processing on reruns
    print(f"[INFO] Filtering RAW records by IS_NEW = 1")
    df = session.table(source_table).filter(col("IS_NEW") == 1)
    print("[INFO] Initial source sample:")
    df.limit(10).show()

    src_count = df.count()
    print(f"[INFO] Source row count (IS_NEW=1): {src_count}")
    
    if src_count == 0:
        warn_msg = f"No new records found in RAW (IS_NEW=1). Nothing to process."
        print(f"[WARN] {warn_msg}")
        log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "PROCESS_COMPLETE",
                    0, "SKIPPED", read_start_time, error_message=warn_msg)
        print("\n[DONE] No new records to process.")
        # Exit this code path - refinement not needed
        raise SystemExit(0)
    
    log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "READ_SOURCE",
                src_count, "SUCCESS", read_start_time)

    # Build a normalized-name lookup so config can match columns case/format-insensitively
    def _norm_name(name: str) -> str:
        # Remove all non-alphanumeric characters and lower-case
        return "".join(ch for ch in name if ch.isalnum()).lower()

    schema_cols = df.schema.names
    normalized_to_actual = {_norm_name(c): c for c in schema_cols}

    # ==========================================================
    # 6) Apply dynamic regex rules for any configured columns
    # ==========================================================
    transform_start_time = time.time()
    transformed_df = df

    if not column_regex_configs:
        print("[INFO] No column_regex_replace configuration found; no regex-based updates will be applied.")
    else:
        print("[INFO] Applying column_regex_replace rules from config...")

        for col_cfg in column_regex_configs:
            if not isinstance(col_cfg, dict):
                try:
                    col_cfg = json.loads(str(col_cfg))
                except Exception:
                    continue

            configured_name = (col_cfg.get("column") or "").strip()
            rules = col_cfg.get("rules") or []

            if not configured_name:
                continue

            # Resolve to actual column name using normalized lookup
            actual_name = normalized_to_actual.get(_norm_name(configured_name))
            if not actual_name:
                print(f"[WARN] Configured column '{configured_name}' not found in source; skipping.")
                continue

            if not isinstance(rules, list) or not rules:
                print(f"[INFO] No rules defined for column '{configured_name}'; skipping.")
                continue

            base_col = col(actual_name)
            case_builder = None

            for rule in rules:
                if not isinstance(rule, dict):
                    try:
                        rule = json.loads(str(rule))
                    except Exception:
                        continue

                match_sub = (rule.get("match_substring") or "").strip()
                search    = (rule.get("search") or "").strip()
                replace   = (rule.get("replace") or "").strip()

                if not (match_sub and search and replace):
                    continue

                condition = base_col.like(f"%{match_sub}%")
                replacement = regexp_replace(base_col, search, replace)

                if case_builder is None:
                    case_builder = when(condition, replacement)
                else:
                    case_builder = case_builder.when(condition, replacement)

            if case_builder is None:
                print(f"[INFO] No valid rules for column '{configured_name}'; no changes applied to this column.")
                continue

            updated_expr = case_builder.otherwise(base_col)
            transformed_df = transformed_df.with_column(actual_name, updated_expr)
            print(f"[INFO] Applied regex replacement rules to column '{configured_name}' (actual column '{actual_name}').")
            transformations_applied.append(f"regex_replace:{actual_name}")
            columns_transformed += 1

    # ==========================================================
    # 7) Apply column_reformat rules (split/reorder operations)
    # ==========================================================
    if not column_reformat_configs:
        print("[INFO] No column_reformat configuration found; no reformat operations will be applied.")
    else:
        print("[INFO] Applying column_reformat rules from config...")

        for reformat_cfg in column_reformat_configs:
            if not isinstance(reformat_cfg, dict):
                try:
                    reformat_cfg = json.loads(str(reformat_cfg))
                except Exception:
                    continue

            configured_name = (reformat_cfg.get("column") or "").strip()
            reformat_type = (reformat_cfg.get("type") or "").strip()

            if not configured_name:
                continue

            # Resolve to actual column name
            actual_name = normalized_to_actual.get(_norm_name(configured_name))
            if not actual_name:
                print(f"[WARN] Configured column '{configured_name}' not found in source; skipping reformat.")
                continue

            # Currently supporting split_reorder type
            if reformat_type == "split_reorder":
                split_by = reformat_cfg.get("split_by", ",")
                part_order = reformat_cfg.get("part_order", [])
                join_with = reformat_cfg.get("join_with", " ")
                trim_parts = reformat_cfg.get("trim_parts", True)

                if not part_order:
                    print(f"[WARN] No part_order defined for column '{configured_name}'; skipping reformat.")
                    continue

                print(f"[INFO] Applying split_reorder to column '{configured_name}' (actual column '{actual_name}')...")
                print(f"      Split by: '{split_by}', Order: {part_order}, Join with: '{join_with}', Trim: {trim_parts}")

                # Split the column
                base_col = col(actual_name)
                split_col = split(base_col, lit(split_by))

                # Build array of reordered parts
                reordered_parts = []
                for idx in part_order:
                    part = split_col[idx]
                    if trim_parts:
                        part = trim(part)
                    reordered_parts.append(part)

                # Join the reordered parts
                if len(reordered_parts) == 1:
                    reformatted_col = reordered_parts[0]
                else:
                    reformatted_col = concat_ws(lit(join_with), *reordered_parts)

                # Update the column
                transformed_df = transformed_df.with_column(actual_name, reformatted_col)
                print(f"[INFO] Successfully reformatted column '{configured_name}'.")
                transformations_applied.append(f"split_reorder:{actual_name}")
                columns_transformed += 1

            else:
                print(f"[WARN] Unsupported reformat type '{reformat_type}' for column '{configured_name}'; skipping.")

    # ==========================================================
    # 7b) Apply column_strip rules (remove specified characters)
    # ==========================================================
    if not column_strip_configs:
        print("[INFO] No column_strip configuration found; no character stripping will be applied.")
    else:
        print("[INFO] Applying column_strip rules from config...")

        for strip_cfg in column_strip_configs:
            if not isinstance(strip_cfg, dict):
                try:
                    strip_cfg = json.loads(str(strip_cfg))
                except Exception:
                    continue

            configured_name = (strip_cfg.get("column") or "").strip()
            chars_to_strip = (strip_cfg.get("chars") or "").strip()

            if not configured_name:
                continue

            if not chars_to_strip:
                print(f"[INFO] No chars defined for column '{configured_name}'; skipping.")
                continue

            # Resolve to actual column name using normalized lookup
            actual_name = normalized_to_actual.get(_norm_name(configured_name))
            if not actual_name:
                print(f"[WARN] Configured column '{configured_name}' not found in source; skipping strip.")
                continue

            # Build regex pattern to match any of the specified characters
            # Escape special regex characters
            escaped_chars = re.escape(chars_to_strip)
            # Create character class pattern [chars]
            strip_pattern = f"[{escaped_chars}]"

            print(f"[INFO] Stripping characters '{chars_to_strip}' from column '{configured_name}' (actual column '{actual_name}')...")
            print(f"      Using regex pattern: {strip_pattern}")

            # Apply regex_replace to remove the characters
            base_col = col(actual_name)
            stripped_col = regexp_replace(base_col, strip_pattern, "")

            transformed_df = transformed_df.with_column(actual_name, stripped_col)
            print(f"[INFO] Successfully stripped characters from column '{configured_name}'.")
            transformations_applied.append(f"strip_chars:{actual_name}")
            columns_transformed += 1

    # Log transformation step
    transform_end_time = time.time()
    transformations_str = ",".join(transformations_applied) if transformations_applied else "none"
    log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "TRANSFORM",
                src_count, "SUCCESS", transform_start_time, transform_end_time,
                columns_transformed=columns_transformed, transformations_applied=transformations_str)

    # Select columns in original order, excluding IS_NEW (RAW tracking column not needed in REFINED)
    # IMPORTANT: Uppercase all column names to match REFINED table schema
    # RAW table may have mixed-case columns (e.g., FirstName) but REFINED expects uppercase (FIRSTNAME)
    columns_for_refined = [c for c in df.columns if c.upper() != "IS_NEW"]
    df_transformed = transformed_df.select([col(c).alias(c.upper()) for c in columns_for_refined])
    print(f"[INFO] Renamed {len(columns_for_refined)} columns to uppercase for REFINED table compatibility")

    # ==========================================================
    # 8) Preview & row count
    # ==========================================================
    print("[INFO] Sample of transformed data:")
    df_transformed.limit(10).show()

    print("[INFO] Distinct values (sample) for transformed columns:")
    if column_regex_configs:
        for col_cfg in column_regex_configs:
            try:
                configured_name = (col_cfg.get("column") or "").strip() if isinstance(col_cfg, dict) else None
            except Exception:
                try:
                    parsed = json.loads(str(col_cfg))
                    configured_name = (parsed.get("column") or "").strip() if isinstance(parsed, dict) else None
                except Exception:
                    configured_name = None

            if not configured_name:
                continue

            actual_name = normalized_to_actual.get(_norm_name(configured_name))
            if actual_name and actual_name in df_transformed.schema.names:
                print(f"\nDistinct values for '{configured_name}':")
                df_transformed.select(actual_name).distinct().limit(20).show()

    if column_reformat_configs:
        for reformat_cfg in column_reformat_configs:
            try:
                configured_name = (reformat_cfg.get("column") or "").strip() if isinstance(reformat_cfg, dict) else None
            except Exception:
                try:
                    parsed = json.loads(str(reformat_cfg))
                    configured_name = (parsed.get("column") or "").strip() if isinstance(parsed, dict) else None
                except Exception:
                    configured_name = None

            if not configured_name:
                continue

            actual_name = normalized_to_actual.get(_norm_name(configured_name))
            if actual_name and actual_name in df_transformed.schema.names:
                print(f"\nSample values for reformatted column '{configured_name}':")
                df_transformed.select(actual_name).limit(20).show()

    if column_strip_configs:
        for strip_cfg in column_strip_configs:
            try:
                configured_name = (strip_cfg.get("column") or "").strip() if isinstance(strip_cfg, dict) else None
            except Exception:
                try:
                    parsed = json.loads(str(strip_cfg))
                    configured_name = (parsed.get("column") or "").strip() if isinstance(parsed, dict) else None
                except Exception:
                    configured_name = None

            if not configured_name:
                continue

            actual_name = normalized_to_actual.get(_norm_name(configured_name))
            if actual_name and actual_name in df_transformed.schema.names:
                print(f"\nSample values for stripped column '{configured_name}':")
                df_transformed.select(actual_name).limit(20).show()

    row_count = df_transformed.count()
    print(f"[INFO] Rows prepared for write (after transformation): {row_count}")

    # ==========================================================
    # 9) Add tracking columns for curated processing
    # ==========================================================
    # IS_VALID = 1: Indicates record is ready for curated processing
    # REFINED_PARENT_RUN_ID: Links records to this refinement run for tracking
    print("[INFO] Adding tracking columns: IS_VALID=1, REFINED_PARENT_RUN_ID")
    df_transformed = (
        df_transformed
        .withColumn("IS_VALID", lit(1))
        .withColumn("REFINED_PARENT_RUN_ID", lit(PARENT_RUN_ID))
    )

    # ==========================================================
    # 10) Write to target table (append if exists, else create)
    # ==========================================================
    write_start_time = time.time()
    
    exists_sql = f"""
    SELECT COUNT(*) AS C
    FROM {ref_db}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{ref_sch}'
      AND TABLE_NAME = '{ref_tbl}'
    """
    exists = session.sql(exists_sql).collect()[0]["C"] > 0

    try:
        if exists:
            print(f"[INFO] Target {target_table} exists. Appending rows...")
            df_transformed.write.save_as_table(target_table, mode="append")
        else:
            print(f"[INFO] Target {target_table} does not exist. Creating and writing rows...")
            df_transformed.write.save_as_table(target_table)
        
        write_end_time = time.time()
        log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "WRITE_TARGET",
                    row_count, "SUCCESS", write_start_time, write_end_time,
                    columns_transformed=columns_transformed)
        
        # ==========================================================
        # 10a) Mark processed records in RAW as IS_NEW = 0
        # ==========================================================
        # This prevents reprocessing on future runs
        print(f"\n[INFO] Marking {row_count} records as processed in RAW (IS_NEW = 0)...")
        update_start_time = time.time()
        update_sql = f"UPDATE {source_table} SET IS_NEW = 0 WHERE IS_NEW = 1"
        session.sql(update_sql).collect()
        update_end_time = time.time()
        update_duration = round(update_end_time - update_start_time, 3)
        print(f"[SUCCESS] Marked records as processed in {update_duration}s")
        
        log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "MARK_RAW_PROCESSED",
                    row_count, "SUCCESS", update_start_time, update_end_time)
        
    except Exception as e:
        write_end_time = time.time()
        error_msg = clean_error_message(str(e))[:500]
        log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "WRITE_TARGET",
                    0, "ERROR", write_start_time, write_end_time, error_message=error_msg)
        raise

    # ==========================================================
    # 11) Verify
    # ==========================================================
    print("[INFO] Verifying write...")
    session.sql(f"SELECT COUNT(*) AS ROWS_IN_TARGET FROM {target_table}").show()
    session.sql(f"SELECT * FROM {target_table} LIMIT 10").show()

    # ==========================================================
    # 12) Final log entry
    # ==========================================================
    etl_end_time = time.time()
    etl_duration = round(etl_end_time - etl_start_time, 3)
    
    log_refined(PRACTICE_NAME, FILE_TYPE, source_table, target_table, "PROCESS_COMPLETE",
                row_count, "SUCCESS", etl_start_time, etl_end_time,
                columns_transformed=columns_transformed, transformations_applied=transformations_str)

    print("\n" + "-" * 60)
    print("[SUMMARY]")
    print(f"  Parent Run ID        : {PARENT_RUN_ID}")
    print(f"  Tracking Source      : {'Upstream (end-to-end)' if RAW_RUN_ID else 'Standalone run'}")
    print(f"  Practice             : {PRACTICE_NAME}")
    print(f"  File Type            : {FILE_TYPE}")
    print(f"  Source Table         : {source_table}")
    print(f"  Target Table         : {target_table}")
    print(f"  Rows Processed       : {row_count}")
    print(f"  Columns Transformed  : {columns_transformed}")
    print(f"  Transformations      : {transformations_str}")
    print(f"  Duration             : {etl_duration} seconds")
    print("-" * 60)

    print("\n[DONE] Column transformation complete.")

    # ==========================================================
    # 13) Trigger Curated Notebook (only if curated config is present)
    # ==========================================================
    curated_configured = all(_is_configured_value(v) for v in (curated_db, curated_sch, curated_tbl))

    if curated_configured:
        curated_target_table = f"{curated_db}.{curated_sch}.{curated_tbl}"
        print(f"\n[INFO] Curated table configured: {curated_target_table}")
        print(f"[INFO] Triggering REFINED_SNOWFLAKE_TO_CURATED_SNOWFLAKE notebook...")

        curated_notebook = "UPPERLINE.RAW_INBOUND_AFFILIATEDATA.REFINED_SNOWFLAKE_TO_CURATED_SNOWFLAKE"
        curated_params = f"{PRACTICE_NAME}|{FILE_TYPE}|{PARENT_RUN_ID}"

        try:
            curated_start_time = time.time()
            
            # ==========================================================
            # v2 CHANGE: Set 3-hour timeout for curated notebook execution
            # ==========================================================
            session.sql("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 10800").collect()
            print("[INFO] Set session timeout to 3 hours (10800 seconds)")
            
            # exec_sql = f"EXECUTE NOTEBOOK {curated_notebook}('{curated_params}')"
            exec_sql = f"CALL UPPERLINE.RAW_INBOUND_AFFILIATEDATA.SP_EXEC_CURATED_NOTEBOOK('{curated_params}')"
            print(f"[INFO] Executing: {exec_sql}")
            session.sql(exec_sql).collect()
            curated_end_time = time.time()
            curated_duration = round(curated_end_time - curated_start_time, 3)

            print(f"[SUCCESS] Curated notebook completed in {curated_duration}s")
            log_refined(PRACTICE_NAME, FILE_TYPE, target_table, curated_target_table,
                        "TRIGGER_CURATED", row_count, "SUCCESS", curated_start_time, curated_end_time)
        except Exception as e:
            curated_end_time = time.time()
            error_msg = clean_error_message(str(e))[:500]
            print(f"[ERROR] Curated notebook failed: {error_msg}")
            log_refined(PRACTICE_NAME, FILE_TYPE, target_table, curated_target_table,
                        "TRIGGER_CURATED", 0, "ERROR", curated_start_time, curated_end_time, error_message=error_msg)
            # Don't raise - refined completed successfully, curated failure is logged but shouldn't fail the run
            print("[WARN] Curated notebook failed but refined processing completed successfully.")
    else:
        missing_fields = []
        if not _is_configured_value(curated_db):
            missing_fields.append("curated_database")
        if not _is_configured_value(curated_sch):
            missing_fields.append("curated_schema")
        if not _is_configured_value(curated_tbl):
            missing_fields.append("curated_table")

        if missing_fields:
            skip_reason = f"Missing or empty curated config fields: {', '.join(missing_fields)}"
        else:
            skip_reason = "Curated config not present"

        print(f"\n[INFO] {skip_reason} - skipping curated notebook trigger.")

        # Log the skip so it is visible in REFINED_INGEST_LOG
        curated_skip_time = time.time()
        log_refined(PRACTICE_NAME, FILE_TYPE, target_table, "N/A",
                    "TRIGGER_CURATED", row_count, "SKIPPED",
                    curated_skip_time, curated_skip_time, error_message=skip_reason)
