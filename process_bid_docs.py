import os, time, logging, random, json, signal, argparse
from typing import Optional, List, Dict, Any

from project_documents_handler import ProjectDocumentsHandler
from opportunities_crud import OpportunitiesCRUD
from bid_doc_parser import S3OpenAIProcessor

# ------------------------------------------------------------
# Config (env overrides)
# ------------------------------------------------------------
PDF_MAX_CHARS        = int(os.getenv("PDF_MAX_CHARS", "60000"))
BATCH_SIZE_DEFAULT   = int(os.getenv("BATCH_SIZE", "100"))
COOLDOWN_MIN_DEFAULT = int(os.getenv("COOLDOWN_MIN", "3"))
COOLDOWN_MAX_DEFAULT = int(os.getenv("COOLDOWN_MAX", "8"))
RETRY_WAIT           = int(os.getenv("RETRY_WAIT", "60"))
DOC_RETRY_ATTEMPTS   = int(os.getenv("DOC_RETRY_ATTEMPTS", "2"))
BURST_PAUSE_EVERY    = int(os.getenv("BURST_PAUSE_EVERY", "15"))
BURST_PAUSE_SECONDS  = int(os.getenv("BURST_PAUSE_SECONDS", "20"))
DEFAULT_S3_BUCKET    = os.getenv("DEFAULT_S3_BUCKET", "bid-docs-h2g")
DEFAULT_S3_PREFIX    = os.getenv("DEFAULT_S3_PREFIX", "all/")

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Mapping helpers (AI JSON -> DB row)
# ------------------------------------------------------------
def _clamp_int(v, lo=0, hi=255) -> Optional[int]:
    try:
        return max(lo, min(int(v), hi))
    except Exception:
        return None

def _strip_or_none(v: Optional[str]) -> Optional[str]:
    return v.strip() if isinstance(v, str) and v.strip() else None

def _one_of(value: Optional[str], allowed: List[str], default: str) -> str:
    if not value:
        return default
    v = value.strip().lower()
    for a in allowed:
        if v == a.lower():
            return a
    return default

def _truncate(v: Optional[str], max_len: int) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s if len(s) <= max_len else s[:max_len]

_JOB_SIZE_ALLOWED     = ["small", "medium", "big", "very big"]
_TECH_COMPLEX_ALLOWED = ["low", "medium", "high", "specialized", "Not specified"]

def _normalize_job_size(v: Optional[str]) -> str:
    return _one_of(v, _JOB_SIZE_ALLOWED, "small")

def _normalize_technical_complexity(v: Optional[str]) -> str:
    if not v:
        return "Not specified"
    if v.strip().lower() == "not specified":
        return "Not specified"
    return _one_of(v, _TECH_COMPLEX_ALLOWED, "Not specified")

def _normalize_contract_value_range(v: Optional[str]) -> str:
    if not v:
        return "Not specified"
    s = v.strip().lower()
    if "not specified" in s:
        return "Not specified"
    if "small" in s:
        return "small (<$500K)"
    if "medium" in s:
        return "medium ($500K-$5M)"
    if "large" in s and ">$" not in s:
        return "medium ($500K-$5M)"
    if "mega" in s:
        if "50m" in s or ">$50" in s:
            return "mega (>$50M)"
        return "large ($5M-$50M)"
    if ">$5m" in s or "$5m" in s or "5m" in s:
        return "large ($5M-$50M)"
    return "Not specified"

def _iter_opportunities(parsed_json: Dict[str, Any]):
    try:
        opps = (parsed_json or {}).get("instrumentation_opportunities", [])
        for o in opps or []:
            if isinstance(o, dict):
                yield o
    except Exception as e:
        logger.warning(f"iter opportunities failed: {e}")

def map_ai_opportunity_to_row(project_id: int, opp: Dict[str, Any]) -> Dict[str, Any]:
    job_code = opp.get("job_code") or ""
    freq = _strip_or_none(opp.get("monitoring_frequency"))
    subm = _strip_or_none(opp.get("submission_deadline"))
    loc  = _strip_or_none(opp.get("project_location"))
    cdur = _strip_or_none(opp.get("contract_duration"))

    return {
        "project_id": project_id,
        "job_code": _truncate(str(job_code), 10) if job_code else str(project_id)[:10],
        "job_description": opp.get("job_description", "") or "",
        "job_summary": opp.get("job_summary", "") or "",
        "job_size": _normalize_job_size(opp.get("job_size")),
        "project_type": opp.get("project_type", "General") or "General",
        "frequency": _truncate(freq or "Not specified", 255),
        "match_confidence": _clamp_int(opp.get("match_confidence"), 0, 100),
        "contract_value_range": _normalize_contract_value_range(
            opp.get("contract_value_range", "Not specified")
        ),
        "submission_deadline": _truncate(subm or "Not specified", 255),
        "licensing_requirements": opp.get("licensing_requirements"),
        "technical_complexity": _normalize_technical_complexity(
            opp.get("technical_complexity", "Not specified")
        ),
        "project_location": _truncate(loc or "Not specified", 255),
        "contract_duration": _truncate(cdur or "Not specified", 255),
        "insurance_requirements": opp.get("insurance_requirements"),
        "equipment_specifications": opp.get("equipment_needed"),
        "compliance_standards": opp.get("compliance_standards"),
        "reporting_requirements": opp.get("reporting_requirements"),
    }

# ------------------------------------------------------------
# Core Production Pipeline
# ------------------------------------------------------------
_stop = False
def _graceful_exit(signum, frame):
    global _stop
    _stop = True
    logger.info("🛑 Received stop signal; finishing current item then exiting…")

for sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(sig, _graceful_exit)
    except Exception:
        pass

def process_bid_documents(batch_size: int, start_offset: int, max_projects: Optional[int]):
    global _stop

    crud      = OpportunitiesCRUD()
    handler   = ProjectDocumentsHandler()
    processor = S3OpenAIProcessor(require_prompt_file=True, pdf_max_chars=PDF_MAX_CHARS)

    # Best-effort warm cache of existing IDs
    try:
        existing = crud.get_existing_project_ids()
    except Exception as e:
        logger.warning(f"get_existing_project_ids failed: {e}. Continuing with empty set.")
        existing = set()

    offset                       = start_offset
    batch_num                    = 1
    inserted_opportunities_total = 0
    failed_total                 = 0
    processed_projects           = 0

    while not _stop:
        docs = handler.fetch_bid_documents_batch(limit=batch_size, offset=offset)
        if not docs:
            logger.info("✅ No more documents to process. Done.")
            break

        if max_projects is not None and processed_projects >= max_projects:
            logger.info(f"⏹️ Reached max_projects={max_projects}; stopping.")
            break

        logger.info(f"📦 Processing batch #{batch_num} ({len(docs)} docs) at offset={offset}…")

        # Refresh the existing set each batch
        try:
            existing = crud.get_existing_project_ids()
        except Exception as e:
            logger.warning(f"get_existing_project_ids failed mid-run: {e}. Using empty set for this batch.")
            existing = set()

        # Only process projects not already in opportunities and with an s3_path
        new_docs = [
    d for d in docs
    if _is_valid_s3_path(d.get("s3_path"))
    and d.get("project_id") not in existing
]
        if not new_docs:
            logger.info(f"All {len(docs)} docs already processed. Skipping batch #{batch_num}.")
            offset += batch_size
            batch_num += 1
            continue

        failed_docs = []
        for doc in new_docs:
            if _stop:
                break
            if max_projects is not None and processed_projects >= max_projects:
                logger.info(f"⏹️ Reached max_projects={max_projects}; stopping before project_id={doc['project_id']}.")
                _stop = True
                break

            pid     = doc["project_id"]
            s3_path = doc["s3_path"]

            # Gentle pacing by projects
            if processed_projects > 0 and processed_projects % BURST_PAUSE_EVERY == 0:
                logger.info(f"⏸️ Burst pause {BURST_PAUSE_SECONDS}s after {processed_projects} projects…")
                time.sleep(BURST_PAUSE_SECONDS)

            try:
                bucket_name, key = _resolve_s3_path(s3_path)

                # Per-doc retry around S3/AI work
                last_err = None
                for attempt in range(1, DOC_RETRY_ATTEMPTS + 1):
                    try:
                        ai_response = processor.process_s3_file(bucket_name, key)
                        summary     = _parse_ai_summary(ai_response)
                        break
                    except Exception as e:
                        last_err = e
                        logger.warning(f"Attempt {attempt}/{DOC_RETRY_ATTEMPTS} failed for project_id={pid}: {e}")
                        if attempt < DOC_RETRY_ATTEMPTS:
                            time.sleep(5 * attempt + random.uniform(0, 1.5))
                else:
                    raise last_err if last_err else RuntimeError("Unknown processing error")

                # Insert all opportunities for this project (CRUD opens/closes its own connection)
                opps_inserted = 0
                for opp in _iter_opportunities(summary):
                    row = map_ai_opportunity_to_row(pid, opp)
                    ok  = crud.insert_opportunity(row)  # ✅ no ensure/connect here
                    if ok:
                        inserted_opportunities_total += 1
                        opps_inserted += 1
                    else:
                        logger.error(f"Insert failed for project_id={pid} (job_code={row.get('job_code')})")

                if opps_inserted == 0:
                    logger.info(f"ℹ️ No opportunities parsed for project_id={pid}; skipping insert.")
                else:
                    logger.info(f"✅ Inserted {opps_inserted} opportunities for project_id={pid}")

                processed_projects += 1

                # Cooldown between OpenAI calls
                sleep_time = random.randint(COOLDOWN_MIN_DEFAULT, COOLDOWN_MAX_DEFAULT)
                logger.info(f"😴 Cooling down {sleep_time}s to respect API limits…")
                time.sleep(sleep_time)

            except Exception as e:
                failed_total += 1
                logger.error(f"❌ Error processing project_id={pid}: {e}")
                failed_docs.append(doc)

        # Single retry pass for failed docs
        if not _stop and failed_docs:
            logger.info(f"⏳ Retrying {len(failed_docs)} failed docs after {RETRY_WAIT}s cooldown…")
            time.sleep(RETRY_WAIT)

            for doc in failed_docs:
                if _stop:
                    break
                if max_projects is not None and processed_projects >= max_projects:
                    logger.info(f"⏹️ Reached max_projects={max_projects}; stopping before retry project_id={doc['project_id']}.")
                    _stop = True
                    break

                pid = doc["project_id"]
                try:
                    bucket_name, key = _resolve_s3_path(doc["s3_path"])
                    ai_response = processor.process_s3_file(bucket_name, key)
                    summary     = _parse_ai_summary(ai_response)

                    opps_inserted = 0
                    for opp in _iter_opportunities(summary):
                        row = map_ai_opportunity_to_row(pid, opp)
                        ok  = crud.insert_opportunity(row)  # ✅ same one-shot pattern
                        if ok:
                            inserted_opportunities_total += 1
                            opps_inserted += 1
                        else:
                            logger.error(f"Retry insert failed for project_id={pid} (job_code={row.get('job_code')})")

                    if opps_inserted == 0:
                        logger.info(f"ℹ️ No opportunities parsed on retry for project_id={pid}; skipping.")
                    else:
                        logger.info(f"✅ Retry inserted {opps_inserted} opportunities for project_id={pid}")

                    processed_projects += 1

                except Exception as e:
                    logger.error(f"Final retry failed for project_id={pid}: {e}")

        offset   += batch_size
        batch_num += 1

    logger.info(
        f"🎉 Done. Inserted opportunities: {inserted_opportunities_total}. "
        f"Processed projects: {processed_projects}. Failed docs (initial pass): {failed_total}."
    )

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _parse_s3_path(s3_path: str):
    path  = (s3_path or "").replace("s3://", "", 1)
    parts = path.split("/", 1)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 path: {s3_path}")
    return parts[0], parts[1]

def _resolve_s3_path(s3_path: str):
    s = (s3_path or "").strip()
    if not s:
        raise ValueError("Empty s3_path")
    if s.lower().startswith("s3://"):
        return _parse_s3_path(s)
    prefix = (DEFAULT_S3_PREFIX or "").strip("/")
    key    = s.lstrip("/")
    key    = f"{prefix}/{key}" if prefix else key
    return DEFAULT_S3_BUCKET, key

def _parse_ai_summary(response):
    try:
        content = response.choices[0].message.content if response and response.choices else ""
        return json.loads(content) if content else {"instrumentation_opportunities": []}
    except Exception as e:
        logger.warning(f"AI JSON parse failed: {e}")
        return {"instrumentation_opportunities": []} 
def _is_valid_s3_path(s3_path):
    """Return False for placeholder or obviously invalid S3 keys like 'NA'."""
    if not s3_path:
        return False
    s = str(s3_path).strip().lower()
    # Common placeholders we’ve seen in data
    invalid = {"na", "n/a", "none", "null", "-", "--"}
    if s in invalid:
        return False
    # Require that it looks like a real PDF key
    return s.endswith(".pdf") or s.startswith("s3://")

# ------------------------------------------------------------
# Entry Point (project-based limit only)
# ------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Process bid documents in batches.")
    ap.add_argument("--offset", type=int, default=int(os.getenv("START_OFFSET", "0")),
                    help="Starting offset into bid_documents (default: 0)")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT,
                    help=f"Batch size per DB page (default: {BATCH_SIZE_DEFAULT})")
    ap.add_argument("--max-projects", type=int, default=None,
                    help="Stop after processing this many projects (not opportunities)")

    args = ap.parse_args()

    if COOLDOWN_MIN_DEFAULT < 0:
        COOLDOWN_MIN_DEFAULT = 0
    if COOLDOWN_MAX_DEFAULT < COOLDOWN_MIN_DEFAULT:
        COOLDOWN_MAX_DEFAULT = COOLDOWN_MIN_DEFAULT

    process_bid_documents(
        batch_size=args.batch_size,
        start_offset=args.offset,
        max_projects=args.max_projects
    )
