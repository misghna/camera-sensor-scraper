import os, time, logging, random, json, signal, argparse
from typing import Optional, List, Dict, Any
from collections import defaultdict

from project_documents_handler import ProjectDocumentsHandler
from opportunities_crud import OpportunitiesCRUD
from bid_doc_parser import S3OpenAIProcessor

# Config
PDF_MAX_CHARS        = int(os.getenv("PDF_MAX_CHARS", "500000"))
BATCH_SIZE_DEFAULT   = int(os.getenv("BATCH_SIZE", "100"))
COOLDOWN_MIN_DEFAULT = int(os.getenv("COOLDOWN_MIN", "3"))
COOLDOWN_MAX_DEFAULT = int(os.getenv("COOLDOWN_MAX", "8"))
RETRY_WAIT           = int(os.getenv("RETRY_WAIT", "60"))
DOC_RETRY_ATTEMPTS   = int(os.getenv("DOC_RETRY_ATTEMPTS", "2"))
BURST_PAUSE_EVERY    = int(os.getenv("BURST_PAUSE_EVERY", "15"))
BURST_PAUSE_SECONDS  = int(os.getenv("BURST_PAUSE_SECONDS", "20"))
DEFAULT_S3_BUCKET    = os.getenv("DEFAULT_S3_BUCKET", "bid-docs-h2g")
DEFAULT_S3_PREFIX    = os.getenv("DEFAULT_S3_PREFIX", "all/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Helper functions (keep existing ones)
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
    if not s3_path:
        return False
    s = str(s3_path).strip().lower()
    invalid = {"na", "n/a", "none", "null", "-", "--"}
    if s in invalid:
        return False
    return s.endswith(".pdf") or s.startswith("s3://")

# ✅ NEW: AI-based merge function
def merge_opportunities_with_ai(processor, all_opportunities, project_id):
    """
    Use OpenAI with merge prompt to intelligently merge opportunities from multiple documents.
    """
    if not all_opportunities:
        logger.info(f"[MERGE] No opportunities to merge for project {project_id}")
        return []
    
    if len(all_opportunities) == 1:
        logger.info(f"[MERGE] Only 1 opportunity, no merge needed for project {project_id}")
        return all_opportunities
    
    # Load the merge prompt
    merge_prompt = processor._load_merge_prompt_local()
    if not merge_prompt:
        logger.warning(f"[MERGE] No merge prompt found, returning opportunities as-is")
        return all_opportunities
    
    # Format all opportunities as JSON for the AI
    opportunities_json = {
        "instrumentation_opportunities": all_opportunities
    }
    opportunities_str = json.dumps(opportunities_json, indent=2)
    
    # Construct the message for OpenAI
    combined_message = f"{merge_prompt}\n\n--- OPPORTUNITIES TO MERGE ---\n{opportunities_str}\n--- END OPPORTUNITIES ---"
    
    messages = [{"role": "user", "content": combined_message}]
    
    logger.info(f"[MERGE] Sending {len(all_opportunities)} opportunities to OpenAI for intelligent merging...")
    logger.info(f"[MERGE] Total characters being sent: {len(combined_message)}")
    
    try:
        # Send to OpenAI
        response = processor._chat_complete(messages)
        
        # Parse the response
        content = response.choices[0].message.content if response and response.choices else ""
        if not content:
            logger.warning("[MERGE] Empty response from OpenAI, returning original opportunities")
            return all_opportunities
        
        # Parse JSON response
        merged_result = json.loads(content)
        merged_opps = merged_result.get("instrumentation_opportunities", [])
        
        logger.info(f"[MERGE] ✅ AI merged {len(all_opportunities)} → {len(merged_opps)} opportunities")
        return merged_opps
        
    except Exception as e:
        logger.error(f"[MERGE] Error during AI merge: {e}")
        logger.warning("[MERGE] Falling back to original opportunities")
        return all_opportunities

# Graceful exit
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
    """Process documents with grouping and AI-based merging"""
    global _stop

    crud      = OpportunitiesCRUD()
    handler   = ProjectDocumentsHandler()
    processor = S3OpenAIProcessor(require_prompt_file=True, pdf_max_chars=PDF_MAX_CHARS, aws_profile="java-default")

    try:
        existing = crud.get_existing_project_ids()
        logger.info(f"📊 Found {len(existing)} existing project_ids in opportunities table")
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

        logger.info(f"\n{'='*80}")
        logger.info(f"📦 BATCH #{batch_num} - Fetched {len(docs)} documents at offset={offset}")
        logger.info(f"{'='*80}")

        try:
            existing = crud.get_existing_project_ids()
            logger.info(f"🔄 Refreshed existing project_ids: {len(existing)} projects already processed")
        except Exception as e:
            logger.warning(f"get_existing_project_ids failed mid-run: {e}. Using empty set for this batch.")
            existing = set()

        new_docs = [
            d for d in docs
            if _is_valid_s3_path(d.get("s3_path"))
            and d.get("project_id") not in existing
        ]
        
        logger.info(f"📋 After filtering: {len(new_docs)} new documents to process (removed {len(docs) - len(new_docs)} already processed/invalid)")
        
        if not new_docs:
            logger.info(f"⏭️ All {len(docs)} docs already processed. Skipping batch #{batch_num}.")
            offset += batch_size
            batch_num += 1
            continue

        # Group documents by project_id
        projects_dict = defaultdict(list)
        for doc in new_docs:
            projects_dict[doc["project_id"]].append(doc)
        
        logger.info(f"\n🗂️ GROUPING SUMMARY:")
        logger.info(f"   Total documents: {len(new_docs)}")
        logger.info(f"   Unique projects: {len(projects_dict)}")
        for pid, docs_list in sorted(projects_dict.items()):
            logger.info(f"   └─ Project {pid}: {len(docs_list)} document(s)")

        failed_projects = {}
        
        for project_num, (pid, project_docs) in enumerate(projects_dict.items(), 1):
            if _stop:
                break
            if max_projects is not None and processed_projects >= max_projects:
                logger.info(f"⏹️ Reached max_projects={max_projects}; stopping.")
                _stop = True
                break

            logger.info(f"\n{'─'*80}")
            logger.info(f"********* PROJECT {project_num}/{len(projects_dict)}: ID={pid} *******")
            logger.info(f"   Documents to process: {len(project_docs)}")
            logger.info(f"{'─'*80}")

            if processed_projects > 0 and processed_projects % BURST_PAUSE_EVERY == 0:
                logger.info(f"⏸️ Burst pause {BURST_PAUSE_SECONDS}s after {processed_projects} projects…")
                time.sleep(BURST_PAUSE_SECONDS)

            all_opportunities_for_project = []
            failed_docs_in_project = []

            for doc_idx, doc in enumerate(project_docs, 1):
                s3_path = doc["s3_path"]
                logger.info(f"\n   📄 Document {doc_idx}/{len(project_docs)}")
                logger.info(f"      S3 Path: {s3_path}")

                try:
                    bucket_name, key = _resolve_s3_path(s3_path)
                    logger.info(f"      Bucket: {bucket_name}, Key: {key}")

                    last_err = None
                    for attempt in range(1, DOC_RETRY_ATTEMPTS + 1):
                        try:
                            logger.info(f"      🤖 Attempt {attempt}/{DOC_RETRY_ATTEMPTS}: Sending to OpenAI...")
                            ai_response = processor.process_s3_file(bucket_name, key)
                            summary     = _parse_ai_summary(ai_response)
                            logger.info(f"      ✅ OpenAI processing successful")
                            break
                        except Exception as e:
                            last_err = e
                            logger.warning(f"      ⚠️ Attempt {attempt}/{DOC_RETRY_ATTEMPTS} failed: {e}")
                            if attempt < DOC_RETRY_ATTEMPTS:
                                wait_time = 5 * attempt + random.uniform(0, 1.5)
                                logger.info(f"      ⏳ Waiting {wait_time:.1f}s before retry...")
                                time.sleep(wait_time)
                    else:
                        raise last_err if last_err else RuntimeError("Unknown processing error")

                    doc_opps = list(_iter_opportunities(summary))
                    all_opportunities_for_project.extend(doc_opps)
                    logger.info(f"      📊 Extracted {len(doc_opps)} opportunity(ies) from this document")

                    if doc_idx < len(project_docs):
                        sleep_time = random.randint(COOLDOWN_MIN_DEFAULT, COOLDOWN_MAX_DEFAULT)
                        logger.info(f"      😴 Cooling down {sleep_time}s before next document...")
                        time.sleep(sleep_time)

                except Exception as e:
                    logger.error(f"      ❌ ERROR processing document {doc_idx}: {e}")
                    failed_docs_in_project.append(doc)
                    failed_total += 1

            if failed_docs_in_project:
                failed_projects[pid] = failed_docs_in_project
                logger.info(f"\n   ⚠️ {len(failed_docs_in_project)} document(s) failed for this project")

            # ✅ Use AI to merge opportunities
            logger.info(f"\n   📊 OPPORTUNITIES SUMMARY for Project {pid}:")
            logger.info(f"      Total opportunities collected: {len(all_opportunities_for_project)}")

            if all_opportunities_for_project:
                # AI-based intelligent merge
                merged_opps = merge_opportunities_with_ai(
                    processor=processor,
                    all_opportunities=all_opportunities_for_project,
                    project_id=pid
                )
                
                logger.info(f"      After AI merge: {len(merged_opps)} unique opportunities")

                opps_inserted = 0
                for opp in merged_opps:
                    row = map_ai_opportunity_to_row(pid, opp)
                    logger.info(f"      💾 Inserting opportunity: {row.get('job_code')} - {row.get('job_description')[:50]}...")
                    ok  = crud.insert_opportunity(row)
                    if ok:
                        inserted_opportunities_total += 1
                        opps_inserted += 1
                    else:
                        logger.error(f"      ❌ Insert failed for job_code={row.get('job_code')}")

                logger.info(f"   ✅ Successfully inserted {opps_inserted}/{len(merged_opps)} opportunities")
            else:
                logger.info(f"   ℹ️ No opportunities found across all {len(project_docs)} document(s)")

            processed_projects += 1
            logger.info(f" ###### Project {pid} completed. Total projects processed so far: {processed_projects} #####")

        # Retry logic (similar pattern with AI merge)
        if not _stop and failed_projects:
            total_failed_docs = sum(len(docs) for docs in failed_projects.values())
            logger.info(f"\n{'='*80}")
            logger.info(f"🔄 RETRY PHASE - {total_failed_docs} docs across {len(failed_projects)} projects")
            logger.info(f"   Waiting {RETRY_WAIT}s...")
            logger.info(f"{'='*80}")
            time.sleep(RETRY_WAIT)

            for retry_num, (pid, failed_docs) in enumerate(failed_projects.items(), 1):
                if _stop or (max_projects is not None and processed_projects >= max_projects):
                    break

                logger.info(f"\n🔄 RETRY {retry_num}/{len(failed_projects)}: Project {pid} ({len(failed_docs)} docs)")
                
                retry_opportunities = []
                for doc_idx, doc in enumerate(failed_docs, 1):
                    logger.info(f"   📄 Retry document {doc_idx}/{len(failed_docs)}: {doc['s3_path']}")
                    try:
                        bucket_name, key = _resolve_s3_path(doc["s3_path"])
                        ai_response = processor.process_s3_file(bucket_name, key)
                        summary     = _parse_ai_summary(ai_response)

                        doc_opps = list(_iter_opportunities(summary))
                        retry_opportunities.extend(doc_opps)
                        logger.info(f"      ✅ Retry successful: {len(doc_opps)} opportunity(ies)")

                    except Exception as e:
                        logger.error(f"      ❌ Final retry failed: {e}")

                if retry_opportunities:
                    # AI merge for retry opportunities too
                    merged_opps = merge_opportunities_with_ai(
                        processor=processor,
                        all_opportunities=retry_opportunities,
                        project_id=pid
                    )
                    
                    opps_inserted = 0
                    for opp in merged_opps:
                        row = map_ai_opportunity_to_row(pid, opp)
                        ok  = crud.insert_opportunity(row)
                        if ok:
                            inserted_opportunities_total += 1
                            opps_inserted += 1

                    logger.info(f"   ✅ Retry inserted {opps_inserted} opportunities for project {pid}")

        offset   += batch_size
        batch_num += 1

    logger.info(f"\n{'='*80}")
    logger.info(f"🎉 PROCESSING COMPLETE")
    logger.info(f"   Total opportunities inserted: {inserted_opportunities_total}")
    logger.info(f"   Total projects processed: {processed_projects}")
    logger.info(f"   Failed documents (initial pass): {failed_total}")
    logger.info(f"{'='*80}")

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