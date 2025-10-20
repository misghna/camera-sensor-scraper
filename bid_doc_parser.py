import os
import io
import json
import logging
import configparser
from typing import Optional, List, Dict, Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from PyPDF2 import PdfReader, PdfWriter
from openai import OpenAI

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('pdf_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Path helpers ----------
def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))

# ---------- Credentials ----------
def load_openai_key(ini_path: str = "credentials.ini") -> str:
    if not os.path.isabs(ini_path):
        ini_path = os.path.join(_script_dir(), ini_path)

    cfg = configparser.ConfigParser()
    if not cfg.read(ini_path):
        raise RuntimeError(f"Missing or unreadable credentials file: {ini_path}")
    try:
        return cfg["credentials"]["openAI_key"].strip()
    except KeyError:
        raise RuntimeError(f"'openAI_key' not found under [credentials] in {ini_path}")

# ---------- Debug helper (unchanged) ----------
def extract_and_print_content(response):
    """Extract content from OpenAI response and print as formatted JSON, or error info"""
    try:
        if hasattr(response, 'choices') and response.choices:
            content = response.choices[0].message.content
            if content and content.strip():
                try:
                    parsed_json = json.loads(content)
                    print(json.dumps(parsed_json, indent=2))
                except json.JSONDecodeError:
                    print(content)
            else:
                print(json.dumps({
                    "error": "Empty response content",
                    "finish_reason": response.choices[0].finish_reason if response.choices else None,
                    "usage": response.usage.model_dump() if hasattr(response, 'usage') else None
                }, indent=2))
        else:
            print(json.dumps({"error": "No choices in response"}, indent=2))
    except Exception as e:
        print(json.dumps({"error": f"Failed to process response: {str(e)}"}, indent=2))

# ---------- Processor ----------
class S3OpenAIProcessor:
    def __init__(
        self,
        region: str = "us-east-1",
        openai_model: str = "gpt-5-mini",
        aws_profile: Optional[str] = None,
        credentials_ini: str = "credentials.ini",
        reasoning_effort: Optional[str] = None,
        verbosity: Optional[str] = None,
        max_completion_tokens: int = 8000,
        prompt_filename: str = "bid_spec_prompt.txt",
        merge_prompt_filename: str = "spec_merge_prompt.txt",
        alt_prompt_filenames: Optional[List[str]] = None,
        require_prompt_file: bool = True,
        pdf_max_chars: Optional[int] = None
    ):
        self.region = region
        self.openai_model = openai_model
        self.reasoning_effort = reasoning_effort
        self.verbosity = verbosity
        self.max_completion_tokens = max_completion_tokens
        self.prompt_filename = prompt_filename
        self.merge_prompt_filename = merge_prompt_filename
        self.alt_prompt_filenames = alt_prompt_filenames or ["bid_spec_prompt.txt"]
        self.require_prompt_file = require_prompt_file
        self.pdf_max_chars = pdf_max_chars

        # prompt caches (avoid repeated file I/O)
        self._prompt_cache: Optional[str] = None
        self._prompt_cache_mtime: Optional[float] = None
        self._merge_prompt_cache: Optional[str] = None
        self._merge_prompt_cache_mtime: Optional[float] = None

        api_key = load_openai_key(credentials_ini)
        self.client = OpenAI(api_key=api_key)

        try:
            if aws_profile:
                session = boto3.Session(profile_name=aws_profile)
                self.s3_client = session.client("s3")
            else:
                self.s3_client = boto3.client("s3")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize S3 client. Ensure AWS credentials are set: {e}")

    # ----- S3 -----
    def download_from_s3(self, bucket_name: str, key: str) -> bytes:
        try:
            logger.info(f"[S3] GET s3://{bucket_name}/{key}")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            data = response["Body"].read()
            logger.info(f"[S3] Downloaded {len(data)} bytes")
            return data
        except (BotoCoreError, ClientError) as e:
            logger.error(f"[S3] Download error: {e}")
            raise RuntimeError(f"Error downloading file from S3: {e}")

    # ----- Prompt Loaders (cached; no preview writes) -----
    def _load_prompt_local(self, explicit_path: Optional[str] = None) -> Optional[str]:
        # use cache if available
        if self._prompt_cache is not None:
            return self._prompt_cache

        candidates: List[str] = []
        if explicit_path:
            candidates.append(explicit_path)
        candidates.append(self.prompt_filename)
        candidates.extend(self.alt_prompt_filenames)

        for name in candidates:
            path = name if os.path.isabs(name) else os.path.join(_script_dir(), name)
            if os.path.isfile(path):
                mtime = os.path.getmtime(path)
                with open(path, "rb") as f:
                    data = f.read()
                try:
                    text = data.decode("utf-8")
                except UnicodeDecodeError:
                    text = data.decode("utf-8", errors="ignore")
                    logger.warning("[PROMPT] Some characters couldn't be decoded and were ignored")

                self._prompt_cache = text
                self._prompt_cache_mtime = mtime
                logger.info(f"[PROMPT] Using prompt file: {os.path.basename(path)} (chars={len(text)})")
                return text

        if self.require_prompt_file:
            raise RuntimeError(f"Required prompt file not found. Tried: {', '.join(candidates)}")
        logger.warning("[PROMPT] No local prompt file found; continuing without external prompt")
        return None

    def _load_merge_prompt_local(self) -> Optional[str]:
        # use cache if available
        if self._merge_prompt_cache is not None:
            return self._merge_prompt_cache

        merge_path = self.merge_prompt_filename if os.path.isabs(self.merge_prompt_filename) \
            else os.path.join(_script_dir(), self.merge_prompt_filename)

        if os.path.isfile(merge_path):
            mtime = os.path.getmtime(merge_path)
            with open(merge_path, "rb") as f:
                data = f.read()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                text = data.decode("utf-8", errors="ignore")
                logger.warning("[PROMPT] Some characters in merge prompt couldn't be decoded and were ignored")

            self._merge_prompt_cache = text
            self._merge_prompt_cache_mtime = mtime
            logger.info(f"[PROMPT] Using merge prompt file: {os.path.basename(merge_path)} (chars={len(text)})")
            return text

        logger.warning(f"[PROMPT] Merge prompt file not found: {merge_path}")
        return None

    # ----- PDF Parser -----
    def _extract_pdf_text(self, file_content: bytes) -> str:
        reader = PdfReader(io.BytesIO(file_content))
        parts: List[str] = []
        total_pages = len(reader.pages)
        logger.info(f"[PDF] Pages detected: {total_pages}")

        for i, page in enumerate(reader.pages):
            try:
                txt = page.extract_text() or ""
            except Exception as e:
                logger.warning(f"[PDF] Failed to extract text from page {i+1}: {e}")
                txt = ""
            if txt:
                parts.append(f"\n--- Page {i+1} ---\n{txt}")
        text = "".join(parts).strip()

        original_length = len(text)
        logger.info(f"[PDF] Extracted {original_length} characters")

        if self.pdf_max_chars and len(text) > self.pdf_max_chars:
            text = text[: self.pdf_max_chars] + "\n\n[Truncated due to pdf_max_chars limit]"
            logger.info(f"[PDF] Truncated from {original_length} to {len(text)} characters due to pdf_max_chars")

        return text or "[No extractable text found in PDF.]"

    # ----- OpenAI -----
    def _chat_complete(self, messages: List[Dict[str, Any]]):
        extra_body = {}
        if self.reasoning_effort:
            extra_body["reasoning_effort"] = self.reasoning_effort
        if self.verbosity:
            extra_body["verbosity"] = self.verbosity

        approx_input_chars = 0
        try:
            approx_input_chars = sum(len(m.get("content", "")) for m in messages)
        except Exception:
            pass
        logger.info(f"[AI] Sending to {self.openai_model}: messages={len(messages)}, approx_input_chars={approx_input_chars}")

        response = self.client.chat.completions.create(
            model=self.openai_model,
            messages=messages,
            max_completion_tokens=self.max_completion_tokens,
            **({"extra_body": extra_body} if extra_body else {})
        )

        # Log and save raw content (kept as-is)
        try:
            content = response.choices[0].message.content
            logger.info(f"[AI] Received content length={len(content) if content else 0}")
        except Exception as e:
            logger.warning(f"[AI] Unable to read raw content: {e}")

        logger.info("Received response from OpenAI")
        return response

    # ----- Chunking -----
    def split_pdf_by_size(self, file_content, max_size_mb=25):
        max_size = max_size_mb * 1024 * 1024
        reader = PdfReader(io.BytesIO(file_content))
        chunks = []
        total_pages = len(reader.pages)

        logger.info(f"[CHUNK] Total file size={len(file_content)} bytes, pages={total_pages}, max_chunk={max_size_mb}MB")

        total_size = len(file_content)
        estimated_pages_per_chunk = max(1, int((total_pages * max_size) / total_size)) if total_size else 1
        logger.info(f"[CHUNK] Estimated {estimated_pages_per_chunk} pages per chunk based on file size")

        start_page = 0
        chunk_num = 1

        while start_page < total_pages:
            end_page = min(start_page + estimated_pages_per_chunk, total_pages)

            current_writer = PdfWriter()
            for page_num in range(start_page, end_page):
                current_writer.add_page(reader.pages[page_num])

            temp_output = io.BytesIO()
            current_writer.write(temp_output)
            temp_size = temp_output.tell()

            if temp_size > max_size and (end_page - start_page) > 1:
                low, high = 1, end_page - start_page
                best_pages = 1
                while low <= high:
                    mid = (low + high) // 2
                    test_end = start_page + mid
                    test_writer = PdfWriter()
                    for page_num in range(start_page, test_end):
                        test_writer.add_page(reader.pages[page_num])
                    test_output = io.BytesIO()
                    test_writer.write(test_output)
                    test_size = test_output.tell()
                    if test_size <= max_size:
                        best_pages = mid
                        low = mid + 1
                    else:
                        high = mid - 1

                end_page = start_page + best_pages
                current_writer = PdfWriter()
                for page_num in range(start_page, end_page):
                    current_writer.add_page(reader.pages[page_num])

            output = io.BytesIO()
            current_writer.write(output)
            chunk_data = output.getvalue()
            chunks.append(chunk_data)

            logger.info(f"[CHUNK] Created chunk {chunk_num} pages {start_page+1}-{end_page} size={len(chunk_data)} bytes")

            start_page = end_page
            chunk_num += 1

            # keep your heuristic; only logging above changed
        logger.info(f"[CHUNK] Split into {len(chunks)} total chunks")
        for idx, c in enumerate(chunks, start=1):
            logger.info(f"[CHUNK] #{idx} size={len(c)} bytes")
        return chunks

    # ----- Orchestration -----
    def _process_pdf_file(self, file_content: bytes, prompt: str):
        logger.info(f"[PROC] Starting PDF process, bytes={len(file_content)}")

        file_splits = self.split_pdf_by_size(file_content)

        # Single-chunk flow
        if len(file_splits) == 1:
            logger.info("[PROC] Single chunk flow")
            text = self._extract_pdf_text(file_splits[0])
            combined = f"{prompt}\n\n--- BEGIN FILE CONTENT ---\n{text}\n--- END FILE CONTENT ---"
            messages = [{"role": "user", "content": combined}]
            return self._chat_complete(messages)

        # Multi-chunk flow
        logger.info(f"[PROC] Multi-chunk flow: {len(file_splits)} chunks")
        response_contents = []

        for i, chunk in enumerate(file_splits):
            logger.info(f"[PROC] Chunk {i+1}/{len(file_splits)}: extracting text")
            text = self._extract_pdf_text(chunk)
            combined = f"{prompt}\n\n--- BEGIN FILE CONTENT ---\n{text}\n--- END FILE CONTENT ---"
            messages = [{"role": "user", "content": combined}]
            chunk_response = self._chat_complete(messages)
            if hasattr(chunk_response, 'choices') and chunk_response.choices:
                content = chunk_response.choices[0].message.content or ""
                logger.info(f"[PROC] Chunk {i+1} AI content length={len(content)}")
                response_contents.append(content or "No content returned")
            else:
                logger.warning(f"[PROC] No response content for chunk {i+1}")
                response_contents.append("Error: No response content")

        # Merge results
        logger.info("[PROC] Combining chunk responses using merge prompt")
        merge_prompt = self._load_merge_prompt_local()

        if merge_prompt:
            combined_content = "\n\n".join([f"=== Chunk {i+1} Results ===\n{content}" for i, content in enumerate(response_contents)])
            full_merge_prompt = f"{merge_prompt}\n\n{combined_content}"
            messages = [{"role": "user", "content": full_merge_prompt}]
        else:
            logger.warning("[PROC] Merge prompt not available, using fallback combine prompt")
            combined_content = "\n\n".join([f"=== Chunk {i+1} ===\n{content}" for i, content in enumerate(response_contents)])
            combine_prompt = f"Combine and consolidate the following analysis results into a single comprehensive JSON response with the same structure:\n\n{combined_content}"
            messages = [{"role": "user", "content": combine_prompt}]

        return self._chat_complete(messages)

    # ----- Public entry points -----
    def process_local_file(self, file_path: str, prompt: Optional[str] = None, prompt_path: Optional[str] = None):
        """Process a local PDF file"""
        used_prompt = prompt if prompt else self._load_prompt_local(explicit_path=prompt_path)
        if used_prompt is None and self.require_prompt_file:
            raise RuntimeError("Prompt file is required but could not be loaded.")

        with open(file_path, 'rb') as f:
            content = f.read()

        logger.info(f"Processing local file: {file_path}")
        return self._process_pdf_file(content, used_prompt or "Analyze this file.")

    def process_s3_file(self, bucket_name: str, key: str, prompt: Optional[str] = None, prompt_path: Optional[str] = None):
        used_prompt = prompt if prompt else self._load_prompt_local(explicit_path=prompt_path)
        if used_prompt is None and self.require_prompt_file:
            raise RuntimeError("Prompt file is required but could not be loaded.")
        content = self.download_from_s3(bucket_name, key)
        logger.info("Processing with OpenAI")
        return self._process_pdf_file(content, used_prompt or "Analyze this file.")
