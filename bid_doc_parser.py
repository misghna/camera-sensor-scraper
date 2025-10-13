#!/usr/bin/env python3
"""
S3 → OpenAI (GPT-5-mini) Processor - Simplified Version
"""

import os
import io
import json
import logging
import configparser
from typing import Optional, List, Dict, Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from pypdf import PdfReader
from openai import OpenAI
from PyPDF2 import PdfReader, PdfWriter
import io

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('pdf_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Credentials ----------

def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


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


# ---------- JSON Helpers ----------

def extract_and_print_content(response):
    """Extract content from OpenAI response and print as formatted JSON, or error info"""
    try:
        # Handle the response object
        if hasattr(response, 'choices') and response.choices:
            content = response.choices[0].message.content
            if content and content.strip():
                # Try to parse as JSON and pretty print
                try:
                    parsed_json = json.loads(content)
                    print(json.dumps(parsed_json, indent=2))
                except json.JSONDecodeError:
                    # If not valid JSON, just print the content
                    print(content)
            else:
                # Empty content - show error info
                print(json.dumps({
                    "error": "Empty response content",
                    "finish_reason": response.choices[0].finish_reason,
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
        prompt_filename: str = "bid_spec_prompt.tx",
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
            logger.info(f"Downloading '{key}' from bucket '{bucket_name}'")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            data = response["Body"].read()
            logger.info(f"Downloaded {len(data)} bytes")
            return data
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"Error downloading file from S3: {e}")

    # ----- Prompt Loader -----
    def _load_prompt_local(self, explicit_path: Optional[str] = None) -> Optional[str]:
        candidates: List[str] = []
        if explicit_path:
            candidates.append(explicit_path)
        candidates.append(self.prompt_filename)
        candidates.extend(self.alt_prompt_filenames)

        for name in candidates:
            path = name if os.path.isabs(name) else os.path.join(_script_dir(), name)
            if os.path.isfile(path):
                logger.info(f"Loading prompt from local file: {os.path.basename(path)}")
                with open(path, "rb") as f:
                    data = f.read()
                try:
                    text = data.decode("utf-8")
                except UnicodeDecodeError:
                    text = data.decode("utf-8", errors="ignore")
                    logger.warning("Some characters in prompt couldn't be decoded and were ignored")
                logger.info("Prompt loaded successfully")
                return text

        if self.require_prompt_file:
            raise RuntimeError(f"Required prompt file not found. Tried: {', '.join(candidates)}")
        logger.warning("No local prompt file found; continuing without external prompt")
        return None

    def _load_merge_prompt_local(self) -> Optional[str]:
        """Load the merge prompt for combining chunk results"""
        merge_path = self.merge_prompt_filename if os.path.isabs(self.merge_prompt_filename) else os.path.join(_script_dir(), self.merge_prompt_filename)
        
        if os.path.isfile(merge_path):
            logger.info(f"Loading merge prompt from local file: {os.path.basename(merge_path)}")
            with open(merge_path, "rb") as f:
                data = f.read()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                text = data.decode("utf-8", errors="ignore")
                logger.warning("Some characters in merge prompt couldn't be decoded and were ignored")
            logger.info("Merge prompt loaded successfully")
            return text
        else:
            logger.warning(f"Merge prompt file not found: {merge_path}")
            return None

    # ----- PDF Parser -----
    def _extract_pdf_text(self, file_content: bytes) -> str:
        reader = PdfReader(io.BytesIO(file_content))
        parts: List[str] = []
        for i, page in enumerate(reader.pages):
            try:
                txt = page.extract_text() or ""
            except Exception as e:
                logger.warning(f"Failed to extract text from page {i+1}: {e}")
                txt = ""
            if txt:
                parts.append(f"\n--- Page {i+1} ---\n{txt}")
        text = "".join(parts).strip()
        
        original_length = len(text)
        if self.pdf_max_chars and len(text) > self.pdf_max_chars:
            text = text[: self.pdf_max_chars] + "\n\n[Truncated due to pdf_max_chars limit]"
            logger.info(f"PDF text truncated from {original_length} to {len(text)} characters")
        
        result = text or "[No extractable text found in PDF.]"
        logger.info(f"Extracted {len(result)} characters from PDF")
        return result

    # ----- OpenAI -----
    def _chat_complete(self, messages: List[Dict[str, Any]]):
        extra_body = {}
        if self.reasoning_effort:
            extra_body["reasoning_effort"] = self.reasoning_effort
        if self.verbosity:
            extra_body["verbosity"] = self.verbosity

        logger.info(f"Sending request to OpenAI model: {self.openai_model}")
        response = self.client.chat.completions.create(
            model=self.openai_model,
            messages=messages,
            max_completion_tokens=self.max_completion_tokens,
            **({"extra_body": extra_body} if extra_body else {})
        )
        logger.info("Received response from OpenAI")
        return response

    def split_pdf_by_size(self, file_content, max_size_mb=25):
        max_size = max_size_mb * 1024 * 1024
        reader = PdfReader(io.BytesIO(file_content))
        chunks = []
        total_pages = len(reader.pages)
        
        logger.info(f"PDF has {total_pages} pages, splitting by {max_size_mb}MB chunks")
        
        # Estimate pages per chunk based on file size
        total_size = len(file_content)
        estimated_pages_per_chunk = max(1, int((total_pages * max_size) / total_size))
        
        logger.info(f"Estimated {estimated_pages_per_chunk} pages per chunk based on file size")
        
        start_page = 0
        chunk_num = 1
        
        while start_page < total_pages:
            # Calculate end page for this chunk
            end_page = min(start_page + estimated_pages_per_chunk, total_pages)
            
            # Create chunk with estimated pages
            current_writer = PdfWriter()
            for page_num in range(start_page, end_page):
                current_writer.add_page(reader.pages[page_num])
            
            # Check actual size
            temp_output = io.BytesIO()
            current_writer.write(temp_output)
            temp_size = temp_output.tell()
            
            # If too big and we have more than 1 page, reduce pages
            if temp_size > max_size and (end_page - start_page) > 1:
                # Binary search to find optimal page count
                low = 1
                high = end_page - start_page
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
                
                # Create final chunk with optimal page count
                end_page = start_page + best_pages
                current_writer = PdfWriter()
                for page_num in range(start_page, end_page):
                    current_writer.add_page(reader.pages[page_num])
            
            # Save the chunk
            output = io.BytesIO()
            current_writer.write(output)
            chunk_data = output.getvalue()
            chunks.append(chunk_data)
            
            logger.info(f"Created chunk {chunk_num} with pages {start_page+1}-{end_page} ({len(chunk_data)} bytes)")
            
            start_page = end_page
            chunk_num += 1
            
            # Update estimation for next chunk based on actual results
            if len(chunks) >= 2:
                # Adjust estimation based on actual chunk sizes
                avg_size_per_page = sum(len(c) for c in chunks) / sum(end_page for end_page in [estimated_pages_per_chunk] * len(chunks))
                estimated_pages_per_chunk = max(1, int(max_size / avg_size_per_page * 0.9))  # 90% safety margin
        
        logger.info(f"Split into {len(chunks)} total chunks")
        return chunks

    def _process_pdf_file(self, file_content: bytes, prompt: str):
        logger.info(f"Processing PDF file of {len(file_content)} bytes")
        
        file_splits = self.split_pdf_by_size(file_content)
        
        # If only one chunk, process directly
        if len(file_splits) == 1:
            logger.info("Processing single chunk with AI")
            text = self._extract_pdf_text(file_splits[0])
            combined = f"{prompt}\n\n--- BEGIN FILE CONTENT ---\n{text}\n--- END FILE CONTENT ---"
            messages = [{"role": "user", "content": combined}]
            return self._chat_complete(messages)
        
        # Multiple chunks - process each and combine
        logger.info(f"Processing {len(file_splits)} chunks with AI")
        response_contents = []
        
        for i, chunk in enumerate(file_splits):
            logger.info(f"AI Processing chunk {i+1}/{len(file_splits)}")
            text = self._extract_pdf_text(chunk)
            combined = f"{prompt}\n\n--- BEGIN FILE CONTENT ---\n{text}\n--- END FILE CONTENT ---"
            messages = [{"role": "user", "content": combined}]
            
            chunk_response = self._chat_complete(messages)
            # Extract the content from the response
            if hasattr(chunk_response, 'choices') and chunk_response.choices:
                content = chunk_response.choices[0].message.content
                response_contents.append(content or "No content returned")
            else:
                logger.warning(f"No response content for chunk {i+1}")
                response_contents.append("Error: No response content")
        
        # Combine all responses using merge prompt
        logger.info("Combining chunk responses using merge prompt")
        
        # Load merge prompt
        merge_prompt = self._load_merge_prompt_local()
        
        if merge_prompt:
            # Use the dedicated merge prompt
            combined_content = "\n\n".join([f"=== Chunk {i+1} Results ===\n{content}" for i, content in enumerate(response_contents)])
            full_merge_prompt = f"{merge_prompt}\n\n{combined_content}"
            messages = [{"role": "user", "content": full_merge_prompt}]
        else:
            # Fallback to generic combine prompt if merge prompt not available
            logger.warning("Merge prompt not available, using fallback combine prompt")
            combined_content = "\n\n".join([f"=== Chunk {i+1} ===\n{content}" for i, content in enumerate(response_contents)])
            combine_prompt = f"Combine and consolidate the following analysis results into a single comprehensive JSON response with the same structure:\n\n{combined_content}"
            messages = [{"role": "user", "content": combine_prompt}]
        
        return self._chat_complete(messages)

    def process_local_file(self, file_path: str, prompt: Optional[str] = None, prompt_path: Optional[str] = None):
        """Process a local PDF file"""
        used_prompt = prompt if prompt else self._load_prompt_local(explicit_path=prompt_path)
        if used_prompt is None and self.require_prompt_file:
            raise RuntimeError("Prompt file is required but could not be loaded.")
        
        # Read local file
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


# ---------- Example usage ----------

def main():
    processor = S3OpenAIProcessor(
        aws_profile="java-default",
        openai_model="gpt-5-mini",
        reasoning_effort="medium",
        verbosity="low",
        max_completion_tokens=8000,
        prompt_filename="bid_spec_prompt.tx",
        merge_prompt_filename="spec_merge_prompt.txt",
        alt_prompt_filenames=["bid_spec_prompt.txt"],
        require_prompt_file=True,
    )

    logger.info("=== Single File Example ===")
    try:
        # single_resp = processor.process_s3_file(
        #     bucket_name="bid-docs-h2g",
        #     key="all/33680821_Division_1___Multiple_Sections.pdf",
        # )
        single_resp = processor.process_local_file('downloads/33866777_Division_0___Multiple_Sections.pdf')
        extract_and_print_content(single_resp)
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        print(json.dumps({"status": "error", "error": str(e)}, indent=2))


if __name__ == "__main__":
    main()