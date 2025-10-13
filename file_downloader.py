import logging
import requests
import os
import time
from urllib.parse import quote
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

class FileDownloader:
    def __init__(self, download_dir='downloads'):
        self.download_dir = download_dir
        self.base_download_url = "https://app.isqft.com/services/file/getprojectdocument"
        
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)
        
    def download_document(self, 
                           document_type, 
                           document_id, 
                           project_id, 
                           access_token,
                           output_format="PDF", 
                           unknown_param="0",
                           source_type="3",
                           allow_file_conversion="true",
                           display_name="document"):
        """
        Download a document and upload directly to S3 instead of saving to disk
        """
        
        logger.info(f"Downloading document ... : {display_name} (ID: {document_id})")
        
        # # Get fresh token for download subdomain
        # fresh_token = self._get_fresh_download_token(access_token)
        # if not fresh_token:
        #     logger.warning("Could not get fresh token, using original token")
        #     fresh_token = access_token
        
        # Use the exact working URL format from debug
        download_url = (
            f"{self.base_download_url}/"
            f"{document_type}/{document_id}/{output_format}/{project_id}/{unknown_param}"
            f"?sourceType={source_type}&allowFileConversion={allow_file_conversion}"
        )
        
        # Create safe filename
        safe_name = self._sanitize_filename(display_name)
        filename = f"{document_id}_{safe_name}.{output_format.lower()}"
        
        try:
            # Use fresh token for download request
            headers = {
                'authorization': f'Bearer {access_token}',
                'accept': '*/*',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info(f"Download URL: {download_url}")
            
            # Make the request
            response = requests.get(download_url, headers=headers, stream=True, timeout=60)
                   
            # Check for HTTP errors
            if response.status_code != 200:
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}: {response.reason}',
                    'document_id': document_id,
                    'document_name': display_name,
                    'status_code': response.status_code
                }
            
            # Collect file content in memory
            file_content = b''
            total_downloaded = 0
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file_content += chunk
                    total_downloaded += len(chunk)
            
            logger.info(f"Download complete: {total_downloaded:,} bytes")
            
            # Validate the content before uploading
            if not file_content.startswith(b'%PDF'):
                logging.error("File not pdf may be access issue terminating")
                exit()
                return {
                    'success': False,
                    'error': 'Downloaded content is not a valid PDF',
                    'document_id': document_id,
                    'document_name': display_name,
                    'file_size': len(file_content)
                }
            
            # Upload to S3
            s3_filename = filename.replace(' ', '_').replace('-', '_')
            s3_result = self.upload_to_s3( file_content, s3_filename, document_id)
            


            if s3_result['success']:
                return {
                    'success': True,
                    's3_path': s3_result['s3_path'],
                    's3_key': s3_result['s3_key'],
                    'filename': s3_filename,
                    'file_size': len(file_content),
                    'document_id': document_id,
                    'document_name': display_name,
                    'status_code': response.status_code
                }
            else:
                return {
                    'success': False,
                    'error': f"S3 upload failed: {s3_result['error']}",
                    'document_id': document_id,
                    'document_name': display_name,
                    'file_size': len(file_content)
                }
            
        except Exception as e:
            logger.error(f"Error downloading document {document_id}: {e}")
            return {
                'success': False,
                'error': f'Download error: {str(e)}',
                'document_id': document_id,
                'document_name': display_name
            }
    
    def _validate_pdf_file(self, file_path):
        """Validate that the downloaded file is a proper PDF"""
        try:
            with open(file_path, 'rb') as f:
                first_bytes = f.read(10)
                
            if first_bytes.startswith(b'%PDF'):
                return {
                    'is_valid': True,
                    'file_type': 'PDF',
                    'message': 'Valid PDF file'
                }
            elif first_bytes.startswith(b'<!DOCTYPE') or first_bytes.startswith(b'<html'):
                # Read more to get error message
                logging.error("Received HTML instead of PDF, exiting")
                exit()
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        html_content = f.read(500)
                    return {
                        'is_valid': False,
                        'file_type': 'HTML',
                        'error': f'Received HTML instead of PDF: {html_content[:200]}...',
                        'html_preview': html_content
                    }
                except:
                    return {
                        'is_valid': False,
                        'file_type': 'HTML',
                        'error': 'Received HTML instead of PDF (could not read content)'
                    }
            elif first_bytes.startswith(b'{"error"') or first_bytes.startswith(b'{"message"'):
                # Read JSON error
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        json_content = f.read()
                    return {
                        'is_valid': False,
                        'file_type': 'JSON',
                        'error': f'API returned JSON error: {json_content[:200]}',
                        'json_error': json_content
                    }
                except:
                    return {
                        'is_valid': False,
                        'file_type': 'JSON',
                        'error': 'API returned JSON error (could not read content)'
                    }
            else:
                return {
                    'is_valid': False,
                    'file_type': 'Unknown',
                    'error': f'Unknown file type. First bytes: {first_bytes}',
                    'first_bytes': first_bytes
                }
                
        except Exception as e:
            return {
                'is_valid': False,
                'file_type': 'Error',
                'error': f'Could not validate file: {e}'
            }
    
    def _get_fresh_download_token(self, current_token):
        """Get fresh token for download subdomain"""
        try:
            headers = {
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': f'Bearer {current_token}',
                'origin': 'https://webtakeoff.takeoff.constructconnect.com',
                'referer': 'https://webtakeoff.takeoff.constructconnect.com/',
                'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
            }
            
            response = requests.get(
                "https://login.io.constructconnect.com/api/echoToken",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                token_data = response.json()
                fresh_token = token_data.get('accessToken')
                if fresh_token:
                    logger.info("Got fresh download token")
                    return fresh_token
            
            logger.warning(f"Token refresh failed: {response.status_code}")
            return None
            
        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            return None
    
    def _sanitize_filename(self, filename):
        """Create a safe filename by removing invalid characters"""
        if not filename:
            return "unknown"
            
        invalid_chars = '<>:"/\\|?*'
        safe_name = filename
        for char in invalid_chars:
            safe_name = safe_name.replace(char, '_')
        
        # Remove extra spaces and limit length
        safe_name = ' '.join(safe_name.split())  # Remove extra whitespace
        return safe_name[:100]  # Limit length
    
    def upload_to_s3(self,file_content, filename, document_id):
        """
        Upload file content directly to S3 bucket
        
        Args:
            file_content: Binary file content (bytes)
            filename: Original filename for the document
            document_id: Document ID to include in S3 path
        
        Returns:
            dict: Upload result with success status and S3 path
        """
        bucket_name = "bid-docs-h2g"
        
        s3_key = f"all/{filename}"
        
        try:
            session = boto3.Session(profile_name='java-default')
            s3_client = session.client('s3')
            # s3_client = boto3.client('s3', profile_name='java-default')
            
            # Upload file content directly to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType='application/pdf'
            )
            
            s3_path = f"s3://{bucket_name}/{s3_key}"
            logger.info(f"File uploaded to S3: {s3_path}")
            
            return {
                'success': True,
                's3_path': s3_path,
                's3_key': s3_key,
                'bucket': bucket_name,
                'file_size': len(file_content)
            }
            
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
            return {
                'success': False,
                'error': f"S3 upload error: {str(e)}",
                's3_path': None
            }
        except Exception as e:
            logger.error(f"Unexpected S3 upload error: {e}")
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                's3_path': None
            }
    
    # def download_multiple_documents(self, 
    #                                documents_list, 
    #                                project_id, 
    #                                access_token,
    #                                max_downloads=None,
    #                                delay_seconds=1,
    #                                output_format="PDF"):
    #     """
    #     Download multiple documents with improved error handling
    #     """
        
    #     logger.info(f"Starting bulk download of {len(documents_list)} documents")
        
    #     download_results = []
    #     downloads_count = 0
    #     successful_count = 0
        
    #     for i, doc in enumerate(documents_list):
    #         # Check max downloads limit
    #         if max_downloads and downloads_count >= max_downloads:
    #             logger.info(f"Reached max downloads limit: {max_downloads}")
    #             break
                
    #         # Only download actual files (IsLeaf = 1)
    #         if doc.get('IsLeaf') != 1:
    #             logger.debug(f"Skipping folder/non-leaf item: {doc.get('DisplayName', 'Unknown')}")
    #             continue
                
    #         downloads_count += 1
    #         doc_name = doc.get('DisplayName', 'Unknown')
    #         logger.info(f"Downloading {downloads_count}/{max_downloads or 'all'}: {doc_name}")
            
    #         result = self.download_document(
    #             document_type=doc.get('DocumentType'),
    #             document_id=doc.get('id'),
    #             project_id=project_id,
    #             access_token=access_token,
    #             output_format=output_format,
    #             display_name=doc_name
    #         )
            
    #         download_results.append(result)
            
    #         if result['success']:
    #             successful_count += 1
    #             logger.info(f"  ✓ Downloaded: {result['filename']} ({result['file_size']:,} bytes)")
    #         else:
    #             logger.error(f"  ❌ Failed: {doc_name} - {result['error']}")
            
    #         # Rate limiting delay (except for last item)
    #         if delay_seconds > 0 and i < len(documents_list) - 1:
    #             time.sleep(delay_seconds)
        
    #     # Summary
    #     failed_count = downloads_count - successful_count
    #     total_size = sum(r.get('file_size', 0) for r in download_results if r['success'])
        
    #     logger.info(f"Download complete: {successful_count} successful, {failed_count} failed, {total_size:,} bytes total")
    #     logger.info(f"\nDownload Summary:")
    #     logger.info(f"  Processed: {downloads_count} documents")
    #     logger.info(f"  Successful: {successful_count}")
    #     logger.info(f"  Failed: {failed_count}")
    #     logger.info(f"  Total size: {total_size:,} bytes")
    #     logger.info(f"  Success rate: {(successful_count/downloads_count*100):.1f}%" if downloads_count > 0 else "  Success rate: N/A")
        
    #     return download_results
    
    def get_project_documents_tree(self, project_id, access_token, source_type="3"):
        """
        Get the project documents tree structure from the API
        
        Args:
            project_id: The project ID to fetch documents for
            access_token: Bearer token for authentication
            source_type: Source type parameter (default "3")
        
        Returns:
            list: Array of document data objects from the "data" key, or empty list on failure
        """
        
        logger.info(f"Fetching project documents tree for project ID: {project_id}")
        
        # Build the API URL
        api_url = (
            f"https://app.isqft.com/services/file/UI_GetProjectDocumentsTree_All"
            f"?projectId={project_id}&sourceType={source_type}"
        )
        
        try:
            # Set up headers similar to download_document method
            headers = {
                'authorization': f'Bearer {access_token}',
                'accept': '*/*',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info(f"Documents tree API URL: {api_url}")
            
            # Make the request
            response = requests.get(api_url, headers=headers, timeout=30)
            
            logger.info(f"Documents tree response status: {response.status_code}")
            
            # Check for HTTP errors
            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code}: {response.reason}")
                return []
            
            # Parse JSON response
            try:
                json_data = response.json()
                
                # Check if response has expected structure
                if not isinstance(json_data, dict):
                    logger.error("Response is not a JSON object")
                    return []
                
                if not json_data.get('success', False):
                    logger.error(f"API returned success=false: {json_data}")
                    return []
                
                # Extract the data array
                documents_data = json_data.get('data', [])
                
                if not isinstance(documents_data, list):
                    logger.error("'data' key is not an array")
                    return []
                
                logger.info(f"Successfully retrieved {len(documents_data)} top-level items from documents tree")
                
                # Log some basic stats
                total_docs = json_data.get('total', 0)
                logger.info(f"API reports total of {total_docs} items")
                
                return documents_data
                
            except ValueError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                return []
                
        except requests.exceptions.Timeout:
            logger.error("Request timed out while fetching documents tree")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while fetching documents tree: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error while fetching documents tree: {e}")
            return []
    
    def get_matching_ids(self, project_id, search_text, access_token, source_type="3", is_exact_search=False, api_version="1.6"):

        logger.info(f"Getting matching document IDs for project {project_id} with search text: '{search_text}'")
        
        # Build the API URL with query parameters
        from urllib.parse import urlencode
        
        base_url = "https://api.app.constructconnect.com/search/MatchingDocuments"
        
        # Prepare query parameters
        params = {
            'SearchText': f'"{search_text}"',
            'IsqftProjectID': project_id,
            'sourceType': source_type,
            'IsExactSearch': str(is_exact_search).lower(),
            'api-version': api_version
        }
        
        # Build the full URL
        query_string = urlencode(params)
        search_url = f"{base_url}?{query_string}"
        
        try:
            # Set up headers similar to download_document method
            headers = {
                'authorization': f'Bearer {access_token}',
                'accept': '*/*',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info(f"Matching documents API URL: {search_url}")
            
            # Make the request
            response = requests.get(search_url, headers=headers, timeout=30)
            
            logger.info(f"Matching documents response status: {response.status_code}")
            
            # Check for HTTP errors
            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code}: {response.reason}")
                return []
            
            # Parse JSON response
            try:
                result = response.json()
                
                # The response should be a direct array of document IDs
                if isinstance(result, list):
                    # Validate that all items are numeric (document IDs)
                    try:
                        document_ids = [int(doc_id) for doc_id in result]
                        logger.info(f"Successfully retrieved {len(document_ids)} matching document IDs")
                        
                        if document_ids:
                            logger.info(f"Sample matching IDs: {document_ids[:5]}{'...' if len(document_ids) > 5 else ''}")
                        
                        return document_ids
                        
                    except (ValueError, TypeError) as e:
                        logger.error(f"Invalid document ID format in response: {e}")
                        logger.error(f"Response content: {result}")
                        return []
                
                else:
                    logger.error(f"Unexpected response format. Expected list, got {type(result)}")
                    logger.error(f"Response content: {result}")
                    return []
                    
            except ValueError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                return []
                
        except requests.exceptions.Timeout:
            logger.error("Request timed out while getting matching document IDs")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while getting matching document IDs: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting matching document IDs: {e}")
            return []
    
    # def test_single_download(self, document_type, document_id, project_id, access_token, display_name="test_document"):
    #     """Test downloading a single document with the working parameters"""
        
    #     logger.info(f"Testing single document download:")
    #     logger.info(f"  Document Type: {document_type}")
    #     logger.info(f"  Document ID: {document_id}")
    #     logger.info(f"  Project ID: {project_id}")
    #     logger.info(f"  Display Name: {display_name}")
    #     logger.info(f"  Token valid: {'✓' if access_token and len(access_token) > 50 else '❌'}")
        
    #     result = self.download_document(
    #         document_type=document_type,
    #         document_id=document_id,
    #         project_id=project_id,
    #         access_token=access_token,
    #         display_name=display_name
    #     )
        
    #     if result['success']:
    #         logger.info(f"✅ Download successful! File: {result['filename']}, Size: {result['file_size']:,} bytes, Path: {result['file_path']}, Validation: {result['validation']['message']}")
    #     else:
    #         debug_info = f", Debug file: {result['debug_file']}" if 'debug_file' in result else ""
    #         logger.error(f"❌ Download failed! Error: {result['error']}{debug_info}")
            
    #     return result
    

    def filter_by_source_file_id(self,json_object, filter_source_file_ids):
        if not isinstance(json_object, list):
            return json_object
        
        filtered_result = []
        
        for item in json_object:
            # Create simplified structure for folders
            if item.get('IsFolder', 0) == 1:
                simplified_item = {
                    "id": item.get('id', ''),
                    "DocumentType": item.get('DocumentType', ''),
                    "DisplayName": item.get('DisplayName', ''),
                    "Children": []
                }
                
                # If item has children, recursively filter them
                if 'children' in item and isinstance(item['children'], list):
                    filtered_children = []
                    
                    for child in item['children']:
                        # Check if child has SourceFileId and if it matches our filter
                        if 'SourceFileId' in child:
                            if child['SourceFileId'] in filter_source_file_ids:
                                filtered_children.append(child)
                        else:
                            # If no SourceFileId, keep the item (like folders)
                            filtered_children.append(child)
                    
                    simplified_item['Children'] = filtered_children
                
                filtered_result.append(simplified_item)
        
        return filtered_result