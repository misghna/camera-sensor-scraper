import logging
import requests
import time
import uuid
from auth import ConstructConnectAuth
from project_documents_handler import store_project_documents

logger = logging.getLogger(__name__)

class DocumentsManager:
    def __init__(self, credentials_file='credentials.ini'):
        self.auth = ConstructConnectAuth(credentials_file)
        
    def _generate_endpoint_context(self):
        """Generate a unique endpoint context ID like browser"""
        return str(uuid.uuid4())
    
    def init_project_information(self, crimson_id):
        """Initialize project information to get internal ProjectId"""
        logger.info(f"Initializing project information for CrimsonId: {crimson_id}")
        
        try:
            init_url = f"{self.auth.app_url}/api/agent/project/initProjectInformation"
            
            payload = [{
                "projectId": str(crimson_id),
                "isCrimsonId": True,
                "sourceType": 3
            }]
            
            # Add endpointcontext header exactly like browser
            headers = {
                'endpointcontext': self._generate_endpoint_context(),
                'referer': f'https://app.constructconnect.com/project/{crimson_id}/c?sourceType=3'
            }
            
            logger.info(f"Making initProjectInformation call with payload: {payload}")
            result = self.auth.make_api_call(init_url, method='POST', data=payload, headers=headers)
            logger.info(f"initProjectInformation response: {result}")
            
            if result and isinstance(result, list) and len(result) > 0:
                project_info = result[0]
                project_id = project_info.get('ProjectId')
                project_name = project_info.get('ProjectName', 'Unknown')
                
                logger.info(f"Project initialized - ProjectId: {project_id}, Name: {project_name[:50]}...")
                return project_info
            else:
                logger.error(f"Failed to initialize project or unexpected response: {result}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to initialize project information: {e}")
            return None
    
    def get_project_document_list_api(self, project_id,crimson_id):
        """Get project documents using the internal ProjectId"""
        logger.info(f"Getting document list for ProjectId: {project_id}")
        
        try:
            docs_url = f"{self.auth.app_url}/api/agent/document/getProjectDocumentList"
            
            payload = [{
                "projectId": str(project_id),
                "sourceType": "3"
            }]
            
            # Add endpointcontext header exactly like browser
            headers = {
                'endpointcontext': self._generate_endpoint_context(),
                'referer': f'https://app.constructconnect.com/project/{project_id}/p?sourceType=3'
            }
            
            logger.info(f"Making getProjectDocumentList call with payload: {payload}")
            result = self.auth.make_api_call(docs_url, method='POST', data=payload, headers=headers)
            logger.debug(f"getProjectDocumentList response length: {len(result)}")
            #TBD - save documents JSON
            success = store_project_documents(project_id,crimson_id,result)
            logger.debug(f"Documents list save status for  {project_id} status : {success}")
            if not success:
                logger.info(f"getProjectDocumentList full response: {result}")

            if result:
                if isinstance(result, list):
                    doc_count = len(result)
                elif isinstance(result, dict) and 'documents' in result:
                    doc_count = len(result['documents'])
                else:
                    doc_count = 1 if result else 0
                
                logger.debug(f"Retrieved {doc_count} documents for ProjectId {project_id}")
                return result
            else:
                logger.warning(f"No documents found for ProjectId {project_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get project document list: {e}")
            return None
    
    def get_project_document_list(self, project_id,crimson_id):
        """Get project documents using the internal ProjectId"""
        logger.info(f"Getting document list for ProjectId: {project_id}")
        
        try:
            docs_url = f"{self.auth.app_url}/api/agent/document/getProjectDocumentList"
            
            payload = [{
                "projectId": str(project_id),
                "sourceType": "3"
            }]
            
            # Add endpointcontext header exactly like browser
            headers = {
                'endpointcontext': self._generate_endpoint_context(),
                'referer': f'https://app.constructconnect.com/project/{project_id}/p?sourceType=3'
            }
            
            logger.info(f"Making getProjectDocumentList call with payload: {payload}")
            result = self.auth.make_api_call(docs_url, method='POST', data=payload, headers=headers)
            logger.debug(f"getProjectDocumentList response length: {len(result)}")
            #TBD - save documents JSON
            success = store_project_documents(project_id,crimson_id,result)
            logger.debug(f"Documents list save status for  {project_id} status : {success}")
            if not success:
                logger.info(f"getProjectDocumentList full response: {result}")

            if result:
                if isinstance(result, list):
                    doc_count = len(result)
                elif isinstance(result, dict) and 'documents' in result:
                    doc_count = len(result['documents'])
                else:
                    doc_count = 1 if result else 0
                
                logger.debug(f"Retrieved {doc_count} documents for ProjectId {project_id}")
                return result
            else:
                logger.warning(f"No documents found for ProjectId {project_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get project document list: {e}")
            return None
    
    def get_project_documents(self, crimson_id):
        """Complete flow to get project documents using CrimsonId"""
        logger.info(f"Getting documents for CrimsonId: {crimson_id}")
        
        # Step 1: Initialize project to get internal ProjectId
        project_info = self.init_project_information(crimson_id)
        if not project_info:
            return None
            
        # Step 2: Get document list using internal ProjectId
        project_id = project_info.get('ProjectId')
        if not project_id:
            logger.error("No ProjectId found in project information")
            return None
            
        documents = self.get_project_document_list(project_id,crimson_id)
        
        return {
            'project_info': project_info,
            'documents': documents,
            'crimson_id': crimson_id,
            'project_id': project_id
        }
    
    def test_document_retrieval(self, crimson_id):
        """Test function to retrieve and display document metadata"""
        logger.info(f"Testing document retrieval for CrimsonId: {crimson_id}")
        
        result = self.get_project_documents(crimson_id)
        
        if result:
            project_info = result['project_info']
            documents = result['documents']
            
            logging.debug(f"\nDocument retrieval successful!")
            logging.debug(f"CrimsonId: {result['crimson_id']}")
            logging.debug(f"Internal ProjectId: {result['project_id']}")
            logging.debug(f"Project Name: {project_info.get('ProjectName', 'N/A')}")
            
            if documents:
                # Count and show actual documents (not folders)
                actual_docs = []
                self._extract_actual_documents(documents, actual_docs)
                
                logging.debug(f"Total document folders: {len(documents)}")
                logging.debug(f"Total actual documents: {len(actual_docs)}")
                
                # Show first few actual documents
                logging.debug(f"\nFirst 5 actual documents:")
                for i, doc in enumerate(actual_docs[:5], 1):
                    logging.debug(f"  {i}. {doc.get('DisplayName', 'N/A')}")
                    logging.debug(f"     ID: {doc.get('id', 'N/A')}")
                    logging.debug(f"     Size: {doc.get('Size', 'N/A')} bytes")
                    logging.debug(f"     Type: {doc.get('DocumentType', 'N/A')}")
                    logging.debug(f"     Uploaded: {doc.get('DateUploaded', 'N/A')}")
            else:
                logging.info("No documents found for this project")
            
            return result
        else:
            logging.warn(f"Failed to retrieve documents for CrimsonId {crimson_id}")
            return None
    
    def _extract_actual_documents(self, data, actual_docs):
        """Recursively extract actual document files (not folders)"""
        if isinstance(data, list):
            for item in data:
                self._extract_actual_documents(item, actual_docs)
        elif isinstance(data, dict):
            # If this is an actual document (IsLeaf = 1), add it
            if data.get('IsLeaf') == 1:
                actual_docs.append(data)
            
            # Check children
            children = data.get('Children', [])
            if children:
                self._extract_actual_documents(children, actual_docs)
    
    def get_documents_for_multiple_projects(self, crimson_ids, delay_seconds=1):
        """Get documents for multiple projects using CrimsonIds"""
        logger.info(f"Getting documents for {len(crimson_ids)} projects")
        
        all_docs = {}
        
        for i, crimson_id in enumerate(crimson_ids):
            logger.info(f"Processing project {i+1}/{len(crimson_ids)}: {crimson_id}")
            
            result = self.get_project_documents(crimson_id)
            if result:
                doc_count = len(result['documents']) if result['documents'] else 0
                all_docs[crimson_id] = {
                    'project_info': result['project_info'],
                    'documents': result['documents'],
                    'count': doc_count
                }
                logger.info(f"CrimsonId {crimson_id}: {doc_count} documents")
            else:
                logger.warning(f"No documents for CrimsonId {crimson_id}")
                all_docs[crimson_id] = {
                    'project_info': None,
                    'documents': None,
                    'count': 0
                }
            
            # Brief delay between requests
            if delay_seconds > 0 and i < len(crimson_ids) - 1:
                time.sleep(delay_seconds)
        
        return all_docs