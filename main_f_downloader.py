import logging
import os
from project_manager import ProjectManager
from documents_manager import DocumentsManager
from file_downloader import FileDownloader
from project_documents_handler import get_missing_bid_docs,insert_bid_document
import time
import random

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('api_chain.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the complete project, documents, and download workflow"""
    
    # Initialize all managers
    pm = ProjectManager()
    dm = DocumentsManager()
    downloader = FileDownloader()
    
    logging.info("Starting ConstructConnect API Test Chain")

    logging.info(f"Session file: {os.path.abspath('session.pkl')}")
    logging.info(f"Log file: {os.path.abspath('api_chain.log')}")
    logging.info(f"Credentials: {os.path.abspath('credentials.ini')}")
    logging.info(f"Downloads: {os.path.abspath('downloads')}")

    # Step 1: Authentication
    print("Step 1: Authentication")
    if pm.auth.ensure_authenticated():
        logging.info(f"Logged in as: {pm.auth.session_data['email']}")
        logging.info(f"Session valid until: {pm.auth.session_data['expires_at']}")
        logging.info(f"CSRF token: {pm.auth.csrf_token[:15]}...")
    else:
        logging.info("Authentication failed")
        return
    
    # documents_tree = downloader.get_project_documents_tree(
    #     project_id="5742499", 
    #     access_token=pm.auth.session_data['id_token']
    # )
    
    matching_ids = downloader.get_matching_ids(
        project_id="5742499", 
        search_text="Vibration monitoring",
        access_token=pm.auth.session_data['id_token']
    )
    
    print(matching_ids)

    exit()
    # Step 2: Project Search
    logging.info(f"\nStep 2: Project Search (GC Bidding, $2M+)")

    bid_docs = get_missing_bid_docs()  # This returns a list of rows/dicts
    if len(bid_docs) > 0:
        for index, bid_doc in enumerate(bid_docs):
            logger.info(f"Downloading doc {index+1} out of {len(bid_doc)}")
            download_result = downloader.download_document(
                document_type=bid_doc.get('document_type'),        # Fixed: bid_doc not bid_docs
                document_id=bid_doc.get('document_id'),
                project_id=bid_doc.get('project_id'),
                access_token=pm.auth.session_data['id_token'],
                display_name=bid_doc.get('display_name'),          # Fixed: display_name typo
            )
            
            if download_result['success']:
                logger.debug(f"File Saved to: {download_result['s3_path']}")

            else:
                logger.debug(f"\nDownload failed: {download_result['error']}")
            
  
            s3_filename = download_result['filename'] if download_result['success'] else 'NA'
            handler = insert_bid_document(
                        project_id = int(bid_doc.get('project_id')),
                        document_type = bid_doc.get('document_type'),
                        document_id=bid_doc.get('document_id'),
                        display_name=bid_doc.get('display_name'),
                        s3_path=s3_filename,
                        retry_count=1
                    )
                
            sleep_duration = random.randint(5, 15)
            logger.info(f"Sleeping for {sleep_duration} seconds...")
            time.sleep(sleep_duration)

            # if index > 0 :
            #     exit()
    else:
        print("No downloadable documents found in response")
    

if __name__ == "__main__":
    # Run main workflow test
    main()
    