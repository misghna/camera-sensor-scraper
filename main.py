import logging
import os
from project_manager import ProjectManager
from documents_manager import DocumentsManager
from file_downloader import FileDownloader
from project_opportunities_crud import process_projects_batch
from project_documents_handler import store_project_documents,get_missing_crimson_ids
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
    
    # Step 2: Project Search
    logging.info(f"\nStep 2: Project Search (GC Bidding, $2M+)")
    logging.info("-" * 30)
    

    # exit()
    # searchKeys =["Vibration monitoring","vibration and settlement monitoring","noise monitoring","Geotechnica instrumentation and monitoring",
    #              "Photographic Documentation","pre-construction Survey","Pre-construction Condition Survey","building Survey","video survey",
    #              "piezometer","inclinometer","Extensometer","MPBX","building Survey","security camera","camera system","AMTS","Robotic Total Station",
    #              "displacement monitoring","Settlement Monitoring"]
    searchKeys =[]
    for idx,searchVal in enumerate(searchKeys):
        keep_fetching=True
        offsetVal=0
        while (keep_fetching):
            time.sleep(7)      
            project_result = pm.search_projects(limit=100, offset=offsetVal,searchText=searchVal,minSearchFreq=2)
            total_hits = project_result['numFound']
            
            logging.info(f"pulling projects for searchKey:{searchVal} offset:{offsetVal}, total-hits : {total_hits}")
            keep_fetching=False

            if total_hits < 100 + offsetVal:
                keep_fetching=False
            offsetVal+=100

            if project_result and project_result.get('docs'):
                project_list=project_result['docs']
                if total_hits > 0 :
                    process_projects_batch(project_list,searchVal)
                else:
                    logging.info(f"there are no projects to process ..")
                    exit()

                prj_ids=[]
                for prj_doc in project_result['docs']:
                    u_id = prj_doc.get('uniqueProjectId', '')
                    if u_id.startswith('cur-'):
                        prj_id = u_id.replace('cur-', '')
                    else:
                        prj_id = prj_doc.get('id', '')        
                    prj_ids.append(prj_id)

                missing_ids = get_missing_crimson_ids(prj_ids)
                
                if(len(missing_ids) > 0):
                    for index,crimson_id in enumerate(missing_ids):
                        project_meta_data = pm.init_project_information(crimson_id)
                        project_id = project_meta_data["ProjectId"]
                        matching_ids = downloader.get_matching_ids(
                            project_id=project_id, 
                            search_text=searchVal,
                            access_token=pm.auth.session_data['id_token']
                        )
                        print("matching_ids: ", matching_ids)

                        logger.info(f" **** Documents (CrimsonId: {crimson_id}), index :{index}, Total: {len(missing_ids)} **** ")

                        if crimson_id and crimson_id.isdigit():
                            docs_result = downloader.get_project_documents_tree(
                                    project_id=project_id, 
                                    access_token=pm.auth.session_data['id_token']
                                )
                            # print("docs_result: ", docs_result)  
                            filtered_docs = downloader.filter_by_source_file_id(docs_result,matching_ids)                      
                            print("filtered_docs: ", len(filtered_docs))    

                            success = store_project_documents(project_id,crimson_id,filtered_docs)
                            if success: print("Docs meta saved successfully for : ", project_id)
                            sleep_duration = random.randint(2, 10)
                            logger.info(f"Sleeping for {sleep_duration} seconds...")
                            time.sleep(sleep_duration)
                        else:
                            logger.warn(f"Invalid CrimsonId extracted: {crimson_id}")

        else:
            logger.info(f"No projects found for search key : {searchVal}")
    
    os.remove('session.pkl') if os.path.exists('session.pkl') else None # remove old access 

    logger.info(f"Starting document/File download . . .")
    bid_docs = get_missing_bid_docs()  # This returns a list of rows/dicts
    if len(bid_docs) > 0:
        for index, bid_doc in enumerate(bid_docs):
            logger.info(f"Downloading doc {index+1} out of {len(bid_docs)}")
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
                
            sleep_duration = random.randint(5, 30)
            logger.info(f"Sleeping for {sleep_duration} seconds...")
            time.sleep(sleep_duration)


if __name__ == "__main__":
    # Run main workflow test
    main()
    
    # Uncomment to test download only:
    # test_download_only()