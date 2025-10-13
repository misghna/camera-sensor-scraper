import logging
import time
from auth import ConstructConnectAuth
import uuid

logger = logging.getLogger(__name__)

class ProjectManager:
    def __init__(self, credentials_file='credentials.ini'):
        self.auth = ConstructConnectAuth(credentials_file)
        self.app_url = "https://app.constructconnect.com"
        
    def search_projects(self, limit=100, offset=0,searchText=None,minSearchFreq=2):
        """Search for projects using the project leads API with GC Bidding filter"""
        logger.info(f"Searching projects - limit: {limit}, offset: {offset}")
        
        search_url = f"{self.app_url}/api/agent/searchAPI/projectLeadsElastic"
        
        payload = [
                    {
                        "limit": limit,
                        "offset": offset,
                        "sort": "lastUpdatedDate",
                        "sortDir": "desc",
                        "includeAllFacets": True,
                        "includeHidden": True,
                        "filters": {
                            "searchText": f'"{searchText}"',
                            "projectValue": {
                                "minValue": 2000000,
                                "includeNull": True
                            },
                            "dates": [
                                {
                                    "type": "LastUpdatedDate",
                                    "value": -1
                                }
                            ],
                            "status": [
                                "GC Bidding",
                                "Sub-Bidding",
                                "Post-Bid"
                            ],
                            "contentType": "CuratedProject, ItbProject",
                            "searchTextTarget": [
                                "Title",
                                "Details",
                                "Documents"
                            ]
                        },
                        "isWatched": False,
                        "isNew": False,
                        "isUpdated": False,
                        "area": "project",
                        "isExactSearch": False
                    }
                ]
        
        result = self.auth.make_api_call(search_url, method='POST', data=payload)
        
        if result:
            if isinstance(result, dict) and 'numFound' in result:
                filtered_docs = []
                for doc in result['docs']:
                    matched_count = doc.get('matchedDocumentCount', 0)
                    if matched_count >= minSearchFreq:
                        filtered_docs.append(doc)
                
                # Update the result object
                filtered_data = result.copy()
                filtered_data['docs'] = filtered_docs
                filtered_data['numFound'] = len(filtered_docs)
                logger.info(f"Total projects Found {result['numFound']} total projects, projects with min match count : {len(filtered_docs)}")
                return filtered_data
            else:
                logger.error(f"Unexpected response format: {type(result)} - {str(result)[:200]}")
                return None
        else:
            logger.error("Failed to get project data - API call returned None")
            return None
    
    def _generate_endpoint_context(self):
        """Generate a unique endpoint context ID like browser"""
        return str(uuid.uuid4())
    
    def init_project_information(self, crimson_id):
        """Initialize project information to get internal ProjectId"""
        logger.info(f"Initializing project information for CrimsonId: {crimson_id}")
        
        try:
            init_url = f"{self.app_url}/api/agent/project/initProjectInformation"
            
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

    # def get_project_summary(self, project_data):
    #     """Extract key information from a project"""
    #     if not project_data:
    #         return None
            
    #     return {
    #         'id': project_data.get('uniqueProjectId', 'N/A'),
    #         'title': project_data.get('title', 'N/A'),
    #         'value': project_data.get('projectValue', 0),
    #         'status': project_data.get('projectStatus', 'N/A'),
    #         'start_date': project_data.get('startDate', 'N/A'),
    #         'city': project_data.get('address', {}).get('city', 'N/A'),
    #         'state': project_data.get('address', {}).get('stateAbbr', 'N/A'),
    #         'description': project_data.get('projectDescription', 'N/A')[:200] + '...',
    #         'last_updated': project_data.get('lastUpdatedDate', 'N/A')
    #     }
    
    # def run_search_loop(self, iterations=1, delay_seconds=10):
    #     """Run project search in a loop for testing"""
    #     logger.info(f"Starting project search loop - {iterations} iterations with {delay_seconds}s delay")
        
    #     results = []
        
    #     for i in range(iterations):
    #         logger.info(f"\n--- Iteration {i+1}/{iterations} ---")
            
    #         result = self.search_projects()
            
    #         if result:
    #             print(f"\n📊 Iteration {i+1} Results:")
    #             print(f"Total projects found: {result.get('numFound', 0)}")
    #             print(f"Projects in this batch: {len(result.get('docs', []))}")
                
    #             # Show first project details
    #             if result.get('docs'):
    #                 first_project = result['docs'][0]
    #                 summary = self.get_project_summary(first_project)
                    
    #                 print(f"\nFirst project summary:")
    #                 print(f"  ID: {summary['id']}")
    #                 print(f"  Title: {summary['title'][:80]}...")
    #                 print(f"  Value: ${summary['value']:,}")
    #                 print(f"  Status: {summary['status']}")
    #                 print(f"  Location: {summary['city']}, {summary['state']}")
    #                 print(f"  Start Date: {summary['start_date']}")
                
    #             results.append(result)
    #         else:
    #             print(f"❌ Iteration {i+1} failed")
            
    #         # Wait before next iteration (except for last one)
    #         if i < iterations - 1:
    #             logger.info(f"Waiting {delay_seconds} seconds before next iteration...")
    #             time.sleep(delay_seconds)
        
    #     return results
    
    # def get_all_projects(self, max_projects=None, delay_between_calls=1):
    #     """Get all available projects with pagination"""
    #     logger.info(f"Starting to fetch all projects (max: {max_projects or 'unlimited'})")
        
    #     all_projects = []
    #     offset = 0
    #     limit = 100
        
    #     while True:
    #         logger.info(f"Fetching projects {offset} to {offset + limit}")
            
    #         result = self.search_projects(limit=limit, offset=offset)
            
    #         if not result or not result.get('docs'):
    #             logger.info("No more projects found")
    #             break
                
    #         projects = result['docs']
    #         all_projects.extend(projects)
            
    #         logger.info(f"Retrieved {len(projects)} projects. Total so far: {len(all_projects)}")
            
    #         # Check if we've reached the max or end of results
    #         if max_projects and len(all_projects) >= max_projects:
    #             all_projects = all_projects[:max_projects]
    #             logger.info(f"Reached max projects limit: {max_projects}")
    #             break
                
    #         if len(projects) < limit:
    #             logger.info("Reached end of available projects")
    #             break
                
    #         offset += limit
            
    #         # Brief delay between requests
    #         if delay_between_calls > 0:
    #             time.sleep(delay_between_calls)
        
    #     logger.info(f"Total projects retrieved: {len(all_projects)}")
    #     return all_projects
    
