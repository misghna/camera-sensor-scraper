"""
Pipeline Projects CRUD Operations Manager
Handles database operations for pipeline_projects table with upsert functionality.
Reads database credentials from credentials.ini file.
"""

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    DB_DRIVER = 'mysql.connector'
except ImportError:
    try:
        import pymysql
        from pymysql import Error as MySQLError
        DB_DRIVER = 'pymysql'
    except ImportError:
        raise ImportError("No MySQL driver available. Please install: pip install mysql-connector-python")

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
import configparser
import os

class PipelineProjectsCRUD:
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the CRUD manager with database configuration.
        If no config provided, loads from credentials.ini file.
        
        Args:
            db_config: Optional database connection parameters
        """
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = self._load_credentials()
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def _load_credentials(self) -> Dict[str, Any]:
        """Load database configuration from credentials.ini file."""
        credentials_path = os.path.join(os.path.dirname(__file__), 'credentials.ini')
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"credentials.ini file not found at: {credentials_path}")
        
        config = configparser.RawConfigParser()
        config.read(credentials_path)
        
        if config.has_section('credentials'):
            config_dict = dict(config.items('credentials'))
        elif config.has_section('DEFAULT'):
            config_dict = dict(config.items('DEFAULT'))
        else:
            raise ValueError("No [credentials] or [DEFAULT] section found in credentials.ini")
        
        required_fields = ['db_host', 'db_name', 'db_user', 'db_password']
        missing_fields = [field for field in required_fields if not config_dict.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required credentials in credentials.ini: {', '.join(missing_fields)}")
        
        return {
            'host': config_dict['db_host'],
            'database': config_dict['db_name'],
            'user': config_dict['db_user'],
            'password': config_dict['db_password'],
            'port': int(config_dict.get('db_port', '3306'))
        }

    def _is_connected(self) -> bool:
        """Check if database connection is active."""
        if not self.connection:
            return False
        
        if DB_DRIVER == 'mysql.connector':
            return self.connection.is_connected()
        else:  # pymysql
            return self.connection.open

    def connect(self) -> bool:
        """Establish database connection."""
        try:
            if DB_DRIVER == 'mysql.connector':
                self.connection = mysql.connector.connect(**self.db_config)
            else:  # pymysql
                self.connection = pymysql.connect(**self.db_config)
            
            self.logger.info(f"Successfully connected to MySQL database using {DB_DRIVER}")
            return True
        except MySQLError as e:
            self.logger.error(f"Error connecting to MySQL: {e}")
            return False

    def disconnect(self):
        """Close database connection."""
        if self.connection and self._is_connected():
            self.connection.close()
            self.logger.info("MySQL connection closed")

    def _get_cursor(self, dictionary=True):
        """Get appropriate cursor based on driver."""
        if DB_DRIVER == 'mysql.connector':
            return self.connection.cursor(dictionary=dictionary)
        else:  # pymysql
            return self.connection.cursor(pymysql.cursors.DictCursor if dictionary else pymysql.cursors.Cursor)

    def _format_datetime(self, date_str: str) -> Optional[datetime]:
        """Convert ISO datetime string to datetime object."""
        if not date_str:
            return None
        try:
            if date_str.endswith('Z'):
                date_str = date_str[:-1] + '+00:00'
            
            # Handle fractional seconds - pad to 6 digits if needed
            if '.' in date_str and '+' in date_str:
                parts = date_str.split('.')
                if len(parts) == 2:
                    fractional_and_tz = parts[1]
                    if '+' in fractional_and_tz:
                        fractional, tz = fractional_and_tz.split('+')
                        # Pad fractional seconds to 6 digits
                        fractional = fractional.ljust(6, '0')
                        date_str = f"{parts[0]}.{fractional}+{tz}"
            
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            self.logger.warning(f"Invalid datetime format: {date_str}")
            return None

    def upsert_project(self, project_data: Dict[str, Any], searchText:str) -> bool:
        """
        Insert new project or update existing one based on id.
        
        Args:
            project_data: Dictionary containing project data matching JSON structure
            
        Returns:
            bool: True if operation successful, False otherwise
        """
        if not self._is_connected():
            if not self.connect():
                return False
        try:
            cursor = self._get_cursor(dictionary=False)
            
            # Map JSON fields to database columns
            db_data = {
                'id': int(project_data.get('id', 0)),
                'unique_project_id': project_data.get('uniqueProjectId', ''),
                'title': project_data.get('title', ''),
                'project_description': project_data.get('projectDescription', ''),
                'bid_date': self._format_datetime(project_data.get('bidDate')),
                'project_status': project_data.get('projectStatus', ''),
                'start_date': self._format_datetime(project_data.get('startDate')),
                'project_value': float(project_data.get('projectValue', 0)) if project_data.get('projectValue') else None,
                'building_uses_string': project_data.get('buildingUsesString', ''),
                'content_type': project_data.get('contentType', ''),
                'contracting_method': project_data.get('contractingMethod', ''),
                'construction_types': ', '.join(project_data.get('constructionTypes', [])) if project_data.get('constructionTypes') else '',
                'project_category': project_data.get('projectCategory', ''),
                'last_updated_date': self._format_datetime(project_data.get('lastUpdatedDate')),
                'pre_bid_meeting_date': self._format_datetime(project_data.get('preBidMeetingDate')),
                'initial_publication_date': self._format_datetime(project_data.get('initialPublicationDate')),
                'document_acquisition_status': project_data.get('documentAcquisitionStatus', ''),
                'state': project_data.get('address', {}).get('state', ''),
                'city': project_data.get('address', {}).get('city', ''),
                'latitude': float(project_data.get('location', {}).get('latitude', 0)) if project_data.get('location', {}).get('latitude') else None,
                'longitude': float(project_data.get('location', {}).get('longitude', 0)) if project_data.get('location', {}).get('longitude') else None,
                'search_text':searchText
            }

            upsert_query = """
            INSERT INTO pipeline_projects (
                id, unique_project_id, title, project_description, bid_date, project_status,
                start_date, project_value, building_uses_string, content_type, contracting_method,
                construction_types, project_category, last_updated_date, pre_bid_meeting_date,
                initial_publication_date, document_acquisition_status, state, city, latitude, longitude,search_text
            ) VALUES (
                %(id)s, %(unique_project_id)s, %(title)s, %(project_description)s, %(bid_date)s,
                %(project_status)s, %(start_date)s, %(project_value)s, %(building_uses_string)s,
                %(content_type)s, %(contracting_method)s, %(construction_types)s, %(project_category)s,
                %(last_updated_date)s, %(pre_bid_meeting_date)s, %(initial_publication_date)s,
                %(document_acquisition_status)s, %(state)s, %(city)s, %(latitude)s, %(longitude)s, %(search_text)s
            )
            ON DUPLICATE KEY UPDATE
                unique_project_id = VALUES(unique_project_id),
                title = VALUES(title),
                project_description = VALUES(project_description),
                bid_date = VALUES(bid_date),
                project_status = VALUES(project_status),
                start_date = VALUES(start_date),
                project_value = VALUES(project_value),
                building_uses_string = VALUES(building_uses_string),
                content_type = VALUES(content_type),
                contracting_method = VALUES(contracting_method),
                construction_types = VALUES(construction_types),
                project_category = VALUES(project_category),
                last_updated_date = VALUES(last_updated_date),
                pre_bid_meeting_date = VALUES(pre_bid_meeting_date),
                initial_publication_date = VALUES(initial_publication_date),
                document_acquisition_status = VALUES(document_acquisition_status),
                state = VALUES(state),
                city = VALUES(city),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                search_text = VALUES(search_text)
            """

            cursor.execute(upsert_query, db_data)
            self.connection.commit()
            
            affected_rows = cursor.rowcount
            if affected_rows == 1:
                self.logger.info(f"Inserted new project: {db_data['unique_project_id']}")
            elif affected_rows == 2:
                self.logger.info(f"Updated existing project: {db_data['unique_project_id']}")
            
            cursor.close()
            return True

        except MySQLError as e:
            self.logger.error(f"Database error during upsert: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during upsert: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def get_project_by_id(self, project_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve project by ID."""
        if not self._is_connected():
            if not self.connect():
                return None

        try:
            cursor = self._get_cursor(dictionary=True)
            query = "SELECT * FROM pipeline_projects WHERE id = %s"
            cursor.execute(query, (project_id,))
            result = cursor.fetchone()
            cursor.close()
            return result
        except MySQLError as e:
            self.logger.error(f"Error retrieving project: {e}")
            return None

    def get_project_by_unique_id(self, unique_project_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve project by unique project ID."""
        if not self._is_connected():
            if not self.connect():
                return None

        try:
            cursor = self._get_cursor(dictionary=True)
            query = "SELECT * FROM pipeline_projects WHERE unique_project_id = %s"
            cursor.execute(query, (unique_project_id,))
            result = cursor.fetchone()
            cursor.close()
            return result
        except MySQLError as e:
            self.logger.error(f"Error retrieving project: {e}")
            return None

    def get_projects_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Retrieve projects by project status."""
        if not self._is_connected():
            if not self.connect():
                return []

        try:
            cursor = self._get_cursor(dictionary=True)
            query = "SELECT * FROM pipeline_projects WHERE project_status = %s ORDER BY bid_date ASC"
            cursor.execute(query, (status,))
            results = cursor.fetchall()
            cursor.close()
            return results
        except MySQLError as e:
            self.logger.error(f"Error retrieving projects: {e}")
            return []

    def delete_project(self, project_id: int) -> bool:
        """Delete project by ID."""
        if not self._is_connected():
            if not self.connect():
                return False

        try:
            cursor = self._get_cursor(dictionary=False)
            query = "DELETE FROM pipeline_projects WHERE id = %s"
            cursor.execute(query, (project_id,))
            self.connection.commit()
            
            success = cursor.rowcount > 0
            if success:
                self.logger.info(f"Deleted project with ID: {project_id}")
            else:
                self.logger.warning(f"No project found with ID: {project_id}")
            
            cursor.close()
            return success
        except MySQLError as e:
            self.logger.error(f"Error deleting project: {e}")
            return False

    def batch_upsert_projects(self, projects: List[Dict[str, Any]], searchText:str) -> Dict[str, int]:
        """
        Batch upsert multiple projects (max 100).
        
        Args:
            projects: List of project dictionaries (max 100)
        
        Returns:
            Dict with counts: {'successful': count, 'failed': count, 'total': count}
        """
        if len(projects) > 100:
            projects = projects[:100]
        
        results = {'successful': 0, 'failed': 0, 'total': len(projects)}
        
        if not self._is_connected():
            if not self.connect():
                return results
        
        for project in projects:
            try:
                if self.upsert_project(project,searchText):
                    results['successful'] += 1
                else:
                    results['failed'] += 1
            except Exception as e:
                self.logger.error(f"Error processing project {project.get('uniqueProjectId', 'unknown')}: {e}")
                results['failed'] += 1
        
        return results

# Factory function for external usage
def create_crud_manager(db_config: Optional[Dict[str, Any]] = None) -> PipelineProjectsCRUD:
    """
    Factory function to create CRUD manager instance.
    
    Args:
        db_config: Optional database configuration dictionary.
                  If None, loads from credentials.ini
        
    Returns:
        PipelineProjectsCRUD instance
    """
    return PipelineProjectsCRUD(db_config)

# Convenience function for batch processing
def process_projects_batch(projects: List[Dict[str, Any]], searchText:str) -> Dict[str, int]:
    """
    Process a batch of projects using credentials.ini.
    
    Args:
        projects: List of project dictionaries (max 100)
        
    Returns:
        Dict with counts: {'successful': count, 'failed': count, 'total': count}
    """
    crud_manager = create_crud_manager()
    try:
        if crud_manager.connect():
            results = crud_manager.batch_upsert_projects(projects,searchText)
        else:
            results = {'successful': 0, 'failed': len(projects), 'total': len(projects)}
    finally:
        crud_manager.disconnect()
    
    return results

# Example usage function for testing
def example_usage():
    """Example of how to use this module."""
    
    # Example project data array
    projects_array = [
        {
            "title": "Aston Drive Drainage & Roadway Improvements",
            "projectDescription": "This project consists of all work associated...",
            "bidDate": "2025-09-05T20:00:00Z",
            "projectStatus": "GC Bidding",
            "startDate": "2025-11-04T00:00:00Z",
            "projectValue": 11000000,
            "buildingUsesString": "Site work and paving for a civil project...",
            "contentType": "CuratedProject",
            "uniqueProjectId": "cur-7217293",
            "contractingMethod": "Open GC Bidding",
            "constructionTypes": ["Site Work", "Paving"],
            "projectCategory": "Construction",
            "lastUpdatedDate": "2025-08-31T04:00:20.983Z",
            "preBidMeetingDate": "2025-08-28T00:00:00Z",
            "initialPublicationDate": "2025-08-20T10:29:37.843Z",
            "documentAcquisitionStatus": "Available",
            "id": "7217293",
            "address": {
                "city": "Sherman",
                "state": "Texas"
            },
            "location": {
                "latitude": 33.6357,
                "longitude": -96.6089
            }
        }
    ]
    
    try:
        # Batch processing with credentials.ini
        results = process_projects_batch(projects_array)
        print(f"Batch processed: {results['successful']} successful, {results['failed']} failed")
        
        # Or use CRUD manager directly
        crud_manager = create_crud_manager()
        if crud_manager.connect():
            project = crud_manager.get_project_by_unique_id("cur-7217293")
            if project:
                print(f"Retrieved: {project['title']}")
            crud_manager.disconnect()
        
    except (FileNotFoundError, ValueError) as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    example_usage()