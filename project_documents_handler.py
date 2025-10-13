import json
import configparser
import os
from typing import Dict, Any, Set,Optional, List
import logging

# Database driver imports (same as your existing code)
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


class ProjectDocumentsHandler:
    def __init__(self, credentials_file='credentials.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(credentials_file)
        
        # Get database credentials
        self.db_config = {
            'host': self.config.get('credentials', 'db_host'),
            'database': self.config.get('credentials', 'db_name'),
            'user': self.config.get('credentials', 'db_user'),
            'password': self.config.get('credentials', 'db_password'),
            'port': int(self.config.get('credentials', 'db_port', fallback=3306))
        }
        
        self._ensure_table_exists()
    
    def _get_connection(self):
        """Create database connection based on available driver"""
        if DB_DRIVER == 'mysql.connector':
            return mysql.connector.connect(**self.db_config)
        else:  # pymysql
            return pymysql.connect(**self.db_config)
    
    def _ensure_table_exists(self):
        """Create the project_documents table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS project_documents (
            project_id VARCHAR(50) PRIMARY KEY,
            plans JSON,
            specs JSON,
            addenda JSON,
            other JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                conn.commit()
        except MySQLError as e:
            logging.error(f"Error creating table: {e}")
            raise
    
    def _categorize_documents(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Categorize documents by type"""
        categorized = {
            'plans': None,
            'specs': None,
            'addenda': None,
            'other': None
        }
        
        for doc in documents:
            doc_type = doc.get('DocumentType', '').lower()
            
            if doc_type == 'plans':
                categorized['plans'] = doc
            elif doc_type == 'specs':
                categorized['specs'] = doc
            elif doc_type == 'addenda':
                categorized['addenda'] = doc
            elif doc_type == 'other':
                categorized['other'] = doc
        
        return categorized
    
    def store_or_update_documents(self, project_id: str, crimson_id: str, documents: List[Dict[str, Any]]) -> bool:
        """
        Store documents if project doesn't exist, update if it does
        
        Args:
            project_id: Unique identifier for the project
            documents: List of document objects from the API
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            categorized = self._categorize_documents(documents)
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if project exists
                check_sql = "SELECT project_id FROM project_documents WHERE project_id = %s"
                cursor.execute(check_sql, (project_id,))
                exists = cursor.fetchone()
                
                if exists:
                    # Update existing record
                    update_sql = """
                    UPDATE project_documents 
                    SET plans = %s, specs = %s, addenda = %s, other = %s, updated_at = CURRENT_TIMESTAMP, crimson_id= %s
                    WHERE project_id = %s
                    """
                    cursor.execute(update_sql, (
                        json.dumps(categorized['plans']) if categorized['plans'] and len(categorized['plans']['Children']) >0 else None,
                        json.dumps(categorized['specs']) if categorized['specs'] and len(categorized['specs']['Children']) >0 else None,
                        json.dumps(categorized['addenda']) if categorized['addenda'] and len(categorized['addenda']['Children']) >0 else None,
                        json.dumps(categorized['other']) if categorized['other'] and len(categorized['other']['Children']) >0 else None,
                        crimson_id,project_id
                    ))
                    logging.info(f"Updated documents for project {project_id}")
                else:
                    # Insert new record
                    insert_sql = """
                    INSERT INTO project_documents (project_id,crimson_id, plans, specs, addenda, other)
                    VALUES (%s, %s,%s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        project_id,crimson_id,
                        json.dumps(categorized['plans']) if categorized['plans'] and len(categorized['plans']['Children']) >0 else None,
                        json.dumps(categorized['specs']) if categorized['specs'] and len(categorized['specs']['Children']) >0 else None,
                        json.dumps(categorized['addenda']) if categorized['addenda'] and len(categorized['addenda']['Children']) >0 else None,
                        json.dumps(categorized['other']) if categorized['other'] and len(categorized['other']['Children']) >0 else None
                    ))
                    logging.debug(f"Inserted documents for project {project_id}, crimson_id : {crimson_id}")
                
                conn.commit()
                return True
                
        except MySQLError as e:
            logging.error(f"Database error: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return False
    
    def get_project_documents(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve documents for a project
        
        Args:
            project_id: Project identifier
            
        Returns:
            Dictionary with categorized documents or None if not found
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                select_sql = "SELECT plans, specs, addenda, other FROM project_documents WHERE project_id = %s"
                cursor.execute(select_sql, (project_id,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'plans': json.loads(result[0]) if result[0] else None,
                        'specs': json.loads(result[1]) if result[1] else None,
                        'addenda': json.loads(result[2]) if result[2] else None,
                        'other': json.loads(result[3]) if result[3] else None
                    }
                return None
                
        except MySQLError as e:
            logging.error(f"Database error: {e}")
            return None

    def get_missing_crimson_ids(self, crimson_ids: List[str]) -> List[str]:
        """
        Get project IDs from the provided list that don't exist in the database
        
        Args:
            project_ids: List of project IDs to check
            
        Returns:
            List of project IDs that don't exist in the database
        """
        if not crimson_ids:
            return []
            
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Create placeholders for the IN clause
                placeholders = ','.join(['%s'] * len(crimson_ids))
                select_sql = f"SELECT crimson_id FROM project_documents WHERE crimson_id IN ({placeholders})"
                logging.info(f"checking ids in doc table: {crimson_ids}")
                cursor.execute(select_sql, crimson_ids)
                existing_ids = {row[0] for row in cursor.fetchall()}
                logging.info(f"No of existing ids are : {len(existing_ids)}")
                # Return project IDs that are in the input list but not in the database
                missing_ids = [pid for pid in crimson_ids if pid not in existing_ids]
                
                logging.info(f"Checked {len(crimson_ids)} project IDs, found {len(missing_ids)} missing")
                return missing_ids
                
        except MySQLError as e:
            logging.error(f"Database error: {e}")
            return crimson_ids  # Return all IDs if there's an error, to be safe
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return crimson_ids  # Return all IDs if there's an error, to be safe
        
    def get_existing_bid_docs_dict(self) -> List[dict]:
        """Get all existing bid document rows as dictionaries."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor(dictionary=True)  # Returns rows as dicts
                select_sql = "SELECT * FROM bid_doc_view"
                cursor.execute(select_sql)
                existing_rows = cursor.fetchall()
                logging.info(f"Bid docs misisng size : {len(existing_rows)} existing bid document rows")
                return existing_rows
                
        except MySQLError as e:
            logging.error(f"Database error: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return []

    def insert_bid_document(self, project_id: int, document_type: str, document_id: str, 
                       display_name: str, s3_path: str, retry_count: int = 0) -> bool:
        """
        Insert a new bid document record into the database
        
        Args:
            project_id: Project ID (integer)
            document_type: Type of document
            document_id: Unique document identifier
            display_name: Human readable document name
            s3_path: S3 path where document is stored
            retry_count: Number of retry attempts (defaults to 0)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                insert_sql = """
                INSERT INTO bid_documents 
                (project_id, document_type, document_id, display_name, s3_path, retry_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_sql, (
                    project_id, document_type, document_id, 
                    display_name, s3_path, retry_count
                ))
                conn.commit()
                
                logging.info(f"Inserted bid document: {document_id} for project {project_id}")
                return True
                
        except MySQLError as e:
            if e.errno == 1062:  # Duplicate entry error
                logging.warning(f"Bid document {document_id} already exists for project {project_id}")
            else:
                logging.error(f"Database error inserting bid document: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error inserting bid document: {e}")
            return False
    
# Or if you really want missing docs:
def get_missing_bid_docs(self, all_doc_ids: Set[str]) -> Set[str]:
    """Get set of document IDs that are missing from bid_documents table."""
    try:
        existing_ids = self.get_existing_bid_doc_ids()
        missing_ids = all_doc_ids - existing_ids
        logging.info(f"Found {len(missing_ids)} missing bid documents")
        return missing_ids
        
    except Exception as e:
        logging.error(f"Error finding missing docs: {e}")
        return all_doc_ids  # Return all if error, to be safe

# Convenience function for direct usage
def store_project_documents(project_id: str,crimson_id: str, documents: List[Dict[str, Any]], credentials_file='credentials.ini') -> bool:

    handler = ProjectDocumentsHandler(credentials_file)
    return handler.store_or_update_documents(project_id,crimson_id, documents)


def get_missing_crimson_ids(project_ids: List[str], credentials_file='credentials.ini') -> List[str]:

    handler = ProjectDocumentsHandler(credentials_file)
    return handler.get_missing_crimson_ids(project_ids)

def get_missing_bid_docs(credentials_file='credentials.ini') -> List[str]:

    handler = ProjectDocumentsHandler(credentials_file)
    return handler.get_existing_bid_docs_dict()

def insert_bid_document(project_id: int, document_type: str, document_id: str, 
                       display_name: str, s3_path: str, retry_count: int = 0, credentials_file='credentials.ini') :

    handler = ProjectDocumentsHandler(credentials_file)
    return handler.insert_bid_document(project_id,document_type, document_id, 
                       display_name, s3_path, retry_count)



# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Sample documents (your provided JSON)
    sample_documents = [
        {
            "id": "-5",
            "DocumentType": "Plans",
            "DisplayName": "Drawings",
            "Children": [
                # ... your children data
            ]
        },
        {
            "id": "-1",
            "DocumentType": "Specs",
            "DisplayName": "Specifications",
            "Children": [
                # ... your children data
            ]
        },
        {
            "id": "-3",
            "DocumentType": "Other",
            "DisplayName": "Other Documents",
            "Children": [
                # ... your children data
            ]
        },
        {
            "id": "-2",
            "DocumentType": "Addenda",
            "DisplayName": "All Addenda Documents",
            "Children": []
        }
    ]
    
    # Test the function
    success = store_project_documents("PROJECT_123", "test",sample_documents)
    print(f"Storage {'successful' if success else 'failed'}")
    
    # Test the new missing project IDs function
    test_project_ids = ["PROJECT_123", "PROJECT_456", "PROJECT_789"]
    missing_ids = get_missing_crimson_ids(test_project_ids)
    print(f"Missing project IDs: {missing_ids}")