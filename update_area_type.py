# update_snapshot_area_types.py
import logging
import configparser
import requests
from typing import Dict, Tuple

# Driver selection (same pattern as opportunities_crud.py)
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    DB_DRIVER = 'mysql.connector'
except ImportError:
    try:
        import pymysql
        from pymysql import Error as MySQLError
        from pymysql.cursors import DictCursor
        DB_DRIVER = 'pymysql'
    except ImportError:
        raise ImportError("No MySQL driver available. Please install: pip install mysql-connector-python")

# Google Places API configuration
PLACES_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_API_KEY = "AIzaSyCCfjyxj72qr_Q48fZpBLQ0EL4uRk3QYeg"

SCHEMA = "camera"
TABLE = "pipeline_projects"

class OpportunityAreaClassifier:
    def __init__(self, credentials_file: str = "credentials.ini", logger=None):
        self.logger = logger if logger else logging.getLogger(__name__)
        self.config = configparser.ConfigParser()
        self.config.read(credentials_file)

        # Same DB config pattern as opportunities_crud.py
        self.db_config = {
            "host": self.config.get("credentials", "db_host"),
            "database": self.config.get("credentials", "db_name"),
            "user": self.config.get("credentials", "db_user"),
            "password": self.config.get("credentials", "db_password"),
            "port": int(self.config.get("credentials", "db_port", fallback=3306)),
        }

    def _get_connection(self):
        if DB_DRIVER == 'mysql.connector':
            return mysql.connector.connect(**self.db_config)
        else:
            return pymysql.connect(**self.db_config)

    def classify_area(self, lat: float, lng: float) -> str:
        """Classify area based on bar and pharmacy counts within 5km radius."""
        types_to_count = ["bar", "pharmacy"]
        radius_m = 5000
        counts = {}

        for current_type in types_to_count:
            params = {
                "location": f"{lat},{lng}",
                "radius": radius_m,
                "type": current_type,
                "key": PLACES_API_KEY,
            }

            try:
                response = requests.get(PLACES_URL, params=params)
                data = response.json()
                status = data.get("status")

                if status == "OK" or status == "ZERO_RESULTS":
                    counts[current_type] = len(data.get("results", []))
                else:
                    self.logger.warning(f"API error for {current_type}: {status}")
                    counts[current_type] = 0
            except Exception as e:
                self.logger.error(f"Error calling Places API for {current_type}: {e}")
                counts[current_type] = 0

        # Classification logic
        bar_count = counts.get("bar", 0)
        pharmacy_count = counts.get("pharmacy", 0)

        if bar_count >= 15 and pharmacy_count >= 15:
            return "URBAN CORE"
        elif bar_count >= 5 and pharmacy_count >= 10:
            return "SUBURBAN/SMALL CITY"
        elif bar_count >= 1 and pharmacy_count >= 1:
            return "SMALL TOWN"
        else:
            return "RURAL AREA"

    def get_snapshots_to_classify(self):
        """Get all pipeline_projects with coordinates but no area_type."""
        sql = f"""
        SELECT id, latitude, longitude 
        FROM {SCHEMA}.{TABLE} 
        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL 
          AND area_type IS NULL
        """
        try:
            with self._get_connection() as conn:
                if DB_DRIVER == 'mysql.connector':
                    cur = conn.cursor(dictionary=True)
                else:
                    cur = conn.cursor(DictCursor)
                cur.execute(sql)
                return cur.fetchall()
        except MySQLError as e:
            self.logger.error(f"Error fetching snapshots: {e}")
            return []

    def update_area_type(self, project_id: int, area_type: str) -> bool:
        """Update the area_type for a specific pipeline project."""
        sql = f"""
        UPDATE {SCHEMA}.{TABLE} 
        SET area_type = %s 
        WHERE id = %s
        """
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, (area_type, project_id))
                conn.commit()
                self.logger.info(f"Updated id={project_id} with area_type={area_type}")
                return True
        except MySQLError as e:
            self.logger.error(f"Update failed for id={project_id}: {e}")
            return False

    def process_all_snapshots(self):
        """Main processing loop: fetch, classify, and update all pipeline projects."""
        snapshots = self.get_snapshots_to_classify()
        total = len(snapshots)
        self.logger.info(f"Found {total} pipeline projects to classify")

        for idx, row in enumerate(snapshots, 1):
            project_id = row['id']
            lat = row['latitude']
            lng = row['longitude']

            self.logger.info(f"Processing {idx}/{total}: id={project_id}")
            
            area_type = self.classify_area(lat, lng)
            self.update_area_type(project_id, area_type)


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run the classifier
    classifier = OpportunityAreaClassifier()
    classifier.process_all_snapshots()
