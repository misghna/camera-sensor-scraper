# opportunities_crud.py
import logging, configparser
from typing import Dict, Any, Set

# Driver selection (same as your handler)
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

SCHEMA = "camera"
TABLE  = "opportunities"

class OpportunitiesCRUD:
    def __init__(self, credentials_file: str = "credentials.ini"):
        self.logger = logging.getLogger(__name__)
        self.config = configparser.ConfigParser()
        self.config.read(credentials_file)

        # same way as ProjectDocumentsHandler
        self.db_config = {
            "host":     self.config.get("credentials", "db_host"),
            "database": self.config.get("credentials", "db_name"),
            "user":     self.config.get("credentials", "db_user"),
            "password": self.config.get("credentials", "db_password"),
            "port":     int(self.config.get("credentials", "db_port", fallback=3306)),
        }

    def _get_connection(self):
        if DB_DRIVER == 'mysql.connector':
            return mysql.connector.connect(**self.db_config)
        else:
            return pymysql.connect(**self.db_config)

    def get_existing_project_ids(self) -> Set[int]:
        """Return project_ids already present in opportunities."""
        sql = f"SELECT DISTINCT project_id FROM {SCHEMA}.{TABLE}"
        try:
            with self._get_connection() as conn:
                cur = conn.cursor(dictionary=True)
                cur.execute(sql)
                rows = cur.fetchall()
                return {row["project_id"] for row in rows}
        except MySQLError as e:
            self.logger.error(f"get_existing_project_ids error: {e}")
            return set()

    def insert_opportunity(self, row: Dict[str, Any]) -> bool:
        sql = f"""
        INSERT INTO {SCHEMA}.{TABLE} (
            project_id, job_code, job_description, job_summary, job_size, frequency,
            match_confidence, contract_value_range, submission_deadline,
            licensing_requirements, technical_complexity, project_location,
            contract_duration, insurance_requirements, equipment_specifications,
            compliance_standards, reporting_requirements, project_type
        ) VALUES (
            %(project_id)s, %(job_code)s, %(job_description)s, %(job_summary)s, %(job_size)s, %(frequency)s,
            %(match_confidence)s, %(contract_value_range)s, %(submission_deadline)s,
            %(licensing_requirements)s, %(technical_complexity)s, %(project_location)s,
            %(contract_duration)s, %(insurance_requirements)s, %(equipment_specifications)s,
            %(compliance_standards)s, %(reporting_requirements)s, %(project_type)s
        )
        """
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, row)
                conn.commit()
                self.logger.info(f"Inserted project_id={row.get('project_id')}")
                return True
        except MySQLError as e:
            self.logger.error(f"Insert failed for project_id={row.get('project_id')}: {e}")
            return False
