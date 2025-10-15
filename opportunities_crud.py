# opportunities_crud.py
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
        raise ImportError("No MySQL driver available. Install: pip install mysql-connector-python")

from typing import Optional, Dict, Any, List, Set
import logging, configparser, os

SCHEMA = "camera"
TABLE  = "opportunities"

class OpportunitiesCRUD:
    def __init__(self, db_config: Optional[Dict[str, Any]] = None, credentials_file: str = "credentials.ini"):
        self.db_config = db_config or self._load_credentials(credentials_file)
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def _load_credentials(self, credentials_file: str) -> Dict[str, Any]:
        path = os.path.join(os.path.dirname(__file__), credentials_file)
        if not os.path.exists(path):
            raise FileNotFoundError(f"credentials.ini not found at: {path}")
        cfg = configparser.RawConfigParser()
        cfg.read(path)
        sect = 'credentials' if cfg.has_section('credentials') else 'DEFAULT'
        creds = dict(cfg.items(sect))
        return {
            'host': creds['db_host'],
            'database': creds['db_name'],
            'user': creds['db_user'],
            'password': creds['db_password'],
            'port': int(creds.get('db_port', 3306))
        }

    def connect(self):
        try:
            if DB_DRIVER == 'mysql.connector':
                import mysql.connector
                self.connection = mysql.connector.connect(**self.db_config)
            else:
                import pymysql
                self.connection = pymysql.connect(**self.db_config)
            self.logger.info("Connected to MySQL.")
            return True
        except MySQLError as e:
            self.logger.error(f"DB connection failed: {e}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.logger.info("MySQL connection closed.")

    def _cur(self, dictionary=True):
        if DB_DRIVER == 'mysql.connector':
            return self.connection.cursor(dictionary=dictionary)
        else:
            import pymysql
            return self.connection.cursor(pymysql.cursors.DictCursor if dictionary else pymysql.cursors.Cursor)

    def get_existing_project_ids(self) -> Set[int]:
        """Fetch all project_ids already in the opportunities table."""
        if not self.connection and not self.connect(): return set()
        sql = f"SELECT project_id FROM {SCHEMA}.{TABLE}"
        cur = self._cur(True)
        cur.execute(sql)
        existing = {row['project_id'] for row in cur.fetchall()}
        cur.close()
        return existing

    def insert_opportunity(self, row: Dict[str, Any]) -> bool:
        """Insert a new opportunity row. Assumes project_id not already present."""
        if not self.connection and not self.connect():
            return False
        cur = self._cur(False)

        sql = f"""
        INSERT INTO {SCHEMA}.{TABLE}
        (
            project_id,
            job_code,
            job_description,
            job_summary,
            job_size,
            frequency,
            match_confidence,
            contract_value_range,
            submission_deadline,
            licensing_requirements,
            technical_complexity,
            project_location,
            contract_duration,
            insurance_requirements,
            equipment_specifications,
            compliance_standards,
            reporting_requirements,
            project_type
        ) VALUES (
            %(project_id)s,
            %(job_code)s,
            %(job_description)s,
            %(job_summary)s,
            %(job_size)s,
            %(frequency)s,
            %(match_confidence)s,
            %(contract_value_range)s,
            %(submission_deadline)s,
            %(licensing_requirements)s,
            %(technical_complexity)s,
            %(project_location)s,
            %(contract_duration)s,
            %(insurance_requirements)s,
            %(equipment_specifications)s,
            %(compliance_standards)s,
            %(reporting_requirements)s,
            %(project_type)s
        )
        """

        try:
            cur.execute(sql, row)
            self.connection.commit()
            self.logger.info(f"Inserted project_id={row.get('project_id')}")
            return True
        except MySQLError as e:
            self.connection.rollback()
            self.logger.error(f"Insert failed for project_id={row.get('project_id')}: {e}")
            return False
        finally:
           cur.close()

