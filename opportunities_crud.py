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
from mysql.connector import pooling, Error as MySQLError

SCHEMA = "camera"
TABLE  = "opportunities"


class OpportunitiesCRUD:
    def __init__(self, use_pool: bool = True, pool_size: int = 5, **db_kwargs):
        self.logger = logging.getLogger(__name__)
        self.use_pool = use_pool
        self.pool = None
        self.connection = None
        self.db_kwargs = db_kwargs  # host, user, password, database, etc.

        if self.use_pool:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="opps_pool",
                pool_size=pool_size,
                **self.db_kwargs
            )

    def connect(self) -> bool:
        try:
            if self.use_pool and self.pool:
                self.connection = self.pool.get_connection()
            else:
                self.connection = mysql.connector.connect(**self.db_kwargs)
            self.logger.info("Connected to MySQL.")
            return True
        except MySQLError as e:
            self.logger.error(f"MySQL connect failed: {e}")
            self.connection = None
            return False

    def ensure_connection(self) -> bool:
        """Ensure the connection is alive (reconnect if needed)."""
        try:
            if not self.connection:
                return self.connect()
            # ping will raise if dead; reconnect=True attempts reopen
            self.connection.ping(reconnect=True, attempts=1, delay=0)
            return True
        except Exception:
            # try fresh connection
            return self.connect()

    def _cur(self, dictionary: bool = False):
        if not self.ensure_connection():
            raise RuntimeError("MySQL Connection not available.")
        return self.connection.cursor(dictionary=dictionary)

    def get_existing_project_ids(self) -> set[int]:
        """Return a set of project_ids already in opportunities."""
        if not self.ensure_connection():
            self.logger.error("MySQL Connection not available.")
            return set()
        cur = self._cur(True)
        try:
            cur.execute(f"SELECT DISTINCT project_id FROM {SCHEMA}.{TABLE}")
            return {row["project_id"] for row in cur.fetchall()}
        except MySQLError as e:
            self.logger.warning(f"get_existing_project_ids failed, reconnecting once: {e}")
            try: cur.close()
            except: pass
            if not self.connect():
                return set()
            cur = self._cur(True)
            cur.execute(f"SELECT DISTINCT project_id FROM {SCHEMA}.{TABLE}")
            return {row["project_id"] for row in cur.fetchall()}
        finally:
            try: cur.close()
            except: pass

    def insert_opportunity(self, row: Dict[str, Any]) -> bool:
        if not self.ensure_connection():
            self.logger.error("MySQL Connection not available.")
            return False

        cur = self._cur(False)
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
        )"""
        try:
            cur.execute(sql, row)
            self.connection.commit()
            self.logger.info(f"Inserted project_id={row.get('project_id')}")
            return True
        except MySQLError as e:
            # retry once after reconnect
            self.logger.warning(f"Insert error, reconnecting once: {e}")
            try: cur.close()
            except: pass
            if not self.connect():
                self.logger.error("Reconnect failed.")
                return False
            cur = self._cur(False)
            try:
                cur.execute(sql, row)
                self.connection.commit()
                self.logger.info(f"Inserted project_id={row.get('project_id')} (after reconnect)")
                return True
            except MySQLError as e2:
                self.connection.rollback()
                self.logger.error(f"Insert failed after reconnect: {e2}")
                return False
        finally:
            try: cur.close()
            except: pass


