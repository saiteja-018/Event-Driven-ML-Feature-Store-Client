import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import time

logger = logging.getLogger(__name__)

class PostgreSQLManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._connect()

    def _connect(self):
        retries = 5
        while retries > 0:
            try:
                self.conn = psycopg2.connect(self.dsn)
                self.conn.autocommit = True
                logger.info("Successfully connected to PostgreSQL")
                return
            except psycopg2.OperationalError as e:
                logger.error(f"Failed to connect to PostgreSQL. Retries left: {retries - 1}")
                retries -= 1
                time.sleep(2)
        raise Exception("Could not connect to database after retries")

    def save_feature(self, entity_id: str, feature_name: str, feature_value: str):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (entity_id, feature_name)
                    DO UPDATE SET feature_value = EXCLUDED.feature_value, timestamp = NOW();
                """, (entity_id, feature_name, feature_value))
        except psycopg2.Error as e:
            logger.error(f"Database error saving feature: {e}")
            try:
                self._connect()
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (entity_id, feature_name)
                        DO UPDATE SET feature_value = EXCLUDED.feature_value, timestamp = NOW();
                    """, (entity_id, feature_name, feature_value))
            except Exception as retry_e:
                logger.error(f"Failed to save feature after retry: {retry_e}")

    def get_features(self, entity_id: str) -> dict:
        features = {}
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT feature_name, feature_value FROM features WHERE entity_id = %s", (entity_id,))
                rows = cur.fetchall()
                for row in rows:
                    features[row['feature_name']] = row['feature_value']
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving features: {e}")
        return features

    def close(self):
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Database connection closed")
