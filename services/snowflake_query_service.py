import logging
import snowflake.connector
from Config import snow_flake_conf  # ✅ Import credentials from config.py

# Configure Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeQueryService:
    """Handles Snowflake connections and queries"""

    def __init__(self):
        """Initialize Snowflake connection"""
        try:
            self.conn = snowflake.connector.connect(
                user=snow_flake_conf["user"],
                password=snow_flake_conf["password"],
                account=snow_flake_conf["account"],  # ✅ Ensure correct format
                warehouse=snow_flake_conf["warehouse"],
                database=snow_flake_conf["database"],
                schema=snow_flake_conf["schema"],
                role="ACCOUNTADMIN",  # ✅ Ensure permissions
                insecure_mode=True  # ✅ Allows insecure SSL mode
            )
            self.cursor = self.conn.cursor()  # ✅ Fix: Create cursor for queries
            logger.info("✅ Connected to Snowflake")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            self.conn = None
            self.cursor = None  # ✅ Ensure attribute exists to prevent crashes

    def execute_query(self, query, params=None):
        """Executes a SQL query and commits the transaction"""
        if not self.cursor:
            logger.error("❌ No active Snowflake connection.")
            return

        try:
            self.cursor.execute(query, params or {})
            self.conn.commit()
            logger.info("✅ Query executed successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to execute query: {e}")

    def fetch_data(self, query):
        """Fetches data from Snowflake"""
        if not self.cursor:
            logger.error("❌ No active Snowflake connection.")
            return []

        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return [{"movie_id": row[0]} for row in result]
        except Exception as e:
            logger.error(f"❌ Failed to fetch data: {e}")
            return []
