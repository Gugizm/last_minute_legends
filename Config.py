import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)


producer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'client.id': os.getenv('KAFKA_CLIENT_ID'),
}

schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL')
auth_user_info = os.getenv('SCHEMA_REGISTRY_AUTH')
schema_subject = "movies_catalog_enriched-value"