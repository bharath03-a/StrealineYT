import os
import sys
import logging
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from airflow.models import Variable
from pymongo import MongoClient

# Pydantic model for settings validation
class Settings(BaseModel):
    mongodb_uri: str = Field(..., env="MONGODB_URI")

class MongoDBClient:
    def __init__(self):
        self.settings = None
        try:
            self.settings = Settings(mongodb_uri=Variable.get("MONGODB_URI"))
            logging.info("Settings loaded and validated.")
        except ValidationError as e:
            logging.error(f"Error loading settings: - {e}")
            sys.exit(1)
    
    def setup_database(self, db_name, collections):
        try:
            client = MongoClient(self.settings.mongodb_uri)
            db = client[db_name]
            
            existing_collections = db.list_collection_names()
            
            for collection_name in collections:
                if collection_name not in existing_collections:
                    collection = db[collection_name]
                    print(f"Collection '{collection_name}' created in database '{db_name}'.")
                else:
                    print(f"Collection '{collection_name}' already exists in database '{db_name}'.")
        
        except Exception as e:
            print(f"Error setting up database: {e}")

if __name__ == "__main__":
    db_name = "my_database"
    collections = ["users", "orders", "products"]

    mongo_client = MongoDBClient()
    mongo_client.setup_database(db_name, collections)