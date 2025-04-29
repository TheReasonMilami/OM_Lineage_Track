import oracledb
import os
from dotenv import load_dotenv


load_dotenv() #load from .env
oracle_username = os.getenv("ORACLE_USERNAME")
service_name = os.getenv("ORACLE_SERVICENAME")
oracle_password = os.getenv("ORACLE_PASSWORD")
gemini_api_key = os.getenv("GEMINI_API_KEY")

connection = oracledb.connect(user=oracle_username, password=oracle_password, host='192.168.1.200', port=1525, service_name=service_name)