import oracledb
from google import genai
from dotenv import load_dotenv
import os
import json

load_dotenv() #load from .env
oracle_username = os.getenv("ORACLE_USERNAME")
service_name = os.getenv("ORACLE_SERVICENAME")
oracle_password = os.getenv("ORACLE_PASSWORD")
gemini_api_key = os.getenv("GEMINI_API_KEY")

#connect to oracle
connection = oracledb.connect(user=oracle_username, password=oracle_password, host='192.168.1.200', port=1525, service_name=service_name)
cursor = connection.cursor()
query_statement = """
    SELECT TEXT
    FROM ALL_SOURCE
    WHERE NAME = 'INSERT_TO_TABLE'
    AND OWNER = 'INDA1'
    AND TYPE = 'PROCEDURE'
    ORDER BY LINE
"""
cursor.execute(query_statement)
query_result = cursor.fetchall()
procedure_lines = [line[0] for line in query_result]
raw_sql = ''.join(procedure_lines)

s_tables = []
t_tables = []
client = genai.Client(api_key=gemini_api_key)
response = client.models.generate_content(
    model='gemini-2.0-flash-lite-preview-02-05',
    contents=f"Trích xuất danh sách bảng nguồn và bảng đích trong SQL sau:\n\n{raw_sql}\n\n"
             f"Trả về kết quả dưới dạng JSON với format:\n"
             f"{{'source_tables': [...], 'target_tables': [...]}}"
)

cleaned_response = response.text.strip().replace("```json", "").replace("```", "").strip() #return a string
parsed_response = json.loads(cleaned_response)

# Gán vào biến
s_tables = parsed_response.get("source_tables", [])
t_tables = parsed_response.get("target_tables", [])
# print(type(s_tables))
# print(type(t_tables))