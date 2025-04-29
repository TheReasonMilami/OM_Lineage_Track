from connect_oracle import connection
from google import genai
import os
import json

gemini_api_key = os.getenv("GEMINI_API_KEY")

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
cursor.close()

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

schema_cursor = connection.cursor()
schema_query_statement = """SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"""

schema_cursor.execute(schema_query_statement)
schema_query = schema_cursor.fetchall()



