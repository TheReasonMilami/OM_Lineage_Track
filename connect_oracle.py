import oracledb
import re
from sqllineage.runner import LineageRunner
from metadata.ingestion.source.database.doris.utils import query

username = 'inda1'
service_name = 'orclpdb'
password = 'Inda1234'

connection = oracledb.connect(user=username, password=password, host='192.168.1.200', port=1525, service_name=service_name)
cursor = connection.cursor()


def sql_for_track_lineage(procedure_name, schema_name):
    query_statement = """
    SELECT TEXT
    FROM ALL_SOURCE
    WHERE NAME = :procedure_name
    AND OWNER = :schema_name
    AND TYPE = 'PROCEDURE'
    ORDER BY LINE
    """

    cursor.execute(statement=query_statement, procedure_name=procedure_name, schema_name=schema_name)
    query_result = cursor.fetchall()
    procedure_lines = [line[0] for line in query_result]
    raw_sql = ''.join(procedure_lines)
    match = re.search(r"BEGIN(.*)END;", raw_sql, flags=re.IGNORECASE|re.DOTALL)
    if match:
        raw_sql = match.group(1).strip()
    regex_result = re.findall(r"(DELETE|INSERT|SELECT|UPDATE)(.*?);", raw_sql, flags=re.IGNORECASE|re.DOTALL)
    sql_script = ';\n'.join(f"{keyword}{statement}" for keyword, statement in regex_result)
    return sql_script


# print(track_lineage(procedure_name, schema_name))