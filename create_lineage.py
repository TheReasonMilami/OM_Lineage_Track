import os
from metadata.generated.schema.type.customProperties.complexTypes import EntityReference
from connect_OM import connectOM
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import (
    EntitiesEdge,
    ColumnLineage,
    LineageDetails,
    EntityLineage
)
from dotenv import load_dotenv
from get_table_by_gemini import (s_tables, t_tables)
import requests
import json


#just to get fqn of tables
load_dotenv()
bearer_token = os.getenv('BEARER_TOKEN')
def find_service():
    url = 'http://192.168.1.45:8585/api/v1/services/databaseServices'
    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }
    params = {
        "fields": "name"
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def find_database():
    url = 'http://192.168.1.45:8585/api/v1/databases'
    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }
    params = {
        "fields": "fullyQualifiedName,databaseSchemas",
        'service': f'{find_service()['data'][0]['fullyQualifiedName']}',
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def find_database_schema():
    url = 'http://192.168.1.45:8585/api/v1/databaseSchemas'
    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }
    params = {
        'database': find_database()['data'][0]['fullyQualifiedName'],
        'fields': 'fullyQualifiedName'
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()


t_tables = find_database_schema()['data'][0]['fullyQualifiedName']+'.'+t_tables[0]
s_table_info = {}
t_table_info = connectOM.get_by_name(Table, t_tables, fields=['id'])#requests.get(f'http://192.168.1.45:8585/api/v1/tables/name/{t_tables}', headers={'Authorization': f'Bearer {bearer_token}'}).json()

for source_table in s_tables:
    source_table = find_database_schema()['data'][0]['fullyQualifiedName'] + '.' + source_table
    s_table_info[source_table] = connectOM.get_by_name(Table, source_table, fields=['*']) #requests.get(f'http://192.168.1.45:8585/api/v1/tables/name/{source_table}', headers={'Authorization': f'Bearer {bearer_token}'}, params={'fqn': source_table}).json()

    from_entity_ref = {
        "id": s_table_info[source_table].id.root,  # Chuyển UUID thành string
        "type": "table"  # Dùng "table" thay vì "Table" (kiểm tra schema xem đúng không)
    }

    to_entity_ref = {
        "id": t_table_info.id.root,  # Chuyển UUID thành string
        "type": "table"
    }

    add_lineage_request = AddLineageRequest(
        edge=EntitiesEdge(
            description=f'{source_table} -> {t_tables}',
            fromEntity=from_entity_ref,
            toEntity=to_entity_ref,
        )
    )

    # add_lineage_request = AddLineageRequest(
    #     edge=EntitiesEdge(
    #         description=f'{source_table} -> {t_tables}',
    #         fromEntity=EntityReference(id=str(s_table_info[source_table].id.root), type='table'),
    #         toEntity=EntityReference(id=str(t_table_info.id.root), type='table'),
    #     )
    # )


    created_lineage = connectOM.add_lineage(add_lineage_request)
