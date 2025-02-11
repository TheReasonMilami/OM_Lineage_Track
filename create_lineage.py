from create_database_service import metadata
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import (
    EntitiesEdge,
    ColumnLineage,
    LineageDetails,
    EntityLineage
)
from sqllineage.runner import LineageRunner
from connect_oracle import sql_for_track_lineage


procedure_name = input('procedure name: ')
schema_name = input('schema name: ')

lineage=LineageRunner(sql_for_track_lineage(procedure_name, schema_name))

s_table_info = {}
for source_table in lineage.source_tables:
    s_table_info[source_table] = metadata.get_by_name(Table, source_table, fields=['*'])


# add_lineage_request = AddLineageRequest()