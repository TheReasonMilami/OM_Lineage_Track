from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (OpenMetadataConnection, AuthProvider)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService, DatabaseServiceType, DatabaseConnection
)
from metadata.generated.schema.entity.services.connections.database.oracleConnection import (
    OracleConnection, OracleServiceName
)
from metadata.ingestion.ometa.mixins.lineage_mixin import OMetaLineageMixin

server_config = OpenMetadataConnection(
    hostPort="http://localhost:8585/api",
    authProvider=AuthProvider.openmetadata,
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiUEVSU09OQUxfQUNDRVNTIiwiaWF0IjoxNzM1NjMxODY0LCJleHAiOjE3NDA4MTU4NjR9.W9jibU1vmoHzCQ0pp9hUOsp036MgE2mi_ru8nhAJ9QT7AsZ8tXVAupCaQTUXkIMfh5ceEEGyg7ueC8X-y5ZM6t5__4eO5_tweFGXFFT7rtycqPJYjF1RXh0x2r-bTNOl3cBo-hM-RCmQk7pUg4zN1ngV68zi9XsdA3TdZ22PZPryQ2ISjq5Rfd81y5JT1mxZJNcgPzMu-rxTEHvVHPZp0p2gr9CdQ_tyhE7U6tGCBCGFDw7aAoabCTgmUx2BqHv7hO03JrfgiHFdDqndqB85J_y9aoe2rxss3-hECXetO7lKy8ZyLMBQBv7yvZP5OJRNImXClij5l6SxOqwS5oBdsA"
    )
)

metadata = OpenMetadata(server_config)

db_info = metadata.get_by_name(DatabaseService, "inda_db2", fields=['*'])
print(db_info)

db_service = DatabaseService(
    id=db_info.id,
    name=db_info.name,
    serviceType=DatabaseServiceType.Oracle
)

metadata.add_lineage_by_query(
    database_service=db_service,
    sql="""
    INSERT INTO TEST_QMINH (countriesId, countriesName,regionId)
    SELECT COUNTRY_ID, COUNTRY_NAME, REGION_ID
    FROM INDA1.COUNTRIES;
""",
    database_name='inda_db2.default',
    schema_name="inda_db2.default.inda1",
    timeout=10
)
