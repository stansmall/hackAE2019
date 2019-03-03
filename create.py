from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

client = bigtable.Client(project='sensorray', admin=True)
instance = client.instance('instance')
table = instance.table('table')
max_versions_rule = column_family.MaxVersionsGCRule(100)
column_family_id = 'sensor'
column_families = {column_family_id: max_versions_rule}
if not table.exists():
        table.create(column_families=column_families)
else:
        print("Table already exists.")
