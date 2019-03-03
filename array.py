from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

client = bigtable.Client(project='sensorray', admin=True)
instance = client.instance('instance')
table = instance.table('table')

row_filter = row_filters.CellsColumnLimitFilter(1)

for i in range(0,1):
    for j in range(0,1):
        pod = str(i).zfill(2) + str(j).zfill(2)
        print(pod)
        key = pod.encode()
        row = table.read_row(key, row_filter)
        cell = row.cells['sensor']['light'.encode()][0]
        print(int.from_bytes(cell.value, 'big'))
