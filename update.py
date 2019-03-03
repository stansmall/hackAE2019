import datetime
from google.cloud import bigtable

# configure the connection to bigtable
client = bigtable.Client(project='sensorray', admin=True)
instance = client.instance('instance')
table = instance.table('table')

# get the current time
timestamp=datetime.datetime.utcnow()

# read the data in from arduino
id = '0000'
data = {
    'water':500,
    'humidity':600,
    'light':800,
}

# write the data
row_key = id.encode()
rows = []

for key, value in data.items():
    row = table.row(row_key)
    row.set_cell(   'sensor',
                    key,
                    int(value),
                    timestamp)
    rows.append(row)

table.mutate_rows(rows)
