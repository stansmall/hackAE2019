from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
import numpy as np
import matplotlib.pyplot as plt

def get_data(sensortype):
    client = bigtable.Client(project='sensorray', admin=True)
    instance = client.instance('instance')
    table = instance.table('table')

    row_filter = row_filters.CellsColumnLimitFilter(1)
    print("Getting 250 most recent records for "+sensortype)
    slist = []
    for i in range(0,10):
        for j in range(0,25):
            pod = str(i).zfill(2) + str(j).zfill(2)
            key = pod.encode()
            row = table.read_row(key, row_filter)
            cell = row.cells['sensor'][sensortype.encode()][0]
            slist.append(int.from_bytes(cell.value, 'big'))
    slist = np.array(slist).reshape(10,25)
    return slist

def main():
    a = get_data('temp')
    fig = plt.figure()
    im = plt.imshow(a,interpolation = "bilinear",cmap = "plasma",vmin = 30,vmax = 90)
    fig.colorbar(im)
    plt.title("Temperature Field")
    plt.show()

if __name__ == "__main__":
    main()
