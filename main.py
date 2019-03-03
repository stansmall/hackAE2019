"""
Copyright: RAM Thubati
Date: 03/03/2019
"""

import time
import datetime
import serial
import numpy as np
import matplotlib.pyplot as plt
import multiprocessing as mp
import matplotlib
import matplotlib.animation

from google.cloud import bigtable
from multiprocessing import Process, Queue
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters


def ard_tuple(tuple_queue):
	"""
	will read serial data from arduino and then push them to
	tuple_queue for another parallel process to push it to cloud.
	:param tuple_queue:
	:return: None.
	"""
	print("Getting data from Arduino..")
	ser = serial.Serial("/dev/cu.usbmodem14201", 9600)
	for i in range(1000):
		data = ser.readline()
		data = data.decode("utf-8").strip("\n")
		temp_tuple = data.split("\t")
		temp_tuple = list(temp_tuple)
		#add timestamp to the tuple as well
		timestamp = datetime.datetime.utcnow()
		temp_tuple.append(timestamp)
		temp_tuple = tuple(temp_tuple)
		tuple_queue.put(temp_tuple)

	ser.close()
	print("All data pushed to tuple_queue, exiting ard_read process")


def tuple_BT(tuple_queue):
	"""
	Will run in parallel to ard_tuple. will watch the tuple_queue for new data
	and incoming data will be pushed to cloud Big Table.
	:param tuple_queue: shared queue with ard_tuple process
	:return: None
	"""
	# configure the connection to BigTable
	print("Pushing data to Big Table")
	client = bigtable.Client(project='sensorray', admin=True)
	instance = client.instance('instance')
	table = instance.table('table')
	for batch in range(4):
		for i in range(10):
			for j in range(25):
				podID = str(i).zfill(2) + str(j).zfill(2)
				id = podID

				# read the data from queue
				temp,light,hum,moisture,timestamp = tuple_queue.get()
				data = {
					'temp': temp,
					'light' :light,
					'humidity': hum,
					'moisture'  : moisture
				}

				# write the data
				rows = []
				for key, value in data.items():
					row = table.row(id)
					row.set_cell('sensor',key,int(value),timestamp)
					rows.append(row)
				table.mutate_rows(rows)
		print("Pushed batch "+str(batch)+" to Big Table")


def get_data(sensortype):
	"""
	retreive sensor data from cloud BigTable
	:param sensortype: filter for the kind of sensordata
	:return: np 2d array of sensordata.
	"""
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


def plot_field():
	"""
	parallel process for plotting recent data
	:return: None
	"""
	print("Plot Process started")
	a = get_data('temp')
	fig = plt.figure()
	im = plt.imshow(a, interpolation="bilinear", cmap="plasma", vmin=30, vmax=90)
	fig.colorbar(im)
	plt.title("Temperature Field")

	def update(t):
		im.set_array(get_data('temp'))

	ani = matplotlib.animation.FuncAnimation(fig, func=update, frames=4, repeat=False, interval=300)
	plt.axis('off')
	plt.show()


if __name__ == "__main__":
	# used between two processes ard to tuple and tuple to BT
	tuple_queue = Queue()
	ar_process = mp.Process(target=ard_tuple,args=(tuple_queue,))
	ar_process.start()

	time.sleep(1)
	bt_process = mp.Process(target = tuple_BT,args=(tuple_queue,))
	bt_process.start()

	plot_process = mp.Process(target = plot_field)
	plot_process.start()

	ar_process.join()
	bt_process.join()
	plot_process.join()
	print("All data pushed to cloud")