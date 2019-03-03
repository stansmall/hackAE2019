import multiprocessing as mp
import time
import datetime
import serial
from google.cloud import bigtable
from multiprocessing import Process, Queue


def ard_tuple(tuple_queue):
	print("Getting data from Arduino")
	ser = serial.Serial("COM10", 9600)
	for i in range(25000):
		data = ser.readline()
		data = data.decode("utf-8").strip("\n")
		temp_tuple = data.split("\t")
		temp_tuple = list(temp_tuple)
		timestamp = datetime.datetime.utcnow()
		temp_tuple.append(timestamp)
		temp_tuple = tuple(temp_tuple)
		tuple_queue.put(temp_tuple)

	ser.close()
	print("All data pushed to tuple_queue, exiting ard_read process")


def tuple_BT(tuple_queue):

	# configure the connection to bigtable
	client = bigtable.Client(project='sensorray', admin=True)
	instance = client.instance('instance')
	table = instance.table('table')
	for _ in range(100):
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


if __name__ == "__main__":
	# used between two processes ard to tuple and tuple to BT
	tuple_queue = Queue()
	ar_process = mp.Process(target=ard_tuple,args=(tuple_queue,))
	ar_process.start()

	time.sleep(5)
	bt_process = mp.Process(target = tuple_BT,args=(tuple_queue,))
	bt_process.start()

	ar_process.join()
	bt_process.join()
	print("All data pushed to cloud")


