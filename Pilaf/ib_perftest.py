#!/usr/bin/python

import sys
import os
import subprocess
import time
import math
import random

MULTI_HOST = "beaker-16"
SERV_CONF = "dht-one16.cnf"
CLIENT_CONF = "tcp_echo/mpiconf1417"
print("MUST RUN ON "+MULTI_HOST)
ccounts = range(0,18) # [8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536,131072]
tests = ["2","1","r"]
CLIENTS = 32
PULL_ITEM = 10 # Clients run 20 iterations; this pulls the middle one's results

output = {}

cwd = os.getcwd()
for test in tests:
	for ccount in ccounts:
		count = 0
	
		try:
			os.unlink("ib_thru.log")
		except:
			pass # no file to delete
	
		proc_kill = subprocess.Popen("killall -9 ib-test",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		proc_kill.communicate()
		time.sleep(1)
	
		fnull = open(os.devnull, "w")
		port = 4001
		proc_server = subprocess.Popen(["./ib-test","s",str(port)], stdout=fnull, stderr=fnull)
	
		# -v-v-v----- THROUGHPUT ----------v-v-v-
		args = "mpirun -n "+str(CLIENTS)+" -bynode -hostfile "+CLIENT_CONF+" ib-test -c "+SERV_CONF+" -"+test+" -m "+str(ccount)+" -t throughput"
		proc_clients = subprocess.Popen(args,shell=True,stdout=subprocess.PIPE, stderr=fnull)
	
		# Read from output
		vals = []
		while True:
			nextline = proc_clients.stdout.readline()
			if nextline == '' and proc_clients.poll() != None:
				break
			line = nextline
			for line in nextline.rstrip().split('\n'):
				line = line.strip()
				pieces = line.split()
	
				if len(pieces) != 3:
					continue
				if (pieces[1] != str(ccount)+"B"): # only read actual value output lines
					continue
				if (pieces[0] != str(PULL_ITEM)): # only read the middle data item
					continue
	
				try:
					val = float(pieces[2]) # operations per second
					vals.append(val)
		
				except ValueError:
					pass

			if len(vals) >= CLIENTS:
				break;

		proc_clients.kill()
		time.sleep(1)
	
		# -^-^-^----- THROUGHPUT ----------^-^-^-

		if 0 == len(vals):
			print "No data!"
			sys.exit(-1)

		avg = sum(vals)/len(vals)
		devsum = 0;
		for val in vals:
			devsum += (val-avg)*(val-avg)
		dev = math.sqrt(devsum/len(vals))
	
		print("%s\t%d\t%f\t%f\t%f\t%f\t[%d]" % (test,ccount,min(vals),avg,dev,max(vals),len(vals)))

		proc_server.kill()
		time.sleep(1)


