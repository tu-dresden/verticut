#!/usr/bin/python

import sys
import os
import subprocess
import time
import math
import random

MULTI_HOST = "beaker-25"
serv_ips = ["216.165.108.105","192.168.5.25"]
print("MUST RUN ON "+MULTI_HOST)
ccounts = [8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536,131072]
tests = ["LAT","THRU"]
CLIENTS = 40
MACHINES = 5

output = {}

cwd = os.getcwd()
for SERV_IP in serv_ips:
	for test in tests:
		for ccount in ccounts:
			count = 0
		
			try:
				os.unlink("echo_thru.log")
			except:
				pass # no file to delete
		
			proc_kill = subprocess.Popen("killall -9 multi",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			proc_kill.communicate()
			time.sleep(1)

			proc_clients = subprocess.Popen("mpirun -n "+str(MACHINES)+" -bynode -hostfile mpiconf killall -9 client",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			proc_clients.wait()
		
			port = random.randint(2048,65535)
		
			# -v-v-v----- THROUGHPUT ----------v-v-v-
			if test == "THRU":
				proc_server = subprocess.Popen(["./multi","-p",str(port),"-s",str(ccount),"-r","10"])

				proc_clients = subprocess.Popen("mpirun --mca btl tcp,self -n "+str(CLIENTS)+" -hostfile mpiconf ./client -a "+SERV_IP+" -p "+str(port)+" -s "+str(ccount)+" -r 10",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		
				# Now collect some samples
				time.sleep(5)					#head
				proc_kill = subprocess.Popen(["/bin/kill","-10",str(proc_server.pid)])
				proc_kill.wait()

				time.sleep(20)					#bulk
				proc_kill = subprocess.Popen(["/bin/kill","-10",str(proc_server.pid)])
				proc_kill.wait()
		
				time.sleep(5)					#tail
				proc_kill = subprocess.Popen(["/bin/kill","-10",str(proc_server.pid)])
				proc_kill.wait()

				proc_server.kill()
				time.sleep(1)
				proc_clients.kill()
		
				# Read from file
				f = open("echo_thru.log",'r');
				vals = []
				for line in f:
					line = line.strip()
					#print line
					pieces = line.split()
					try:
						val = float(pieces[2])/(float(pieces[1])/1000000) # operations per second
						vals.append(val)
		
					except ValueError:
						pass
					except ZeroDivisionError:
						print "One of the times was zero!"
				if 1 >= len(vals):
					print "No data for this run!"
					continue
	
				vals = [vals[1]]
		
				avg = sum(vals)/len(vals)
				devsum = 0;
				for val in vals:
					devsum += (val-avg)*(val-avg)
				dev = math.sqrt(devsum/len(vals))
		
				print("%s\t%d\t%f\t%f\t%f\t%f" % (test,ccount,min(vals),avg,dev,max(vals)))
	
			# -^-^-^----- THROUGHPUT ----------^-^-^-
	
			# -v-v-v-----  LATENCY   ----------v-v-v-
			else:
				proc_server = subprocess.Popen(["./multi","-p",str(port),"-s",str(ccount),"-r","10","-L"])

				proc_clients = subprocess.Popen("ssh beaker-21 \"cd /home/kerm/infiniband/rdma-hashtable/tcp_echo/ && ./client -a "+SERV_IP+" -p "+str(port)+" -s "+str(ccount)+" -r 10\"",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		
				proc_server.wait()
				time.sleep(2)
				proc_clients.kill()
				# Read from file
				f = open("echo_lat.log",'r');
				vals = []
				for line in f:
					line = line.strip()
					if len(line):
						print("LAT\t%d\t%s" % (ccount,line))
			# -^-^-^-----  LATENCY   ----------^-^-^-
	
	
