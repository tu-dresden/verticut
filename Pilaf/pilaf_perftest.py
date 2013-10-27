#!/usr/bin/python

import sys
import os
import subprocess
import time
import math

ccounts = [40]
tests = ["p","g"]
servers = 4

output = {}
for test in tests:
	output[test] = []

print "MUST RUN FROM BEAKER-22!!"
for logging in [0]:
	for ccount in ccounts:
	
		count = 0
		for test in tests:
			if test[0] == 'L':
				clsize = (12*1024*1024*1024)/((ccount)*(1024+64+64))
				clsize = min(clsize,800000)
			else:
				clsize = (14*1024*1024*1024)/((ccount)*(64+16+64))
				clsize = min(clsize,1000000)
	
			if 'p' in test:
				print("Restarting server...")
				proc_kill = subprocess.Popen("killall -9 dht-test",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				proc_kill.communicate()
				time.sleep(1)
		
				port = 4000
				fnull = open(os.devnull, "w")
				slog  = open("server.log", "w")
				presize = (test[-1] != 'r')
				if test[-1] == 'r':
					args = ["./dht-test","-s",str(port),"-T","-P",str(ccount*clsize)]
				else:
					args = ["./dht-test","-s",str(port),"-T"]
				if logging:
					args = args+["-l","/ssd/kerm/dhtlog.log"]
				print " ".join(args)
				proc_server = subprocess.Popen(args, stdout=slog, stderr=slog)
				time.sleep(5)	# Make sure old servers clear
	
			# Crop out the 'r'
			test_code = test
			if test_code[-1] == 'r':
				test_code = test_code[:-1]
	
			args = "mpirun -n "+str(ccount)+" -bynode -hostfile mpiconf ./dht-test -c dht-one.cnf -T -r -t "+test_code+" -P "+str(clsize)+""
			print args
			proc = subprocess.Popen(args,shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
			pipes = proc.communicate()
	
			# Read from pipes
			vals = []
			complete = 0
			for line in pipes[0].splitlines():
				print(line)
	
				if line.strip() == "COMPLETE -<>-":
					complete+=1
					continue
	
				try:
					val = float(line.strip())
					vals.append(val)
	
				except ValueError:
					pass
	
			if 0 == len(vals):
				print pipes
				sys.exit(-1)
	
			if complete != ccount:
				print("Didn't get enough COMPLETE -<>- lines (%d/%d)" % (complete,ccount))
				print("Some processes failed.")
				sys.exit(-1)
	
			avg = sum(vals)/len(vals)
			devsum = 0;
			for val in vals:
				devsum += (val-avg)*(val-avg)
			dev = math.sqrt(devsum/len(vals))
	
			output[test].append("%f\t%f\t%f\t%f" % (min(vals),avg,dev,max(vals)))
	
			count += 1
	
			print("------- Completed %d ccounts" % count)
			for test in tests:
				for item in output[test]:
					print(item)
				print("")


