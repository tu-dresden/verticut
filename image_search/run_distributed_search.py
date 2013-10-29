#! /usr/bin/python
import sys
import getopt
from subprocess import call

#Default value
config_path = "dht-test.cnf"
binary_bits = 128
substr_len = 32
k = 100
image_count = 1000000
n = 4
read_mode = 0

def usage():
  print "Usage :"
  print """./run_distributed_search.py [-c config path], [-i image count] [-b binary bits]
   [-s substr len] [-k k nearest] [-n n workers] [-r read mode]"""

try:
  opts, args = getopt.getopt(sys.argv[1:], "c:i:b:s:k:n:r:")
except getopt.GetoptError as err:
  print str(err)
  usage()
  sys.exit(2)

for o, a in opts:
  if o == "-c":
    config_path = a
  elif o == "-i":
    image_count = a
  elif o == "-b":
    binary_bits = a
  elif o == "-s":
    substr_len = a
  elif o == "-k":
    k = a
  elif o == "-n":
    n = a
  elif o == "-r":
    read_mode = a
  else:
    usage()

arg = ['mpirun.openmpi', '-n', str(n),  'distributed-image-search', config_path, 
  str(image_count), str(binary_bits), str(substr_len), str(k), str(read_mode)]

print "Run with config_path = %s, image_count = %s, binary_bits = %s, substr_bits = %s,\
 k = %s, read_mode = %s" % (config_path, image_count, binary_bits, substr_len, k, read_mode)

call(arg)
