#! /usr/bin/python
import sys
import getopt
from subprocess import call

#Default value
binary_bits = 128
substr_len = 32
k = 100
image_count = 1000000
n = 4
read_mode = 0
server = "pilaf"
memcached_config = "../config/memcached.cnf"
pilaf_config = "../config/pilaf.cnf"
config_path = None

def usage():
  print "Usage :"
  print """./run_distributed_search.py [-c config path], [-i image count] [-b binary bits]
   [-s substr len] [-k k nearest] [-n n workers] [-r read mode] [--server memcached|pilaf]"""

try:
  opts, args = getopt.getopt(sys.argv[1:], "c:i:b:s:k:n:r:", ['server=', ])
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
  elif o == "--server":
    server = a
  else:
    usage()

if server == "pilaf" and config_path is None:
  config_path = pilaf_config
elif server == "memcached" and config_path is None:
  config_path = memcached_config
else:
  print "Unrecognized server type."
  usage()
  sys.exit(-1)

arg = ['mpirun.openmpi', '-n', str(n),  'distributed-image-search', config_path, 
  str(image_count), str(binary_bits), str(substr_len), str(k), server, str(read_mode)]

print "Run with config_path = %s, image_count = %s, binary_bits = %s, substr_bits = %s,\
 k = %s, server: %s, read_mode = %s" % (config_path, image_count, binary_bits, substr_len, k, server, read_mode)

call(arg)
