#! /usr/bin/python
import sys
import getopt
from subprocess import call
import os

#Default value
binary_bits = 128
substr_len = 32
k = 100
image_count = 100000000
n = 4
read_mode = 0
server = "pilaf"
memcached_config = "../config/memcached.cnf"
pilaf_config = "../config/pilaf.cnf"
redis_config = "../config/redis.cnf"
config_path = None
approximate_knn = 0
query_id = 34
query_file = None

def usage():
  print "Usage :"
  print """./run_distributed_search.py [-q query id] [-a approximate knn][-c config path], [-i image count], [-f query file],
  [-b binary bits], [-s substr len],[-k k nearest] [-n n workers] [-r read mode] [--server memcached|pilaf|redis]"""

try:
  opts, args = getopt.getopt(sys.argv[1:], "f:q:c:i:b:s:k:n:r:a", ['server=', ])
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
  elif o == "-a":
    approximate_knn = 1
  elif o == "-q":
    query_id = a
  elif o == "-f":
    query_file = a
  else:
    usage()

if server == "pilaf" and config_path is None:
  config_path = pilaf_config
elif server == "memcached" and config_path is None:
  config_path = memcached_config
elif server == "redis" and config_path is None:
  config_path = redis_config
elif server != "pilaf" and server != "memcached" and server != "redis":
  print "Unrecognized server type."
  usage()
  sys.exit(-1)

cur_dir = os.path.dirname(os.path.realpath(__file__))

arg = ['mpirun', '-n', str(n),  cur_dir + '/distributed-image-search', config_path, 
  str(image_count), str(binary_bits), str(substr_len), str(k), server, str(read_mode), str(approximate_knn), 
  str(query_id)]

if query_file is not None:
  arg.append(query_file)

print "Run with config_path = %s, image_count = %s, binary_bits = %s, substr_bits = %s,\
k = %s, server: %s, read_mode = %s apprximate_knn = %s, query id: %s" % (config_path, image_count, binary_bits, substr_len, 
k, server, read_mode, approximate_knn, query_id)

call(arg)
