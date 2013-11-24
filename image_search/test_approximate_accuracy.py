#!/usr/bin/python
import sys
import subprocess
import random
import time
import numpy as np

query_id = 0
knn = 0
image_total = 100000000
iterations = 10
test_knns = [3, 100, 500, 1000]

diff = np.zeros(len(test_knns), np.float32)
dist_a = np.zeros(len(test_knns), np.float32)
dist_b = np.zeros(len(test_knns), np.float32)
time_a = np.zeros(len(test_knns), np.float32)
time_b = np.zeros(len(test_knns), np.float32)

def compare_diff(a, b):
  assert(len(a) == len(b))
  max_dist = int(a[0].split(":")[1])
  total = len(a)
  qualified = 0

  for i in b:
    dist = int(i.split(":")[1])
    if dist <= max_dist:
      qualified += 1

  return qualified / float(total)


def compare_distance(a, b):
  assert(len(a) == len(b))
  dist_a = 0.0
  dist_b = 0.0

  for i in xrange(len(a)):
    dist_a += int(a[i].split(":")[1])
    dist_b += int(b[i].split(":")[1])
  
  return (dist_a / len(a), dist_b / len(b))
   

def test():
  #initialize query id
  query_id = random.randint(0, image_total)
  
  for i, k in enumerate(test_knns):
    knn = k 
    command_approximate = ["./run_distributed_search.py", "-k", str(20 * knn), "-i", 
        str(image_total), "-a", "-q", str(query_id)]
    
    command_exact = ["./run_distributed_search.py", "-k", str(knn), "-i", 
        str(image_total), "-q", str(query_id)]
    
    pa = subprocess.Popen(command_approximate, stdout = subprocess.PIPE)
    pe = subprocess.Popen(command_exact, stdout = subprocess.PIPE)
    
    start_t = time.time()  
    [sa, t] = pa.communicate()
    time_a[i] += (time.time() - start_t)

    start_t = time.time()  
    [se, t] = pe.communicate()
    time_b[i] += (time.time() - start_t)
    
    pa.wait()
    pe.wait()

    liste = se.split("\n")
    lista = sa.split("\n")
    diff[i] += compare_diff(liste[-2-knn:-2], lista[-2-knn:-2])
    bd, ad = compare_distance(liste[-2-knn:-2], lista[-2-knn:-2])
    dist_a[i] += ad
    dist_b[i] += bd


for i in xrange(iterations):
  print "%d th iteration:" % (i,)
  test()
  print "accuracy:"
  print diff / (i+1)
  print "approximate distance:"
  print dist_a / (i+1)
  print "exact distance:"
  print dist_b / (i+1)
  print "approximate time:"
  print time_a / (i+1)
  print "exact time:"
  print time_b / (i+1)
