// Copyright (C) 2013, Peking University & New York University
// Author: Qi Chen (chenqi871025@gmail.com) & Yisheng Liao(eason.liao@nyu.edu)
#include <iostream>
#include "mpi_coordinator.h"
#include <string.h>
#include "image_search.pb.h"
#include <vector>
#include <algorithm>
#include "pilaf_proxy.h"
#include "memcached_proxy.h"
#include "redis_proxy.h"
#include <iostream>
#include "search_worker.h"
#include "timer.h"
#include <algorithm>
using namespace google;

static mpi_coordinator* coord;
static BaseProxy<protobuf::Message, protobuf::Message>* proxy_clt;
static uint32_t image_count;
static read_modes read_mode;
static int k;
static char* config_path;
static int n_local_bits;
static int binary_bits;
static bool approximate_knn;
static int query_image_id = -1;
static char* query_file = 0;

//How many rdma accesses performs
extern uint64_t pilaf_n_rdma_read;

//Statisctis
static uint64_t total_dist_ex, total_dist_app, inaccurate_count;
static uint64_t time_app, time_ex;

void cleanup();
void setup(int argc, char* argv[]);
void record_statistic(std::list<SearchWorker::search_result_st> &list_ex, 
                      std::list<SearchWorker::search_result_st> &list_app);

uint64_t gettime(){
  struct timeval tm_t;
  gettimeofday(&tm_t, NULL); 
  return (1000000 * tm_t.tv_sec + tm_t.tv_usec);
}

int main(int argc, char* argv[]){  
  ID image_id;
  BinaryCode code;
  uint64_t n_main_reads, n_sub_reads, n_local_reads;
  uint64_t n_main_reads_total = 0, n_sub_reads_total = 0, n_local_reads_total = 0;
  uint32_t radius, radius_total;

  setup(argc, argv); 

  SearchWorker worker(coord, proxy_clt, image_count);
  assert(query_image_id != -1 && query_image_id < image_count);

  if(query_file){
    FILE* f = fopen(query_file, "r");
    if(f == 0){
      fprintf(stderr, "Couldn't open file %s\n", query_file);
      mpi_coordinator::die("Error");
    }
    
    int n_query = 0;
    char code[17];
    code[16] = '\0';
    

    while(fread(code, 16, 1, f) != 0){
      list<SearchWorker::search_result_st> result_app, result_exact;
      
      coord->synchronize();
      
      uint64_t start = gettime();
      result_app = worker.find(code, 16, k, true);
      time_app += gettime() - start;
      
      /*
      coord->synchronize();
      
      start = gettime();
      result_exact = worker.find(code, 16, k, false);
      time_ex += gettime() - start;
      coord->synchronize();
      
      n_query++;
    
      if(coord->is_master()){
        record_statistic(result_exact, result_app);
        std::cout<<(float)total_dist_ex / n_query / k<<" "<<(float)total_dist_app / n_query / k<<" "<<(float)inaccurate_count / n_query / k<<std::endl;
        std::cout<<"app time : "<<(double)time_app / n_query / 1000000<<", ex time : "<<(double)time_ex / n_query / 1000000 <<std::endl;
      }
      coord->synchronize();
      */
    }
  }
  else{
    mpi_coordinator::die("The version without main table doesn't support query by id.\.");
  }

  cleanup();
  return 0;
}

uint32_t dist_accumulate(std::list<SearchWorker::search_result_st>& a){ 
  std::list<SearchWorker::search_result_st>::iterator iter = a.begin();
  uint32_t total = 0;

  for(; iter != a.end(); ++iter)
    total += iter->dist;

  return total;
}

uint32_t test_inaccurate(uint32_t dist_threshold, std::list<SearchWorker::search_result_st>& app){
  uint32_t count = 0;
  std::list<SearchWorker::search_result_st>::iterator iter = app.begin();
  for(; iter != app.end(); ++iter, ++count)
    if(iter->dist <= dist_threshold)
      break;
  
  return count;
}

void record_statistic(std::list<SearchWorker::search_result_st> &list_ex, 
                      std::list<SearchWorker::search_result_st> &list_app){
  uint32_t dist_ex = dist_accumulate(list_ex);
  uint32_t dist_app = dist_accumulate(list_app);

  total_dist_ex += dist_ex;
  total_dist_app += dist_app;
  
  inaccurate_count += test_inaccurate(list_ex.front().dist, list_app);
}

//Clean up code.
void cleanup(){
  if(coord != 0){
    delete coord;
    coord = 0;
  }
  if(proxy_clt != 0){
    proxy_clt->close();
    delete proxy_clt;
    proxy_clt = 0;
  }
  mpi_coordinator::finalize(); 
}

//Set up code. The arguments should be passed by bootstrap script(run_distributed_search.py) 
void setup(int argc, char* argv[]){
  if(argc < 10)
    mpi_coordinator::die("Incorrect number of arguments!");
  
  config_path = argv[1];
  image_count = atoi(argv[2]);
  binary_bits = atoi(argv[3]);
  n_local_bits = atoi(argv[4]);
  k = atoi(argv[5]);
  read_mode = (read_modes)atoi(argv[7]);
  approximate_knn = atoi(argv[8]);
  query_image_id = atoi(argv[9]);
  
  if(argc == 11)
    query_file = argv[10];

  mpi_coordinator::init(argc, argv);
  coord = new mpi_coordinator;
  
  if(strcmp(argv[6], "pilaf") == 0)
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(argv[6], "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(argv[6], "redis") == 0)
    proxy_clt = new RedisProxy<protobuf::Message, protobuf::Message>;
  else
    mpi_coordinator::die("Unrecognized server type.");
  {
  timer t("connect");
  proxy_clt->init(config_path);
  }
} 
