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
#include <map>
#include <iostream>
#include "search_worker.h"

using namespace google;

static mpi_coordinator* coord;
static BaseProxy<protobuf::Message, protobuf::Message>* proxy_clt;
static uint32_t image_count;
static read_modes read_mode;
static int k;
static char* config_path;
static int n_local_bits;
static int binary_bits;

void cleanup();
void setup(int argc, char* argv[]);

int main(int argc, char* argv[]){  
  ID image_id;
  BinaryCode code;
  
  setup(argc, argv); 
  
  SearchWorker worker(coord, proxy_clt, k, image_count);
   
  int query_image;
  
  srand(34);
  if(coord->is_master())
    query_image = rand() % image_count;
 
  coord->bcast(&query_image);
  
  image_id.set_id(query_image);
  
  if(proxy_clt->get(image_id, code) != PROXY_FOUND)
    mpi_coordinator::die("Can't find match\n");

  std::string query_code = code.code();
  
  list<SearchWorker::search_result_st> result = worker.find(query_code.c_str(), 16);

  if(coord->is_master()){
    list<SearchWorker::search_result_st>::iterator iter = result.begin(); 
    for(; iter != result.end(); ++iter)
      std::cout<<"Image id : "<<iter->image_id<<" , hamming distance : "<<iter->dist<<endl;  
  }

  cleanup();
  return 0;
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
  if(argc != 8)
    mpi_coordinator::die("Incorrect number of arguments!");
  
  config_path = argv[1];
  image_count = atoi(argv[2]);
  binary_bits = atoi(argv[3]);
  n_local_bits = atoi(argv[4]);
  k = atoi(argv[5]);
  read_mode = (read_modes)atoi(argv[7]);

  mpi_coordinator::init(argc, argv);
  coord = new mpi_coordinator;
  
  if(strcmp(argv[6], "pilaf") == 0)
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(argv[6], "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else
    mpi_coordinator::die("Unrecognized server type.");

  proxy_clt->init(config_path);
} 
